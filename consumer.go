package flowstream

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/makasim/flowstate"
	"github.com/oklog/ulid/v2"
)

type Consumer struct {
	id     string
	stream string
	group  string
	d      flowstate.Driver
	l      *slog.Logger

	mu   sync.Mutex
	s    flowstate.State
	iter *flowstate.Iter
	curr *Message

	stopCtx    context.Context
	stopCancel context.CancelFunc
}

func NewConsumer(stream, group string, d flowstate.Driver, l *slog.Logger) (*Consumer, error) {
	if stream == "" {
		panic("BUG: stream is required")
	}
	if group == "" {
		panic("BUG: group is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		id:     ulid.Make().String(),
		stream: stream,
		group:  group,
		d:      d,
		l:      l,

		stopCtx:    ctx,
		stopCancel: cancel,
	}
	if err := c.getLatestOrCreateLocked(0); err != nil {
		return nil, err
	}

	// DEBUG
	if c.canConsume(c.s) {
		c.l.Debug(fmt.Sprintf("%s: active rev=%d ann=%+v", c.id, c.s.Rev, c.s.Annotations))
	} else {
		c.l.Debug(fmt.Sprintf("%s: standby rev=%d ann=%+v", c.id, c.s.Rev, c.s.Annotations))
	}

	go c.doSyncState()

	return c, nil
}

func (c *Consumer) ID() string {
	return c.id
}

func (c *Consumer) Shutdown() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.s.Annotations["id"] != c.id {
		return nil
	}
	if c.s.Annotations["state"] != "1" {
		return nil
	}

	c.s.Annotations["state"] = "0"

	stateCtx := c.s.CopyToCtx(&flowstate.StateCtx{})
	if err := c.d.Commit(flowstate.Commit(flowstate.Park(stateCtx))); err != nil {
		return err
	}
	stateCtx.Current.CopyTo(&c.s)

	c.stopCancel()

	return nil
}

func (c *Consumer) Next() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.canConsume(c.s) {
		return false
	}

	if c.iter.Next() {
		s := c.iter.State()
		c.curr = &Message{
			Rev:  s.Rev,
			Body: []byte(s.Annotations["body"]),
		}
		return true
	}

	return false
}

func (c *Consumer) Message() *Message {
	if c.curr == nil {
		panic("BUG: Next() must be called and return true")
	}

	return c.curr
}

func (c *Consumer) Err() error {
	return c.iter.Err()
}

func (c *Consumer) Wait(ctx context.Context) {
	c.mu.Lock()
	can := c.canConsume(c.s)
	c.mu.Unlock()

	if can {
		c.iter.Wait(ctx)
		return
	}

	<-ctx.Done()
}

func (c *Consumer) Commit(rev int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.canConsume(c.s) {
		return fmt.Errorf("consumer cannot commit as it is in standby state")
	}

	stateCtx := c.s.CopyToCtx(&flowstate.StateCtx{})
	stateCtx.Current.SetAnnotation("rev", strconv.FormatInt(rev, 10))
	if err := c.d.Commit(flowstate.Commit(flowstate.Park(stateCtx))); err != nil {
		return err
	}
	stateCtx.Current.CopyTo(&c.s)
	c.initIterLocked()

	return nil
}

func (c *Consumer) doSyncState() {
	c.mu.Lock()
	s := c.s.CopyTo(&flowstate.State{})
	c.mu.Unlock()

	iter := flowstate.NewIter(c.d, flowstate.GetStatesByLabels(map[string]string{
		"consumer.stream": c.stream,
		"consumer.group":  c.group,
	}).WithSinceRev(s.Rev).WithLatestOnly())

	hbt, hbf := c.resetHeartbeat(s, time.NewTimer(0))

	for {
		select {
		case <-hbt.C:
			if err := hbf(s); err != nil {
				c.l.Error("failed to perform heartbeat or takeover",
					"id", c.id,
					"stream", c.stream,
					"group", c.group,
					"err", err,
				)

				hbt, hbf = c.resetHeartbeat(s, hbt)

				time.Sleep(time.Second * 5)
				continue
			}
		case <-c.stopCtx.Done():
			return
		default:
		}

		for iter.Next() {
			nextS := iter.State()

			c.mu.Lock()
			if nextS.Rev <= c.s.Rev {
				c.mu.Unlock()
				continue
			}
			nextS.CopyTo(&c.s)
			nextS.CopyTo(&s)
			c.mu.Unlock()

			// DEBUG
			if c.canConsume(c.s) {
				c.l.Debug(fmt.Sprintf("%s: active rev=%d ann=%+v", c.id, c.s.Rev, c.s.Annotations))
			} else {
				c.l.Debug(fmt.Sprintf("%s: standby rev=%d ann=%+v", c.id, c.s.Rev, c.s.Annotations))
			}

			if !c.isActive(s) {
				if err := c.doTakeover(s); err != nil {
					c.l.Error("failed to takeover",
						"id", c.id,
						"stream", c.stream,
						"group", c.group,
						"err", iter.Err(),
					)

					time.Sleep(time.Second * 5)
					continue
				}
			}

			hbt, hbf = c.resetHeartbeat(s, hbt)
		}

		if iter.Err() != nil {
			c.l.Error("failed to get next consumer state",
				"id", c.id,
				"stream", c.stream,
				"group", c.group,
				"err", iter.Err(),
			)

			iter = flowstate.NewIter(c.d, flowstate.GetStatesByLabels(map[string]string{
				"consumer.stream": c.stream,
				"consumer.group":  c.group,
			}).WithSinceRev(s.Rev).WithLatestOnly())

			time.Sleep(time.Second * 5)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		iter.Wait(ctx)
		cancel()
	}
}

func (c *Consumer) maybeDoHeartbeat(s flowstate.State) error {
	msgStateCtx := &flowstate.StateCtx{}
	if err := c.d.GetStateByLabels(flowstate.GetStateByLabels(msgStateCtx, map[string]string{
		"stream": c.stream,
	})); err != nil {
		return err
	}

	if msgStateCtx.Current.Rev <= c.sinceRev(s) {
		return nil
	}

	if err := c.doHeartbeat(s); flowstate.IsErrRevMismatch(err) {
		return c.getLatestOrCreateLocked(s.Rev)
	} else if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) doHeartbeat(s flowstate.State) error {
	if !c.canConsume(s) {
		panic("BUG: consumer in standbay mode should not perform heartbeat")
	}

	stateCtx := s.CopyToCtx(&flowstate.StateCtx{})
	stateCtx.Current.SetAnnotation("rev", strconv.FormatInt(c.sinceRev(s), 10))
	if err := c.d.Commit(flowstate.Commit(flowstate.Park(stateCtx))); err != nil {
		return err
	}
	stateCtx.Current.CopyTo(&c.s)
	c.initIterLocked()

	return nil
}

func (c *Consumer) maybeDoTakeover(s flowstate.State) error {
	msgStateCtx := &flowstate.StateCtx{}
	if err := c.d.GetStateByLabels(flowstate.GetStateByLabels(msgStateCtx, map[string]string{
		"stream": c.stream,
	})); err != nil {
		return err
	}

	if msgStateCtx.Current.Rev <= c.sinceRev(s) {
		return nil
	}

	return c.doTakeover(s)
}

func (c *Consumer) doTakeover(s flowstate.State) error {
	stateCtx := s.CopyToCtx(&flowstate.StateCtx{})
	stateCtx.Current.Annotations["id"] = c.id
	stateCtx.Current.Annotations["state"] = "1"
	if err := c.d.Commit(flowstate.Commit(flowstate.Park(stateCtx))); flowstate.IsErrRevMismatch(err) {
		return c.getLatestOrCreateLocked(s.Rev)
	} else if err != nil {
		return err
	}
	stateCtx.Current.CopyTo(&c.s)
	c.initIterLocked()

	return nil
}

func (c *Consumer) getLatestOrCreateLocked(sinceRev int64) error {
	sID := flowstate.StateID(fmt.Sprintf("consumer.%s.%s", c.stream, c.group))

	getCmd := flowstate.GetStatesByLabels(map[string]string{
		"consumer.stream": c.stream,
		"consumer.group":  c.group,
	}).WithSinceRev(sinceRev).WithLatestOnly().WithLimit(1)
	if err := c.d.GetStates(getCmd); err != nil {
		return err
	}
	if len(getCmd.MustResult().States) == 0 {
		s := &flowstate.StateCtx{
			Current: flowstate.State{
				ID: sID,
				Annotations: map[string]string{
					"id":    c.id,
					"rev":   "0",
					"state": "1",
				},
				Labels: map[string]string{
					"consumer.stream": c.stream,
					"consumer.group":  c.group,
				},
			},
		}

		if err := c.d.Commit(flowstate.Commit(flowstate.Park(s))); err == nil {
			s.Current.CopyTo(&c.s)
			c.initIterLocked()
			return nil
		} else if !flowstate.IsErrRevMismatch(err) {
			return err
		}

		s = &flowstate.StateCtx{}
		if err := c.d.GetStateByID(flowstate.GetStateByID(s, sID, 0)); err != nil {
			return err
		}

		s.Current.CopyTo(&c.s)
		c.initIterLocked()
		return nil
	}
	getCmd.MustResult().States[0].CopyTo(&c.s)
	c.initIterLocked()
	return nil
}

func (c *Consumer) initIterLocked() {
	c.iter = flowstate.NewIter(c.d, flowstate.GetStatesByLabels(map[string]string{
		"stream": c.stream,
	}).WithSinceRev(c.sinceRev(c.s)))
}

func (c *Consumer) sinceRev(s flowstate.State) int64 {
	sinceRev, _ := strconv.ParseInt(s.Annotations["rev"], 0, 64)
	return sinceRev
}

func (c *Consumer) canConsume(s flowstate.State) bool {
	return c.id == s.Annotations["id"] && s.Annotations["state"] == "1"
}

func (c *Consumer) isActive(s flowstate.State) bool {
	return s.Annotations["state"] == "1"
}

func (c *Consumer) resetHeartbeat(s flowstate.State, t *time.Timer) (*time.Timer, func(s flowstate.State) error) {
	if t == nil {
		t = time.NewTimer(0)
	}

	if c.canConsume(s) {
		t.Reset(time.Second * 29)
		return t, c.maybeDoHeartbeat
	}

	t.Reset(time.Minute)
	return t, c.maybeDoTakeover
}
