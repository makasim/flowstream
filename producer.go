package flowstream

import (
	"errors"
	"fmt"

	"github.com/makasim/flowstate"
	"github.com/oklog/ulid/v2"
)

type ProduceMessage struct {
	Stream string
	Body   []byte
}

type Producer struct {
	d flowstate.Driver
}

func NewProducer(d flowstate.Driver) *Producer {
	return &Producer{
		d: d,
	}
}

func (ch *Producer) Send(msgs ...*ProduceMessage) error {
	if len(msgs) == 0 {
		return fmt.Errorf("no message provided")
	}

	var validErr error

	var cmds []flowstate.Command
	for i, msg := range msgs {
		if msg.Stream == "" {
			errors.Join(validErr, fmt.Errorf("msgs[%d].stream is empty", i))
		}

		msgStateCtx := &flowstate.StateCtx{
			Current: flowstate.State{
				ID: flowstate.StateID(ulid.Make().String()),
				Labels: map[string]string{
					"stream": msg.Stream,
				},
			},
		}

		if len(msg.Body) <= 100 {
			msgStateCtx.Current.SetAnnotation("body", string(msg.Body))
		} else {
			msgStateCtx.Datas = map[string]*flowstate.Data{
				"body": {
					Blob: msg.Body,
				},
			}
			cmds = append(cmds, flowstate.StoreData(msgStateCtx, "body"))
		}

		cmds = append(cmds, flowstate.Park(msgStateCtx))
	}

	if validErr != nil {
		return validErr
	}

	return ch.d.Commit(flowstate.Commit(cmds...))
}
