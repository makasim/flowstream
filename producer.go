package dchan

import (
	"github.com/makasim/flowstate"
	"github.com/oklog/ulid/v2"
)

type Message struct {
	Rev  int64
	Body []byte
}

type Producer struct {
	name string
	e    *flowstate.Engine
}

func NewProducer(stream string, e *flowstate.Engine) *Producer {
	if stream == "" {
		panic("BUG: stream is required")
	}

	return &Producer{
		name: stream,
		e:    e,
	}
}

func (ch *Producer) Send(msgs ...*Message) error {
	if len(msgs) == 0 {
		return nil
	}

	var cmds []flowstate.Command
	for _, msg := range msgs {
		cmds = append(cmds, flowstate.Park(&flowstate.StateCtx{
			Current: flowstate.State{
				ID: flowstate.StateID(ulid.Make().String()),
				Labels: map[string]string{
					"stream": ch.name,
				},
				Annotations: map[string]string{
					"body": string(msg.Body),
				},
			},
		}))
	}

	return ch.e.Do(flowstate.Commit(cmds...))
}
