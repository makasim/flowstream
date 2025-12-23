package flowstream

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
	d    flowstate.Driver
}

func NewProducer(stream string, d flowstate.Driver) *Producer {
	if stream == "" {
		panic("BUG: stream is required")
	}

	return &Producer{
		name: stream,
		d:    d,
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

	return ch.d.Commit(flowstate.Commit(cmds...))
}
