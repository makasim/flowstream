package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/makasim/flowstate"
	"github.com/makasim/flowstate/memdriver"
	"github.com/makasim/flowstream"
)

// The message processing takes too long, so not to lost active state the consumer sends a heartbeat (30s in).
func main() {
	l := slog.Default()
	d := memdriver.New(l)

	var cnt int
	p := flowstream.NewProducer(`fooStream`, d)

	for i := 0; i < 10; i++ {
		cnt++
		if err := p.Send(&flowstream.Message{
			Body: []byte(fmt.Sprintf("hello world %d", cnt)),
		}); err != nil {
			log.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		consume(d)
	})

	wg.Go(func() {
		consume(d)
	})

	wg.Wait()
}

func consume(d flowstate.Driver) {
	l := slog.Default()
	c, err := flowstream.NewConsumer(`fooStream`, `aGroup`, d, l)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Shutdown()

	for {
		for c.Next() {
			m := c.Message()
			l.Info("got message", "consumer", c.ID(), "rev", m.Rev, "body", string(m.Body))

			if string(m.Body) == `hello world 3` {
				l.Info("the message processing takes 40s")
				time.Sleep(time.Second * 40)
			}

			if err := c.Commit(m.Rev); err != nil {
				log.Fatal(err)
			}
		}
		if err := c.Err(); err != nil {
			log.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		c.Wait(ctx)
		cancel()
	}
}
