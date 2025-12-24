package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/makasim/flowstate"
	"github.com/makasim/flowstate/memdriver"
	"github.com/makasim/flowstream"
)

// Only one consumer should consume message, the other one should be in stand by mode.
func main() {
	l := slog.Default()
	d := flowstate.NewCacheDriver(memdriver.New(l), 1000, l)

	p := flowstream.NewProducer(d)
	for i := 0; i < 10; i++ {
		if err := p.Send(&flowstream.ProduceMessage{
			Stream: `fooStream`,
			Body:   []byte(fmt.Sprintf("hello world %d", i)),
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		m, err := c.Receive(ctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			log.Fatal(err)
		}

		l.Info("got message", "consumer", m.ConsumerID, "rev", m.Rev, "body", string(m.Body))
		if err := c.Commit(m.Rev); err != nil {
			log.Fatal(err)
		}
	}
}
