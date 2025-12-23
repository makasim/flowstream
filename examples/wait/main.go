package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/makasim/flowstate/memdriver"
	"github.com/makasim/flowstream"
)

func main() {
	l := slog.Default()
	d := memdriver.New(l)

	go func() {
		time.Sleep(time.Second * 10)
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
	}()

	c, err := flowstream.NewConsumer(`fooStream`, `aGroup`, d, l)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Shutdown()

	for {
		for c.Next() {
			m := c.Message()
			l.Info("got message", "consumer", c.ID(), "rev", m.Rev, "body", string(m.Body))
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
