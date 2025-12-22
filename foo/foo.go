package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/makasim/flowstate"
	"github.com/makasim/flowstate/memdriver"
	"github.com/makasim/flowstream"
)

func main() {
	var wg sync.WaitGroup

	l := slog.New(slog.NewTextHandler(os.Stderr, nil))
	d := memdriver.New(l)
	e, err := flowstate.NewEngine(d, &flowstate.DefaultFlowRegistry{}, l)
	if err != nil {
		log.Fatal(err)
	}

	wg.Go(func() {
		var cnt int
		p := flowstream.NewProducer(`fooStream`, e)

		for i := 0; i < 10; i++ {
			cnt++
			if err := p.Send(&flowstream.Message{
				Body: []byte(fmt.Sprintf("hello world %d", cnt)),
			}); err != nil {
				log.Fatal(err)
			}
		}

		time.Sleep(65 * time.Second)
		for i := 0; i < 10; i++ {
			cnt++
			if err := p.Send(&flowstream.Message{
				Body: []byte(fmt.Sprintf("hello world %d", cnt)),
			}); err != nil {
				log.Fatal(err)
			}
		}
	})

	wg.Go(func() {
		c, err := flowstream.NewConsumer(`fooStream`, `aGroup`, e, l)
		if err != nil {
			log.Fatal(err)
		}

		for {
			for c.Next() {
				log.Println("aGroup: Got message:", string(c.Message().Body))
				if err := c.Commit(c.Message().Rev); err != nil {
					log.Fatal(err)
				}

				if string(c.Message().Body) == `hello world 4` {
					if err := c.Shutdown(); err != nil {
						log.Fatal(err)
					}
					return
				}
			}
			if err := c.Err(); err != nil {
				log.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			c.Wait(ctx)
			cancel()
		}
	})

	wg.Go(func() {
		c, err := flowstream.NewConsumer(`fooStream`, `aGroup`, e, l)
		if err != nil {
			log.Fatal(err)
		}

		for {
			for c.Next() {
				log.Println("aGroup: Got message:", string(c.Message().Body))
				if err := c.Commit(c.Message().Rev); err != nil {
					log.Fatal(err)
				}

				if string(c.Message().Body) == `hello world 4` {
					if err := c.Shutdown(); err != nil {
						log.Fatal(err)
					}
					return
				}
			}
			if err := c.Err(); err != nil {
				log.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			c.Wait(ctx)
			cancel()
		}
	})

	//wg.Go(func() {
	//	c, err := dchan.NewConsumer(`fooStream`, `aGroup123`, e, l)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//
	//	for {
	//		for c.Next() {
	//			log.Println("aGroup123: Got message:", string(c.Message().Body))
	//			if err := c.Commit(c.Message().Rev); err != nil {
	//				log.Fatal(err)
	//			}
	//		}
	//		if err := c.Err(); err != nil {
	//			log.Fatal(err)
	//		}
	//
	//		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	//		c.Wait(ctx)
	//		cancel()
	//	}
	//})

	wg.Wait()

}
