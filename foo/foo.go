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
	d := flowstate.NewCacheDriver(memdriver.New(l), 10000, l)

	wg.Go(func() {
		defer func() {
			log.Println("c2 done")
		}()

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
		defer func() {
			log.Println("c1 done")
		}()

		c, err := flowstream.NewConsumer(`fooStream`, `aGroup`, d, l)
		if err != nil {
			log.Fatal(err)
		}

		for {
			for c.Next() {
				m := c.Message()
				log.Printf("%s: got: rev=%d body='%s'", c.ID(), m.Rev, string(m.Body))

				if string(m.Body) == `hello world 4` {
					//if err := c.Shutdown(); err != nil {
					//	log.Fatal(err)
					//}
					return
				}

				log.Printf("%s: commit: rev=%d", c.ID(), m.Rev)
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
	})

	time.Sleep(time.Millisecond * 100)

	wg.Go(func() {
		defer func() {
			log.Println("c2 done")
		}()

		c, err := flowstream.NewConsumer(`fooStream`, `aGroup`, d, l)
		if err != nil {
			log.Fatal(err)
		}

		for {
			for c.Next() {
				m := c.Message()
				log.Printf("%s: got: rev=%d body='%s'", c.ID(), m.Rev, string(m.Body))
				log.Printf("%s: commit: rev=%d", c.ID(), m.Rev)
				if err := c.Commit(m.Rev); err != nil {
					log.Fatal(err)
				}

				//if string(m.Body) == `hello world 4` {
				//	if err := c.Shutdown(); err != nil {
				//		log.Fatal(err)
				//	}
				//	return
				//}
			}
			if err := c.Err(); err != nil {
				log.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			c.Wait(ctx)
			cancel()
		}
	})

	wg.Wait()

}
