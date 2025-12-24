## flowstream

Flowstream is a Golang package and server providing Kafka-like streaming capabilities:
- Produce messages into a stream or streams.
- Messages produced in one call preserve the order. The order between separate calls is not determined.
- The message order in a stream is determined and cannot change. All consumer groups read messages in the same order.
- Only one consumer per group is allowed to consume messages.
- In case of failures, other standby consumers can take over, only one becomes active.
- Several independent consumer groups could exist.
- Partitions are not supported but will be.
- No replication as there is no consensus algorithm.

### Purpose

Provide a lightweight streaming solution, with sane fault tolerancy that runs on commodity databases like PostgreSQL. 
Works without distributed consensus protocols or heavy coordination.

**Key benefits:**
- Simple architecture with minimal moving parts
- No consensus protocol (Raft, Paxos, etc.) required
- Minimal coordination overhead - almost no heartbeats or high-frequency synchronization
- Minimal metadata state changes for better performance
- Leverages existing database infrastructure (PostgreSQL, BadgerDB)
- Fast performance on PostgreSQL (benchmarks TBD)

### Failure modes

In most cases, only one consumer is active. However, in edge cases or during crashes, two consumers might briefly work in parallel for a short period. The system will detect this and quickly converge back to a single active consumer.

**How the system handles failures:**

- **Consumer crash**: Standby consumers attempt to take over one minute after the last consumer group state change. One consumer wins and becomes active.
- **Long message processing**: If message processing takes more than 30 seconds, the active consumer sends a heartbeat at 28 seconds since the last consumer state commit to maintain its active status.
- **Graceful shutdown**: The active consumer explicitly commits its state as inactive. Standby consumers observe the state change and attempt to take over. One consumer wins.
- **Zombie consumer**: Forced into standby mode by the async state sync goroutine or by revision mismatch conflict on commit, preventing dual active consumers. 

### Examples

Produce:
```go
package main

import (
	"log"
	"fmt"
	"log/slog"

	"github.com/makasim/flowstate"
	"github.com/makasim/flowstate/memdriver"
	"github.com/makasim/flowstream"
)

func main() {
	l := slog.Default()
	
	// See flowstate docs for available driver implementations.
	// https://github.com/makasim/flowstate#drivers
	d := flowstate.NewCacheDriver(memdriver.New(l), 1000, l)
	
	p := flowstream.NewProducer(d)
	for i := 0; i < 10; i++ {
		if err := p.Send(&flowstream.ProduceMessage{
			Stream: `foo-stream`,
			Body: []byte(fmt.Sprintf("hello world %d", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}
}
```

Consume:
```go
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

func main() {
    l := slog.Default()
    
    // See flowstate docs for available driver implementations.
    // https://github.com/makasim/flowstate#drivers
    d := flowstate.NewCacheDriver(memdriver.New(l), 1000, l)
    
    c, err := flowstream.NewConsumer(`foo-stream`, `aConsumerGroup`, d, slog.Default())
    if err != nil {
        log.Fatal(err)
    }

	for {
		//  context.DeadlineExceeded signals the consumer is at the head of the stream or in standby mode.
		// Wait until:
		//   - a new message arrives,
		//   - the consumer is reactivated,
		//
		// The timeout guarantees control is returned to the caller,
		// allowing periodic housekeeping or other work.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		m, err := c.Receive(ctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			log.Fatal(err)
		}

		// Process the message.
		// Message processing should be idempotent, since delivery is at-least-once.

		// Commit progress either per-message or in batches.
		// Committing acknowledges that all messages up to the given revision
		// have been successfully processed.
		if err := c.Commit(m.Rev); err != nil {
			log.Fatal(err)
		}
	}
}
```


* [Basic example](examples/simple/main.go).
* [Consumer waits for new messages](examples/wait/main.go).
* [Active\stand by consumers](examples/onlyone/main.go).
* [Heartbeat](examples/heartbeat/main.go).