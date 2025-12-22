## flowstreat

Flowstream is a library providing Kafka-like streaming semantics. 
It works with any driver supported by [flowstate](https://github.com/makasim/flowstate).
Currently PostgreSQL and BadgerDB are supported (see [Drivers](https://github.com/makasim/flowstate?tab=readme-ov-file#drivers)).

The model is **at least once** delivery, the model does best effort to guaranty only one consumer from the group can read stream. 
In normal conditions with steady stream of upcoming message and fast message processing  the guarantee will hold. 
In some edge cases several consumers may start processing messages but they should quickly detect race condition either by sync group state or on commit and diverge on single mode.
That design is implemented on purpose this way and provides several benefits.
- It is simple.
- There is no consensus protocol.
- Almost no heartbeats or other high frequency coordination.
- There is as few meta state changes as possible.
- Works on Postgress with great speed (TBD).

Produce:
```go
// See flowstate docs on how to init Engine
var e *flowstate.Engine

p := flowstream.NewProducer(`foo-stream`, e)

for i := 0; i < 10; i++ {
	if err := p.Send(&flowstream.Message{
		Body: []byte(fmt.Sprintf("hello world %d", cnt)),
	}); err != nil {
		log.Fatal(err)
	}
}
```

Consume:
```go
// See flowstate docs on how to init Engine
var e *flowstate.Engine

c, err := flowstream.NewConsumer(`foo-stream`, `aConsumerGroup`, e, l)
if err != nil {
	log.Fatal(err)
}

for {
	for c.Next() {  
		m := c.Message()
		// handle your message here
		
		// commit one by one or by batches
		//if err := c.Commit(c.Message().Rev); err != nil {
		//	log.Fatal(err)
		//}
	}
	
	if err := c.Err(); err != nil {
		log.Fatal(err)
	}

	// Consumer reached the head of the topic, wait for a new message arrive or ctx time out.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	c.Wait(ctx)
	cancel()
}
```