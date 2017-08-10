# grapevine
brokerless cluster with service auto-discover using gomsg


It is very easy to use.

The Producer
```go
var producer = grapevine.NewPeer(grapevine.Config{})
producer.Handle("GREETING", func(name string) string {
	return "Hello " + name
})
if err := <-producer.Bind(":7000"); err != nil {
	panic(err)
}
```


The consumer
```go
var consumer = grapevine.NewPeer(grapevine.Config{})
go func() {
	if err := <-consumer.Bind(":8000"); err != nil {
		panic(err)
	}
}()

// give time for the Peer to connect
time.Sleep(time.Second * 2)

<-consumer.Request("GREETING", "Paulo", func(reply string) {
	fmt.Println("Reply:", reply)
})
```
