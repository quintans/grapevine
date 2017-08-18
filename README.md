# grapevine
brokerless cluster with service auto-discover using gomsg

The cluster is implemented using **gomsg**.

It is very easy to use.

The provider
```go
var peer = grapevine.NewPeer(grapevine.Config{})
peer.Handle("GREETING", func(name string) string {
	return "Hello " + name
})
if err := <-peer.Bind(":7000"); err != nil {
	panic(err)
}
```


The consumer
```go
var peer = grapevine.NewPeer(grapevine.Config{})
go func() {
	if err := <-peer.Bind(":8000"); err != nil {
		panic(err)
	}
}()

// give time for the Peer to connect
time.Sleep(time.Second)

<-peer.Request("GREETING", "Paulo", func(reply string) {
	fmt.Println("Reply:", reply)
})
```

### Details
Auto discovery is implemented by a UDP heartbeat that every node emits.
When a node receives a heartbeat from a new node, it connects to it.

A node is implemented with a **gomsg.Server** (to make calls) and a list of
**gomsg.Client** (to listen), one for each cluster node.
