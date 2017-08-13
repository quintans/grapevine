# grapevine
brokerless cluster with service auto-discover using gomsg


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
time.Sleep(time.Second * 2)

<-peer.Request("GREETING", "Paulo", func(reply string) {
	fmt.Println("Reply:", reply)
})
```
