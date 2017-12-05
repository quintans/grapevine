package main

import (
	"flag"

	"github.com/quintans/grapevine"
	"github.com/quintans/grapevine/examples/echo"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.INFO).ShowCaller(true)
}

func main() {
	var addr = flag.String("addr", ":55000", "echo-server address [ip]:port")
	flag.Parse()

	var logger = log.LoggerFor("echo-service")
	var peer = grapevine.NewPeer(grapevine.Config{})
	peer.SetLogger(logger)
	peer.Handle(shared.SERVICE_ECHO, func(greeting string) string {
		return "Hi " + greeting + "!"
	})

	logger.Infof("Echo server at %s", *addr)
	if err := <-peer.Bind(*addr); err != nil {
		panic(err)
	}
}
