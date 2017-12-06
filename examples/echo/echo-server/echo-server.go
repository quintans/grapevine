package main

import (
	"github.com/quintans/grapevine"
	"github.com/quintans/grapevine/examples/echo"
	"github.com/quintans/toolkit"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.INFO).ShowCaller(true)
}

func main() {
	var config grapevine.Config
	if err := toolkit.LoadConfiguration(&config, "echo-service.json", true); err != nil {
		panic(err)
	}
	var logger = log.LoggerFor("echo-service")
	var peer = grapevine.NewPeer(config)
	peer.SetLogger(logger)
	peer.Handle(shared.SERVICE_ECHO, func(greeting string) string {
		return "Hi " + greeting + "!"
	})

	if err := <-peer.Bind(); err != nil {
		panic(err)
	}
}
