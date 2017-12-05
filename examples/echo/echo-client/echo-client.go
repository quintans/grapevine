package main

import (
	"flag"

	"github.com/quintans/gomsg"
	"github.com/quintans/grapevine"
	"github.com/quintans/grapevine/examples/echo"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.INFO).ShowCaller(true)
}

func main() {
	var name = flag.String("name", "Paulo", "echo-client name")
	var addr = flag.String("addr", ":55001", "echo-client address [ip]:port")
	flag.Parse()

	var logger = log.LoggerFor("echo-client")
	var peer = grapevine.NewPeer(grapevine.Config{})
	peer.SetLogger(logger)

	// listen to the appearence of topics
	peer.AddNewTopicListener(func(event gomsg.TopicEvent) {
		logger.Infof("NEW remote topic: %s %s", event.Name)
		switch event.Name {
		case shared.SERVICE_ECHO:
			// as soon we have a service available, we call, and when we get reply we shutdown
			peer.Request(shared.SERVICE_ECHO, *name, func(reply string) {
				logger.Infof("The server replied '%s'", reply)
				peer.Destroy()
			})
		}
	})

	logger.Infof("Echo client at %s", *addr)
	if err := <-peer.Bind(*addr); err != nil {
		panic(err)
	}
}
