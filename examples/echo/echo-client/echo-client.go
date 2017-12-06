package main

import (
	"flag"

	"github.com/quintans/gomsg"
	"github.com/quintans/grapevine"
	"github.com/quintans/grapevine/examples/echo"
	"github.com/quintans/toolkit"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.INFO).ShowCaller(true)
}

func main() {
	var name = flag.String("name", "Paulo", "echo-client name")
	flag.Parse()

	var logger = log.LoggerFor("echo-client")
	var config grapevine.Config
	if err := toolkit.LoadConfiguration(&config, "echo-client.json", true); err != nil {
		panic(err)
	}
	var peer = grapevine.NewPeer(config)
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

	if err := <-peer.Bind(); err != nil {
		panic(err)
	}
}
