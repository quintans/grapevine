package grapevine

import (
	"fmt"
	"testing"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/grapevine"
	"github.com/quintans/toolkit/log"
)

func wait() {
	time.Sleep(time.Millisecond * 100)
}

func init() {
	log.Register("/", log.INFO).ShowCaller(true)
}

const (
	SERVICE_GREETING = "GREETING"
)

var codec = gomsg.JsonCodec{}

func TestGrapevine(t *testing.T) {
	var mw = func(r *gomsg.Request) {
		fmt.Println("##### Calling endpoint #####", r.Name)
		defer fmt.Println("##### Called endpoint #####", r.Name)
		r.Next()
	}

	var greet = 0
	var peer1 = grapevine.NewPeer(grapevine.Config{})
	peer1.Handle(SERVICE_GREETING, func(greeting string) string {
		greet++
		return "#1: hi " + greeting
	})
	peer1.AddNewTopicListener(func(event gomsg.TopicEvent) {
		fmt.Println("=====> #1: remote topic: ", event.SourceAddr, event.Name)
	})
	peer1.Bind(":7001")

	/*
		var cli2 = grapevine.NewPeer(uuid())
		cli2.Handle(SERVICE_GREETING, func(greeting string) string {
			return "#2: hi " + greeting
		})
		cli2.Connect(":7002")
	*/

	wait()

	var uuid3 = gomsg.NewUUID()
	var cfg3 = grapevine.Config{Uuid: uuid3}
	var peer3 = grapevine.NewPeer(cfg3)
	peer3.Handle(SERVICE_GREETING, mw, func(r *gomsg.Request) {
		fmt.Println("=====> Calling SERVICE_GREETING #3")
		greet++
		var greeting string
		codec.Decode(r.Payload(), &greeting)
		// return "hi from #3"
		// direct in json format because I am lazy (it will be decoded)
		r.SetReply([]byte("\"#3: hi " + greeting + "\""))
	})
	peer3.Bind(":7003")
	wait()

	time.Sleep(time.Second * 2)

	var replies = 0
	<-peer1.RequestAll(SERVICE_GREETING, "#1", func(reply string) {
		replies++
		var str = reply
		if reply == "" {
			str = "[END]"
		}
		fmt.Println("=====>", str)
	})
	if replies != 2 {
		t.Fatal("ERROR =====> expected 2 replies, got", replies)
	}
	if greet != 1 {
		t.Fatal("ERROR =====> expected 1 greet, got", greet)
	}

	// replies should rotate
	/*
		for i := 0; i < 3; i++ {
			<-cli2.Request(SERVICE_GREETING, "#2", func(r gomsg.Response) {
				fmt.Println("=====>", string(r.Reply()))
			})
		}
	*/

	peer3.Destroy()
	fmt.Println("Waiting...")
	time.Sleep(time.Second * 5)
	// does it reconnect?
	peer3 = grapevine.NewPeer(cfg3)
	peer3.Bind(":7003")
	time.Sleep(time.Second * 7)
}
