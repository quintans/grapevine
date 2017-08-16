// THIS IS A WORK IN PROGRESS
// based on http://zguide.zeromq.org/php:chapter8#Detecting-Disappearances

// The Peer will listen on gomsg.Client and call on gomsg.Server

package grapevine

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/imdario/mergo"
	"github.com/quintans/gomsg"
	tk "github.com/quintans/toolkit"
	"github.com/quintans/toolkit/faults"
	"github.com/quintans/toolkit/log"
)

// defaults
const (
	UuidSize = 16
	PING     = "PING"
)

type node struct {
	uuid            string
	client          *gomsg.Client
	debouncer       *tk.Debouncer
	beaconLastTime  time.Time
	beaconCountdown int
}

type Config struct {
	Uuid                  []byte
	BeaconAddr            string
	BeaconName            string
	BeaconMaxDatagramSize int
	BeaconInterval        time.Duration
	BeaconMaxInterval     time.Duration
	// BeaconCountdown is the number of consecutive pings inside the ping window
	// to reactivate the UDP health check
	BeaconCountdown int
}

type Peer struct {
	sync.RWMutex
	*gomsg.Server

	cfg     Config
	tcpAddr string
	// peers ones will be used to listen
	peers map[string]*node
	// the server will be used to send
	udpConn      *net.UDPConn
	handlers     map[string][]interface{}
	beaconTicker *tk.Ticker
}

func NewPeer(cfg Config) *Peer {
	if cfg.Uuid == nil {
		cfg.Uuid = tk.NewUUID()
	}

	// apply defaults
	var defaultConfig = Config{
		BeaconMaxDatagramSize: 1024,
		BeaconAddr:            "224.0.0.1:9999",
		BeaconName:            "Beacon",
		BeaconInterval:        time.Second,
		BeaconMaxInterval:     time.Second * 2,
		BeaconCountdown:       3,
	}
	mergo.Merge(&cfg, defaultConfig)

	peer := &Peer{
		Server:   gomsg.NewServer(),
		cfg:      cfg,
		peers:    make(map[string]*node),
		handlers: make(map[string][]interface{}),
	}

	return peer
}

func (peer *Peer) Config() Config {
	return peer.cfg
}

func (peer *Peer) Bind(tcpAddr string) <-chan error {
	peer.tcpAddr = tcpAddr
	peer.Logger().Infof("Binding Cluster=%s, UUID=%X at %s", peer.cfg.BeaconName, peer.cfg.Uuid, tcpAddr)

	// special case where we receive a targeted request
	// when a peer tries to check if I exist
	// because it did not received the beacon in time
	peer.Server.Handle(PING, func() {})

	var err = peer.serveUDP(peer.beaconHandler)
	if err != nil {
		var cherr = make(chan error, 1)
		cherr <- err
		return cherr
	}
	peer.Server.AddBindListeners(func(l net.Listener) {
		peer.startBeacon()
	})

	return peer.Server.Listen(tcpAddr)
}

func (peer *Peer) checkPeer(uuid string, addr string) {
	peer.Lock()
	defer peer.Unlock()

	if n := peer.peers[uuid]; n != nil {
		if addr != n.client.Address() {
			peer.Logger().Infof("%s - OLD peer at %s", peer.tcpAddr, addr)
			// client reconnected with another address
			n.client.Destroy()
			n.debouncer.Kill()
			delete(peer.peers, uuid)
			peer.connectPeer(uuid, addr)
		} else {
			peer.checkBeacon(n)
		}
	} else {
		peer.Logger().Infof("%s - NEW peer at %s", peer.tcpAddr, addr)
		peer.connectPeer(uuid, addr)
	}
}

func (peer *Peer) checkBeacon(n *node) {
	if n.beaconCountdown == 0 {
		// this debouncer is only for UDP beacon when beaconCountdown == 0
		n.debouncer.Delay(nil)
	} else {
		var now = time.Now()
		if now.Sub(n.beaconLastTime) < peer.cfg.BeaconMaxInterval {
			n.beaconCountdown--
		} else {
			n.beaconCountdown = peer.cfg.BeaconCountdown
		}
		if n.beaconCountdown == 0 {
			// the client responded, switching to UDP
			peer.Logger().Infof("%s - Peer at %s responded. Switching to UDP listening", peer.tcpAddr, n.client.Address())
			// kill the TCP health check
			n.debouncer.Kill()
			peer.healthCheckByUDP(n)
		}
		n.beaconLastTime = now
	}
}

func (peer *Peer) connectPeer(uuid string, addr string) error {
	var cli = gomsg.NewClient()
	cli.SetLogger(peer.Logger())
	var e = <-cli.Connect(addr)
	if e != nil {
		peer.Logger().Errorf("%s - Unable to connect to %s", peer.tcpAddr, addr)
		return e
	}
	var n = &node{
		uuid:   uuid,
		client: cli,
	}
	peer.healthCheckByUDP(n)
	peer.peers[uuid] = n

	// apply all handlers
	for k, v := range peer.handlers {
		cli.Handle(k, v...)
	}

	peer.listPeers(addr)

	return nil
}

func (peer *Peer) dropPeer(n *node) {
	peer.Lock()
	defer peer.Unlock()

	peer.Logger().Infof("%s - Droping unresponsive peer at %s", peer.tcpAddr, n.client.Address())
	n.client.Destroy()
	n.debouncer = nil
	delete(peer.peers, n.uuid)

	peer.listPeers("")
}

func (peer *Peer) listPeers(addr string) {
	if peer.Logger().IsActive(log.INFO) {
		var arr = make([]*node, len(peer.peers))
		var i = 0
		for _, n := range peer.peers {
			arr[i] = n
			i++
		}
		sort.Slice(arr, func(i, j int) bool {
			return strings.Compare(arr[i].client.Address(), arr[j].client.Address()) < 0
		})

		var buf bytes.Buffer
		buf.WriteString(" {\n    ")
		var peerAddr string
		var ip, err = gomsg.IP()
		if err == nil {
			peerAddr = fmt.Sprintf("%s:%d", ip, peer.BindPort())
		} else {
			peerAddr = peer.BindAddress().String()
		}
		buf.WriteString(peerAddr)
		buf.WriteString(" (this)\n")
		for _, n := range arr {
			buf.WriteString("    ")
			var a = n.client.Address()
			buf.WriteString(a)
			if a == addr {
				buf.WriteString(" (NEW)")
			}
			buf.WriteString("\n")
		}
		buf.WriteString(" }\n")

		peer.Logger().Infof("Peers of %s\n%s", peer.cfg.BeaconName, buf.String())
	}
}

// healthCheckByIP is the client that checks actively the remote peer
func (peer *Peer) healthCheckByTCP(n *node) {
	var ticker = tk.NewTicker(peer.cfg.BeaconInterval, func(t time.Time) {
		<-n.client.RequestTimeout(PING, nil, func() {
			n.debouncer.Delay(nil)
		}, peer.cfg.BeaconInterval)
	})
	n.debouncer = tk.NewDebounce(peer.cfg.BeaconMaxInterval, func(o interface{}) {
		peer.Logger().Debugf("%s - Failed to contact by TCP peer at %s.", peer.tcpAddr, n.client.Address())
		peer.dropPeer(n)
	})
	n.debouncer.OnExit = func() {
		ticker.Stop()
	}
}

func (peer *Peer) healthCheckByUDP(n *node) {
	n.debouncer = tk.NewDebounce(peer.cfg.BeaconMaxInterval, func(o interface{}) {
		// the client did not responded, switching to TCP
		peer.Logger().Debugf("%s - Silent peer at %s. Switching to TCP ping", peer.tcpAddr, n.client.Address())
		n.beaconCountdown = peer.cfg.BeaconCountdown
		peer.healthCheckByTCP(n)
	})
}

func (peer *Peer) beaconHandler(src *net.UDPAddr, n int, b []byte) {
	// starts with tag
	if bytes.HasPrefix(b, []byte(peer.cfg.BeaconName)) {
		var r = bytes.NewReader(b)
		r.Seek(int64(len(peer.cfg.BeaconName)), io.SeekStart)
		var uuid = make([]byte, UuidSize)
		r.Read(uuid)
		// ignore self
		if bytes.Compare(uuid, peer.cfg.Uuid) != 0 {
			var buf16 = make([]byte, 2)
			r.Read(buf16)
			var port = int(binary.LittleEndian.Uint16(buf16))
			peer.checkPeer(hex.EncodeToString(uuid), src.IP.String()+":"+strconv.Itoa(port))
		}
	}
}

func (peer *Peer) startBeacon() error {
	addr, err := net.ResolveUDPAddr("udp", peer.cfg.BeaconAddr)
	if err != nil {
		return faults.Wrap(err)
	}
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return faults.Wrap(err)
	}
	var buf16 = make([]byte, 2)
	var port = uint16(peer.Server.BindPort())
	binary.LittleEndian.PutUint16(buf16, port)

	var buf bytes.Buffer
	buf.WriteString(peer.cfg.BeaconName)
	buf.Write(peer.cfg.Uuid)
	buf.Write(buf16)

	var data = buf.Bytes()
	if peer.beaconTicker != nil {
		peer.beaconTicker.Stop()
	}
	peer.Logger().Infof("Heartbeat on %s every %s",
		peer.cfg.BeaconAddr,
		peer.cfg.BeaconInterval)
	peer.beaconTicker = tk.NewDelayedTicker(0, peer.cfg.BeaconInterval, func(t time.Time) {
		var _, err = c.Write(data)
		if err != nil {
			peer.Logger().Errorf("Unable to write to UDP %s. Error: %+v", peer.cfg.BeaconAddr, faults.Wrap(err))
		}
	})

	return nil
}

func (peer *Peer) serveUDP(hnd func(*net.UDPAddr, int, []byte)) error {
	addr, err := net.ResolveUDPAddr("udp", peer.cfg.BeaconAddr)
	if err != nil {
		return faults.Wrap(err)
	}

	l, err := net.ListenUDP("udp", addr)
	if err != nil {
		return faults.Wrap(err)
	}
	l.SetReadBuffer(peer.cfg.BeaconMaxDatagramSize)
	peer.udpConn = l
	go func() {
		for {
			var payload = make([]byte, peer.cfg.BeaconMaxDatagramSize)
			n, src, err := l.ReadFromUDP(payload)
			if peer.udpConn == nil {
				return
			} else if err != nil {
				peer.Logger().Errorf("%s - ReadFromUDP failed: %+v", peer.tcpAddr, faults.Wrap(err))
				return
			}
			hnd(src, n, payload)
		}
	}()
	return nil
}

func (peer *Peer) Handle(name string, hnd ...interface{}) {
	peer.Logger().Infof("Registering handler for %s", name)
	peer.Lock()
	defer peer.Unlock()

	peer.handlers[name] = hnd
	for _, v := range peer.peers {
		v.client.Handle(name, hnd)
	}
}

func (peer *Peer) Cancel(name string) {
	peer.Logger().Infof("Canceling handler for %s", name)
	peer.Lock()
	defer peer.Unlock()

	for _, v := range peer.peers {
		v.client.Cancel(name)
	}
}

func (peer *Peer) Destroy() {
	peer.Server.Destroy()
	var conn = peer.udpConn
	peer.udpConn = nil
	if conn != nil {
		conn.Close()
	}
	peer.Lock()
	defer peer.Unlock()
	for _, v := range peer.peers {
		v.debouncer.Kill()
		v.client.Destroy()
	}
	peer.peers = make(map[string]*node)
	if peer.beaconTicker != nil {
		peer.beaconTicker.Stop()
	}
	peer.beaconTicker = nil
}
