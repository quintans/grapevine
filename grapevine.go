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
	UuidSize       = 16
	PING           = "PING"
	PeerAddressKey = "peer.address"
)

type PeerListener func(*gomsg.Client)

type node struct {
	uuid            string
	client          *gomsg.Client
	debouncer       *tk.Debouncer
	beaconLastTime  time.Time
	beaconCountdown int
}

type Config struct {
	Addr   string  `json:"addr"`
	Uuid   tk.UUID `json:"uuid"`
	Beacon `json:"beacon"`
}

type Beacon struct {
	Addr            string        `json:"addr"`
	Name            string        `json:"name"`
	MaxDatagramSize int           `json:"maxDatagramSize"`
	Interval        time.Duration `json:"interval"`
	MaxInterval     time.Duration `json:"maxInterval"`
	// BeaconCountdown is the number of consecutive UDP pings inside the ping window
	// to reactivate the UDP health check
	Countdown int `json:"countdown"`
}

type Peer struct {
	sync.RWMutex
	*gomsg.Server

	cfg Config
	// peers that will be used to listen
	peers map[string]*node
	// the server will be used to send
	udpConn           *net.UDPConn
	handlers          map[string][]interface{}
	beaconTicker      *tk.Ticker
	newPeerListeners  map[uint64]PeerListener
	dropPeerListeners map[uint64]PeerListener
	listenersIdx      uint64
}

func NewPeer(cfg Config) *Peer {
	if cfg.Uuid.IsZero() {
		cfg.Uuid = tk.NewUUID()
	}

	// apply defaults
	var defaultConfig = Config{
		Addr: ":55000",
		Beacon: Beacon{
			MaxDatagramSize: 1024,
			Addr:            "224.0.0.1:9999",
			Name:            "grapevine",
			Interval:        time.Second,
			MaxInterval:     time.Second * 2,
			Countdown:       3,
		},
	}
	mergo.Merge(&cfg, defaultConfig)

	peer := &Peer{
		Server: gomsg.NewServer(),
		cfg:    cfg,
	}

	peer.reset()

	return peer
}

func (this *Peer) reset() {
	this.peers = make(map[string]*node)
	this.handlers = make(map[string][]interface{})
	this.newPeerListeners = make(map[uint64]PeerListener)
	this.dropPeerListeners = make(map[uint64]PeerListener)
	this.listenersIdx = 0
}

func (this *Peer) AddNewPeerListener(listener PeerListener) uint64 {
	this.Lock()
	defer this.Unlock()

	this.listenersIdx++
	this.newPeerListeners[this.listenersIdx] = listener
	return this.listenersIdx
}

// RemoveSendListener removes a previously added listener on send messages
func (this *Peer) RemoveNewPeerListener(idx uint64) {
	this.Lock()
	delete(this.newPeerListeners, idx)
	this.Unlock()
}

func (this *Peer) fireNewPeerListener(cli *gomsg.Client) {
	for _, listener := range clonePeerListeners(this.RWMutex, this.newPeerListeners) {
		listener(cli)
	}
}

func (this *Peer) AddDropPeerListener(listener PeerListener) uint64 {
	this.Lock()
	defer this.Unlock()

	this.listenersIdx++
	this.dropPeerListeners[this.listenersIdx] = listener
	return this.listenersIdx
}

// RemoveSendListener removes a previously added listener on send messages
func (this *Peer) RemoveDropPeerListener(idx uint64) {
	this.Lock()
	defer this.Unlock()

	delete(this.dropPeerListeners, idx)
}

func (this *Peer) fireDropPeerListener(cli *gomsg.Client) {
	for _, listener := range clonePeerListeners(this.RWMutex, this.dropPeerListeners) {
		listener(cli)
	}
}

func clonePeerListeners(mu sync.RWMutex, m map[uint64]PeerListener) []PeerListener {
	mu.RLock()
	arr := make([]PeerListener, len(m))
	var i = 0
	for _, v := range m {
		arr[i] = v
		i++
	}
	mu.RUnlock()

	return arr
}

func (peer *Peer) Config() Config {
	return peer.cfg
}

func (peer *Peer) Bind() <-chan error {
	peer.Logger().Infof("Binding Cluster=%s, UUID=%s at %s", peer.cfg.Beacon.Name, peer.cfg.Uuid, peer.cfg.Addr)

	// special case where we receive a targeted request
	// when a peer tries to check if I exist
	// because it did not received the beacon in time
	peer.Server.Handle(PING, func(uuid []byte) bool {
		return bytes.Compare(uuid, peer.cfg.Uuid.Bytes()) == 0
	})

	var err = peer.serveUDP(peer.beaconHandler)
	if err != nil {
		var cherr = make(chan error, 1)
		cherr <- err
		return cherr
	}
	peer.Server.AddBindListeners(func(l net.Listener) {
		peer.startBeacon()
	})

	return peer.Server.Listen(peer.cfg.Addr)
}

func (peer *Peer) checkPeer(uuid string, addr string) {
	peer.Lock()
	var connect = false
	if n := peer.peers[uuid]; n != nil {
		if addr != n.client.Address() {
			delete(peer.peers, uuid)
			peer.Logger().Infof("%s - OLD peer at %s", peer.cfg.Addr, addr)
			// client reconnected with another address
			n.client.Destroy()
			n.debouncer.Kill()
			connect = true
		} else {
			peer.checkBeacon(n)
		}
	} else {
		peer.Logger().Infof("%s - NEW peer at %s", peer.cfg.Addr, addr)
		connect = true
	}
	peer.Unlock()

	if connect {
		var err = peer.connectPeer(uuid, addr)
		if err != nil {
			peer.Logger().Errorf("Unable to connect to peer at %s: %+v", addr, err)
		}
	}
}

func (peer *Peer) checkBeacon(n *node) {
	if n.beaconCountdown == 0 {
		// this debouncer is only for UDP beacon when beaconCountdown == 0
		n.debouncer.Delay(nil)
	} else {
		var now = time.Now()
		if now.Sub(n.beaconLastTime) < peer.cfg.Beacon.MaxInterval {
			n.beaconCountdown--
		} else {
			n.beaconCountdown = peer.cfg.Beacon.Countdown
		}
		if n.beaconCountdown == 0 {
			// the client responded, switching to UDP
			peer.Logger().Infof("%s - Peer at %s responded. Switching to UDP listening", peer.cfg.Addr, n.client.Address())
			// kill the TCP health check
			n.debouncer.Kill()
			peer.healthCheckByUDP(n)
		}
		n.beaconLastTime = now
	}
}

func (peer *Peer) connectPeer(uuid string, addr string) error {
	var cli = gomsg.NewClient()
	// register peer public IP:Port
	var ip, err = gomsg.IP()
	if err != nil {
		return err
	}
	// copy metada
	var md = cli.Metadata()
	for k, v := range peer.Metadata() {
		md[k] = v
	}
	cli.Metadata()[PeerAddressKey] = ip + ":" + strconv.Itoa(peer.BindPort())

	cli.SetLogger(log.Wrap{peer.Logger().CallerAt(2), "{Client@" + addr + "}"})
	var n = &node{
		uuid:   uuid,
		client: cli,
	}
	peer.healthCheckByUDP(n)

	peer.Lock()
	peer.peers[uuid] = n
	peer.Unlock()

	// when it connects it will be already in peers
	var e = <-cli.Connect(addr)
	if e != nil {
		peer.Lock()
		delete(peer.peers, uuid)
		peer.Unlock()
		return e
	}

	peer.RLock()
	// apply all handlers
	for k, v := range peer.handlers {
		cli.Handle(k, v...)
	}
	peer.RUnlock()

	peer.listPeers(addr)

	peer.fireNewPeerListener(cli)

	return nil
}

func (peer *Peer) dropPeer(n *node) {
	peer.fireDropPeerListener(n.client)

	peer.Lock()
	peer.Logger().Infof("%s - Droping unresponsive peer at %s", peer.cfg.Addr, n.client.Address())
	n.client.Destroy()
	// no need to kill the debouncer. If this was called, it means it fired.
	n.debouncer = nil
	delete(peer.peers, n.uuid)
	peer.Unlock()

	peer.listPeers("")

}

func (peer *Peer) listPeers(addr string) {
	if peer.Logger().IsActive(log.INFO) {
		peer.RLock()
		var arr = make([]*node, len(peer.peers))
		var i = 0
		for _, n := range peer.peers {
			arr[i] = n
			i++
		}
		peer.RUnlock()

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

		peer.Logger().Infof("Peers of %s\n%s", peer.cfg.Beacon.Name, buf.String())
	}
}

// healthCheckByIP is the client that checks actively the remote peer
func (peer *Peer) healthCheckByTCP(n *node) {
	var uuid = n.client.RemoteUuid().Bytes()
	var ticker = tk.NewTicker(peer.cfg.Beacon.Interval, func(t time.Time) {
		<-n.client.RequestTimeout(PING, uuid, func(ok bool) {
			if ok {
				n.debouncer.Delay(nil)
			} else {
				peer.Logger().Debugf("Peer with UUID %x at %s no longer valid.", uuid, n.client.Address())
				n.debouncer.Kill()
			}
		}, peer.cfg.Beacon.Interval)
	})
	n.debouncer = tk.NewDebounce(peer.cfg.Beacon.MaxInterval, func(o interface{}) {
		peer.Logger().Debugf("%s - Failed to contact by TCP peer at %s.", peer.cfg.Addr, n.client.Address())
		peer.dropPeer(n)
	})
	n.debouncer.OnExit = func() {
		ticker.Stop()
	}
}

func (peer *Peer) healthCheckByUDP(n *node) {
	n.debouncer = tk.NewDebounce(peer.cfg.Beacon.MaxInterval, func(o interface{}) {
		// the client did not responded, switching to TCP
		peer.Logger().Debugf("%s - Silent peer at %s. Switching to TCP ping", peer.cfg.Addr, n.client.Address())
		n.beaconCountdown = peer.cfg.Beacon.Countdown
		peer.healthCheckByTCP(n)
	})
}

func (peer *Peer) beaconHandler(src *net.UDPAddr, n int, b []byte) {
	// starts with tag
	if bytes.HasPrefix(b, []byte(peer.cfg.Beacon.Name)) {
		var r = bytes.NewReader(b)
		r.Seek(int64(len(peer.cfg.Beacon.Name)), io.SeekStart)
		var uuid = make([]byte, UuidSize)
		r.Read(uuid)
		// ignore self
		if bytes.Compare(uuid, peer.cfg.Uuid.Bytes()) != 0 {
			var buf16 = make([]byte, 2)
			r.Read(buf16)
			var port = int(binary.LittleEndian.Uint16(buf16))
			peer.checkPeer(hex.EncodeToString(uuid), src.IP.String()+":"+strconv.Itoa(port))
		}
	}
}

func (peer *Peer) startBeacon() error {
	addr, err := net.ResolveUDPAddr("udp", peer.cfg.Beacon.Addr)
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
	buf.WriteString(peer.cfg.Beacon.Name)
	buf.Write(peer.cfg.Uuid.Bytes())
	buf.Write(buf16)

	var data = buf.Bytes()
	if peer.beaconTicker != nil {
		peer.beaconTicker.Stop()
	}
	peer.Logger().Infof("Heartbeat on %s every %s",
		peer.cfg.Beacon.Addr,
		peer.cfg.Beacon.Interval,
	)
	peer.beaconTicker = tk.NewDelayedTicker(0, peer.cfg.Beacon.Interval, func(t time.Time) {
		var _, err = c.Write(data)
		if err != nil {
			peer.Logger().Errorf("Unable to write to UDP %s. Error: %+v", peer.cfg.Beacon.Addr, faults.Wrap(err))
		}
	})

	return nil
}

func (peer *Peer) serveUDP(hnd func(*net.UDPAddr, int, []byte)) error {
	addr, err := net.ResolveUDPAddr("udp", peer.cfg.Beacon.Addr)
	if err != nil {
		return faults.Wrap(err)
	}

	l, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		return faults.Wrap(err)
	}
	l.SetReadBuffer(peer.cfg.Beacon.MaxDatagramSize)
	peer.udpConn = l
	go func() {
		var payload = make([]byte, peer.cfg.Beacon.MaxDatagramSize)
		for {
			n, src, err := l.ReadFromUDP(payload)
			if peer.udpConn == nil {
				return
			} else if err != nil {
				peer.Logger().Errorf("%s - ReadFromUDP failed: %+v", peer.cfg.Addr, faults.Wrap(err))
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
	peer.handlers[name] = hnd
	for _, v := range peer.peers {
		v.client.Handle(name, hnd)
	}
	peer.Unlock()
}

func (peer *Peer) Cancel(name string) {
	peer.Logger().Infof("Canceling handler for %s", name)

	peer.Lock()
	delete(peer.handlers, name)
	for _, v := range peer.peers {
		v.client.Cancel(name)
	}
	peer.Unlock()
}

func (peer *Peer) Destroy() {
	peer.Server.Destroy()
	var conn = peer.udpConn
	peer.udpConn = nil
	if conn != nil {
		conn.Close()
	}
	if peer.beaconTicker != nil {
		peer.beaconTicker.Stop()
	}
	peer.beaconTicker = nil

	peer.Lock()
	for _, v := range peer.peers {
		v.debouncer.Kill()
		v.client.Destroy()
	}
	peer.reset()
	peer.Unlock()
}
