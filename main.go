package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const TCP = "tcp"

const (
	MinElectionTimeout = 150
	MaxElectionTimeout = 300
	HeartbeatTimeout   = time.Millisecond * 50
)

type StateType int

const (
	Leader StateType = iota + 1
	Candidate
	Follower
)

func (t StateType) String() string {
	switch t {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	case Follower:
		return "follower"
	default:
		return "unknown"
	}
}

type MessageType int

const (
	// RAFT message types
	ReqVote MessageType = iota + 1
	GrantedVote
	Heartbeat

	// network bootstrapping message types
	TellMeKnownNodes
	KnownNodes
	Hello
)

type Conn struct {
	conn     net.Conn
	serverIP string
}

type ConnectionsStorage struct {
	conns map[string]*Conn
	sync.Mutex
}

type Message struct {
	Type       MessageType `json:"type"`
	KnownNodes []string    `json:"known_nodes"`
	MyAddr     string      `json:"my_addr"`
}

type Philosopher struct {
	// raft
	electionTimeout time.Duration
	heartbeat       time.Time
	state           StateType
	granted         int
	majority        int
	newState        chan StateType
	stateChanged    chan struct{}

	port, bootnode string
	foundNewNode   chan string
	addr           string
	connsStorage   *ConnectionsStorage
	bootstrapped   chan struct{}
}

func (s *ConnectionsStorage) Save(addr string, conn net.Conn) {
	s.Lock()
	s.conns[conn.RemoteAddr().String()] = &Conn{conn: conn, serverIP: addr}
	s.Unlock()
}

func (s *ConnectionsStorage) Delete(conn net.Conn) {
	s.Lock()
	delete(s.conns, conn.RemoteAddr().String())
	s.Unlock()
}

func NewPhilosopher(port, bootnode string) *Philosopher {
	p := &Philosopher{
		electionTimeout: time.Millisecond * time.Duration(rand.Intn(MaxElectionTimeout-MinElectionTimeout)+MinElectionTimeout),
		newState:        make(chan StateType),
		stateChanged:    make(chan struct{}),

		connsStorage: &ConnectionsStorage{
			conns: map[string]*Conn{},
		},
		bootstrapped: make(chan struct{}),
		foundNewNode: make(chan string),
		addr:         fmt.Sprintf("127.0.0.1:%s", port),
		port:         port, bootnode: bootnode,
	}
	p.UpdateMajority()
	return p
}

func (p *Philosopher) ChangeRAFTState(newState StateType) {
	log.Debug().Msgf("change state to %s", newState)
	p.stateChanged <- struct{}{}
	p.state = newState
	p.newState <- newState
	log.Debug().Msgf("state changed to %s", p.state)
}

func (p *Philosopher) Run() {
	go p.RunTCPServer()

	// goroutine for manage/store tcp connection
	go func() {
		// connect to bootnode if exists
		go p.ConnectToNode(p.bootnode, true)

		for {
			select {
			case addr := <-p.foundNewNode:
				go p.ConnectToNode(addr, false)
			}
		}
	}()

	<-p.bootstrapped
	go p.RAFT()
	go p.Philosophize()

	select {}
}

func (p *Philosopher) Philosophize() {}

func (p *Philosopher) RAFT() {
	// TODO add comment here
	go func() {
		go func() {
			_ = <-p.stateChanged
		}()
		p.ChangeRAFTState(Follower)
	}()

	for {
		select {
		case newState := <-p.newState:
			switch newState {
			case Leader:
				go func() {
					heartbeatTickerC := time.NewTicker(HeartbeatTimeout).C

					for {
						select {
						case <-p.stateChanged:
							log.Debug().Msg("leave from leader state")
							return
						case <-heartbeatTickerC:
							p.SendToAll(&Message{Type: Heartbeat})
						}
					}
				}()
			case Candidate:
				go func() {
					// vote for ourselves
					p.GrantedVote()
					p.SendToAll(&Message{Type: ReqVote})

					electionTicker := time.NewTicker(p.electionTimeout)

					for {
						select {
						case <-p.stateChanged:
							log.Debug().Msg("leave from candidate state")
							return
						case <-electionTicker.C:
							if p.ElectionTimeoutNotPassed() {
								electionTicker.Stop()
								go p.ChangeRAFTState(Follower)
							} else {
								// TODO check this logic. is it need or is it correct?
								p.GrantedVote()
								p.SendToAll(&Message{Type: ReqVote})
								// fmt.Println("NOT PASSED")
							}
						}
					}
				}()
			case Follower:
				go func() {
					electionTicker := time.NewTicker(p.electionTimeout)

					for {
						select {
						case <-p.stateChanged:
							log.Debug().Msg("leave from follower state")
							return
						case <-electionTicker.C:
							if p.ElectionTimeoutNotPassed() {
								continue
							}
							electionTicker.Stop()
							go p.ChangeRAFTState(Candidate)
						}
					}
				}()
			}
		}
	}
}

func (p *Philosopher) ElectionTimeoutNotPassed() bool {
	if time.Since(p.heartbeat) <= p.electionTimeout {
		return true
	}
	return false
}

func (p *Philosopher) Reply(conn net.Conn, message *Message) {
	buf, _ := json.Marshal(message)
	buf = append(buf, '\n')
	_, _ = conn.Write(buf)
}

func (p *Philosopher) SendToAll(message *Message) {
	for _, conn := range p.connsStorage.conns {
		p.Reply(conn.conn, message)
	}
}

func (p *Philosopher) ConnectToNode(addr string, tellMeKnownNodes bool) {
	if addr == "" {
		p.bootstrapped <- struct{}{}
		return
	}

	conn, err := net.Dial(TCP, addr)
	if err != nil {
		log.Fatal().Err(err).Msg("connect to bootnode error")
	}

	p.NewIncomingConnection(addr, conn)

	go p.HandleTCPConnection(conn)

	if tellMeKnownNodes {
		p.Reply(conn, &Message{Type: TellMeKnownNodes, MyAddr: p.addr})
	} else {
		p.Reply(conn, &Message{Type: Hello, MyAddr: p.addr})
	}

	select {}
}

func (p *Philosopher) RunTCPServer() {
	ln, err := net.Listen(TCP, ":"+p.port)
	if err != nil {
		log.Fatal().Err(err).Msg("listen tcp server error")
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal().Err(err).Msg("accept new connection error")
		}

		go p.HandleTCPConnection(conn)
	}
}

func (p *Philosopher) NewIncomingConnection(addr string, conn net.Conn) {
	log.Trace().Msgf("new connection %s", addr)
	p.connsStorage.Save(addr, conn)
	p.UpdateMajority()
}

func (p *Philosopher) UpdateMajority() {
	p.majority = int(math.Ceil(float64(len(p.connsStorage.conns))/2)) + 1
}

func (p *Philosopher) LoseConnection(conn net.Conn) {
	log.Trace().Msgf("connection lost %s", conn.RemoteAddr())
	p.connsStorage.Delete(conn)
	p.UpdateMajority()
}

// externalInitiator means that new connection initiated by external node
func (p *Philosopher) HandleTCPConnection(conn net.Conn) {
	for {
		buf, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			p.LoseConnection(conn)
			return
		}

		message := &Message{}
		_ = json.Unmarshal(buf, message)

		go p.ProcessReceivedMessage(conn, message)
	}
}

func (p *Philosopher) GrantedVote() {
	p.granted++
	if p.granted >= p.majority {
		p.granted = 0
		go p.ChangeRAFTState(Leader)
	}
}

func (p *Philosopher) ProcessReceivedMessage(conn net.Conn, message *Message) {
	switch message.Type {
	case Heartbeat:
		p.heartbeat = time.Now()
	case ReqVote:
		if p.ElectionTimeoutNotPassed() {
			p.Reply(conn, &Message{Type: GrantedVote})
		}
	case GrantedVote:
		if p.state == Candidate {
			p.GrantedVote()
		}
	case KnownNodes:
		for _, addr := range message.KnownNodes {
			if addr == p.addr {
				continue
			}
			p.foundNewNode <- addr
		}
		p.bootstrapped <- struct{}{}
	case Hello:
		p.NewIncomingConnection(message.MyAddr, conn)
	case TellMeKnownNodes:
		p.NewIncomingConnection(message.MyAddr, conn)
		var addrs []string
		for _, conn := range p.connsStorage.conns {
			addrs = append(addrs, conn.serverIP)
		}
		p.Reply(conn, &Message{Type: KnownNodes, KnownNodes: addrs})
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	log.Logger = log.
		Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		With().
		Caller().
		Logger().
		Level(zerolog.TraceLevel)

	port := flag.String("port", "", "set tcp port")
	bootnode := flag.String("boot", "", "set bootnode address")
	flag.Parse()

	p := NewPhilosopher(*port, *bootnode)
	go p.Run()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	select {
	case <-interrupt:
		log.Info().Msg("interrupted")
	case <-ctx.Done():
		log.Info().Msg(ctx.Err().Error())
	}
}
