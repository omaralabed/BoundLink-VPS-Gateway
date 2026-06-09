// Package tunnel receives bounded UDP traffic on the VPS and reassembles it.
package tunnel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/boundlink/vps/pkg/protocol"
	"github.com/boundlink/vps/pkg/reassembler"
)

// Server runs on the VPS: receives tunneled packets, reassembles, forwards.
type Server struct {
	listen     *net.UDPConn
	reasm      *reassembler.Reassembler
	egress     *net.UDPConn
	egressAddr *net.UDPAddr
}

// ServerConfig configures the VPS tunnel server.
type ServerConfig struct {
	ListenPort int
	EgressAddr string
	Reassembly reassembler.Config
}

// NewServer creates a VPS tunnel server.
func NewServer(cfg ServerConfig) (*Server, error) {
	ln, err := net.ListenUDP("udp", &net.UDPAddr{Port: cfg.ListenPort})
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	eg, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("egress socket: %w", err)
	}
	s := &Server{
		listen: ln,
		reasm:  reassembler.New(cfg.Reassembly),
		egress: eg,
	}
	if cfg.EgressAddr != "" {
		addr, err := net.ResolveUDPAddr("udp", cfg.EgressAddr)
		if err != nil {
			ln.Close()
			eg.Close()
			return nil, err
		}
		s.egressAddr = addr
	} else {
		log.Printf("vps: egress_addr unset — using per-packet DestIP/DestPort from Pi")
	}
	return s, nil
}

// StartBackground runs periodic reorder/NACK sweeps until ctx is cancelled.
func (s *Server) StartBackground(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				for _, sweep := range s.reasm.Sweep(now) {
					for _, p := range sweep.Result.Ready {
						s.egressPacket(p)
					}
					s.sendControl(sweep.SessionID, sweep.Result)
				}
			}
		}
	}()
}

// Run blocks, processing incoming tunnel packets.
func (s *Server) Run(ctx context.Context) error {
	buf := make([]byte, protocol.HeaderLen+protocol.MaxPayload)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		_ = s.listen.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, remote, err := s.listen.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		pkt, err := protocol.Decode(buf[:n])
		if err != nil {
			log.Printf("vps: drop invalid packet: %v", err)
			continue
		}
		if pkt.IsACK() || pkt.IsNACK() {
			continue
		}
		res := s.reasm.Process(pkt, remote)
		for _, p := range res.Ready {
			s.egressPacket(p)
		}
		s.sendControl(pkt.SessionID, res)
	}
}

func (s *Server) sendControl(sessionID uint32, res reassembler.Result) {
	addrs := s.reasm.SessionAddrs(sessionID)
	if len(addrs) == 0 {
		return
	}
	if res.HasAck {
		ack := protocol.NewACKPacket(sessionID, res.AckSeq)
		s.writeControl(ack, addrs)
	}
	if len(res.NackSeq) > 0 {
		nack, err := protocol.NewNACKPacket(sessionID, res.NackSeq)
		if err != nil {
			log.Printf("vps: nack build: %v", err)
			return
		}
		s.writeControl(nack, addrs)
	}
}

func (s *Server) writeControl(pkt *protocol.Packet, addrs []*net.UDPAddr) {
	buf := make([]byte, protocol.HeaderLen+protocol.MaxPayload)
	n, err := pkt.Encode(buf)
	if err != nil {
		log.Printf("vps: control encode: %v", err)
		return
	}
	for _, addr := range addrs {
		if _, err := s.listen.WriteToUDP(buf[:n], addr); err != nil {
			log.Printf("vps: control send %s: %v", addr, err)
		}
	}
}

func (s *Server) egressPacket(pkt *protocol.Packet) {
	var addr *net.UDPAddr
	if pkt.HasDest() {
		addr = &net.UDPAddr{IP: pkt.DestIP, Port: int(pkt.DestPort)}
	} else if s.egressAddr != nil {
		addr = s.egressAddr
	}
	if addr == nil {
		log.Printf("vps: drop packet seq=%d: no egress destination", pkt.Sequence)
		return
	}
	if _, err := s.egress.WriteToUDP(pkt.Payload, addr); err != nil {
		log.Printf("vps: egress error: %v", err)
	}
}

// Close shuts down the server.
func (s *Server) Close() error {
	var err error
	if s.egress != nil {
		err = s.egress.Close()
	}
	if cerr := s.listen.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// ListenAddr returns the bound address.
func (s *Server) ListenAddr() net.Addr { return s.listen.LocalAddr() }

// ErrClosed is returned when Run exits after intentional shutdown.
var ErrClosed = errors.New("server closed")
