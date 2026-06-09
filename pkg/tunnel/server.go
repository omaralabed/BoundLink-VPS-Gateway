// Package tunnel receives bounded UDP traffic on the VPS and reassembles it.
package tunnel

import (
	"fmt"
	"log"
	"net"

	"github.com/boundlink/vps/pkg/protocol"
	"github.com/boundlink/vps/pkg/reassembler"
)

// Server runs on the VPS: receives tunneled packets, reassembles, forwards.
type Server struct {
	listen     *net.UDPConn
	reasm      *reassembler.Reassembler
	egress     *net.UDPConn
	egressAddr *net.UDPAddr
	encodeBuf  []byte
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
	s := &Server{
		listen:    ln,
		reasm:     reassembler.New(cfg.Reassembly),
		encodeBuf: make([]byte, protocol.HeaderLen+protocol.MaxPayload),
	}
	if cfg.EgressAddr != "" {
		addr, err := net.ResolveUDPAddr("udp", cfg.EgressAddr)
		if err != nil {
			ln.Close()
			return nil, err
		}
		s.egressAddr = addr
		eg, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
		if err != nil {
			ln.Close()
			return nil, err
		}
		s.egress = eg
	}
	return s, nil
}

// Run blocks, processing incoming tunnel packets.
func (s *Server) Run() error {
	buf := make([]byte, protocol.HeaderLen+protocol.MaxPayload)
	for {
		n, remote, err := s.listen.ReadFromUDP(buf)
		if err != nil {
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
	if res.AckSeq > 0 {
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
	n, err := pkt.Encode(s.encodeBuf)
	if err != nil {
		log.Printf("vps: control encode: %v", err)
		return
	}
	for _, addr := range addrs {
		if _, err := s.listen.WriteToUDP(s.encodeBuf[:n], addr); err != nil {
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
	if s.egress == nil || addr == nil {
		return
	}
	if _, err := s.egress.WriteToUDP(pkt.Payload, addr); err != nil {
		log.Printf("vps: egress error: %v", err)
	}
}

// Close shuts down the server.
func (s *Server) Close() error {
	if s.egress != nil {
		s.egress.Close()
	}
	return s.listen.Close()
}

// ListenAddr returns the bound address.
func (s *Server) ListenAddr() net.Addr { return s.listen.LocalAddr() }
