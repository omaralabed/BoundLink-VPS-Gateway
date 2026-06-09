// Package protocol defines the Bound Link UDP tunnel wire format.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	Magic     uint32 = 0x42444C4B // "BDLK"
	Version   uint8  = 1
	HeaderLen        = 36
	MaxPayload       = 1400
)

// Flags
const (
	FlagFEC      uint8 = 1 << 0 // parity / duplicate for recovery
	FlagPrimary  uint8 = 1 << 1 // primary path packet (vs FEC copy)
	FlagFragment uint8 = 1 << 2 // reserved for future fragmentation
	FlagACK      uint8 = 1 << 3 // VPS cumulative ack to Pi
	FlagNACK     uint8 = 1 << 4 // VPS requests resend of missing seq(s)
	FlagResend   uint8 = 1 << 5 // Pi resent packet after NACK
)

// TrafficClass determines scheduling and reorder behavior.
type TrafficClass uint16

const (
	ClassLiveMedia       TrafficClass = 1 // sub-second: Starlink primary
	ClassResilientStream TrafficClass = 2 // 1-3s buffer OK
	ClassBulk            TrafficClass = 3 // best effort
)

// ProtocolHint identifies detected application protocol.
type ProtocolHint uint8

const (
	ProtoUnknown ProtocolHint = 0
	ProtoSRT     ProtocolHint = 1
	ProtoRTP     ProtocolHint = 2
	ProtoWebRTC  ProtocolHint = 3
)

// Packet is the tunnel envelope sent between router and VPS.
type Packet struct {
	Version      uint8
	Flags        uint8
	Class        TrafficClass
	SessionID    uint32
	Sequence     uint32
	Timestamp    time.Time
	SourceLinkID uint8
	Proto        ProtocolHint
	DestIP       net.IP // original destination (gateway capture); nil = VPS default egress
	DestPort     uint16
	Payload      []byte
}

// Encode serializes a packet into buf. buf must be at least HeaderLen+len(payload).
func (p *Packet) Encode(buf []byte) (int, error) {
	if len(p.Payload) > MaxPayload {
		return 0, fmt.Errorf("payload exceeds max %d bytes", MaxPayload)
	}
	need := HeaderLen + len(p.Payload)
	if len(buf) < need {
		return 0, fmt.Errorf("buffer too small: need %d, have %d", need, len(buf))
	}

	binary.BigEndian.PutUint32(buf[0:4], Magic)
	buf[4] = p.Version
	buf[5] = p.Flags
	binary.BigEndian.PutUint16(buf[6:8], uint16(p.Class))
	binary.BigEndian.PutUint32(buf[8:12], p.SessionID)
	binary.BigEndian.PutUint32(buf[12:16], p.Sequence)
	us := p.Timestamp.UnixMicro()
	binary.BigEndian.PutUint64(buf[16:24], uint64(us))
	buf[24] = p.SourceLinkID
	buf[25] = uint8(p.Proto)
	ip4 := p.DestIP.To4()
	if ip4 == nil {
		ip4 = net.IPv4zero
	}
	copy(buf[26:30], ip4)
	binary.BigEndian.PutUint16(buf[30:32], p.DestPort)
	binary.BigEndian.PutUint32(buf[32:36], uint32(len(p.Payload)))
	copy(buf[HeaderLen:], p.Payload)
	return need, nil
}

// Decode parses a wire packet. Payload is copied into a new slice.
func Decode(data []byte) (*Packet, error) {
	if len(data) < HeaderLen {
		return nil, errors.New("packet too short")
	}
	if binary.BigEndian.Uint32(data[0:4]) != Magic {
		return nil, errors.New("invalid magic")
	}
	ver := data[4]
	if ver != Version {
		return nil, fmt.Errorf("unsupported version %d", ver)
	}
	payloadLen := binary.BigEndian.Uint32(data[32:36])
	if payloadLen > MaxPayload {
		return nil, fmt.Errorf("invalid payload length %d", payloadLen)
	}
	if len(data) < HeaderLen+int(payloadLen) {
		return nil, errors.New("truncated payload")
	}

	us := int64(binary.BigEndian.Uint64(data[16:24]))
	destIP := net.IP(append([]byte(nil), data[26:30]...))
	destPort := binary.BigEndian.Uint16(data[30:32])
	payload := make([]byte, payloadLen)
	copy(payload, data[HeaderLen:HeaderLen+payloadLen])

	return &Packet{
		Version:      ver,
		Flags:        data[5],
		Class:        TrafficClass(binary.BigEndian.Uint16(data[6:8])),
		SessionID:    binary.BigEndian.Uint32(data[8:12]),
		Sequence:     binary.BigEndian.Uint32(data[12:16]),
		Timestamp:    time.UnixMicro(us),
		SourceLinkID: data[24],
		Proto:        ProtocolHint(data[25]),
		DestIP:       destIP,
		DestPort:     destPort,
		Payload:      payload,
	}, nil
}

// IsFEC returns true if this packet is a FEC/duplicate copy.
func (p *Packet) IsFEC() bool { return p.Flags&FlagFEC != 0 }

// IsPrimary returns true if this is the primary-path packet.
func (p *Packet) IsPrimary() bool { return p.Flags&FlagPrimary != 0 }

// IsACK returns true for VPS→Pi cumulative acknowledgements.
func (p *Packet) IsACK() bool { return p.Flags&FlagACK != 0 }

// IsNACK returns true for VPS→Pi resend requests.
func (p *Packet) IsNACK() bool { return p.Flags&FlagNACK != 0 }

// IsResend returns true when Pi resent a cached packet.
func (p *Packet) IsResend() bool { return p.Flags&FlagResend != 0 }

// IsData returns true for packets carrying stream payload.
func (p *Packet) IsData() bool {
	return !p.IsACK() && !p.IsNACK()
}

// HasDest returns true when the Pi supplied an original egress destination.
func (p *Packet) HasDest() bool {
	return p.DestPort != 0 && len(p.DestIP) == 4 && !p.DestIP.Equal(net.IPv4zero)
}

// ReorderBudget returns max reorder wait for this traffic class.
func (c TrafficClass) ReorderBudget() time.Duration {
	switch c {
	case ClassLiveMedia:
		return 800 * time.Millisecond
	case ClassResilientStream:
		return 2 * time.Second
	default:
		return 5 * time.Second
	}
}
