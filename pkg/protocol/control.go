package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// MaxNackSeqs is the maximum missing sequences per NACK control packet.
const MaxNackSeqs = 32

// EncodeNackPayload serializes missing sequence numbers for a NACK packet.
func EncodeNackPayload(seqs []uint32) ([]byte, error) {
	if len(seqs) == 0 {
		return nil, errors.New("empty nack")
	}
	if len(seqs) > MaxNackSeqs {
		return nil, fmt.Errorf("nack exceeds max %d", MaxNackSeqs)
	}
	buf := make([]byte, 2+4*len(seqs))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(seqs)))
	for i, seq := range seqs {
		binary.BigEndian.PutUint32(buf[2+i*4:2+(i+1)*4], seq)
	}
	return buf, nil
}

// DecodeNackPayload parses missing sequence numbers from a NACK payload.
func DecodeNackPayload(data []byte) ([]uint32, error) {
	if len(data) < 2 {
		return nil, errors.New("nack payload too short")
	}
	n := int(binary.BigEndian.Uint16(data[0:2]))
	if n == 0 || n > MaxNackSeqs {
		return nil, fmt.Errorf("invalid nack count %d", n)
	}
	need := 2 + 4*n
	if len(data) < need {
		return nil, errors.New("truncated nack payload")
	}
	out := make([]uint32, n)
	for i := 0; i < n; i++ {
		out[i] = binary.BigEndian.Uint32(data[2+i*4 : 2+(i+1)*4])
	}
	return out, nil
}

// NewACKPacket builds a VPS→Pi cumulative ACK through ackSeq (inclusive).
func NewACKPacket(sessionID, ackSeq uint32) *Packet {
	return &Packet{
		Version:   Version,
		Flags:     FlagACK,
		SessionID: sessionID,
		Sequence:  ackSeq,
	}
}

// NewNACKPacket builds a VPS→Pi resend request for missing sequences.
func NewNACKPacket(sessionID uint32, seqs []uint32) (*Packet, error) {
	payload, err := EncodeNackPayload(seqs)
	if err != nil {
		return nil, err
	}
	return &Packet{
		Version:   Version,
		Flags:     FlagNACK,
		SessionID: sessionID,
		Payload:   payload,
	}, nil
}
