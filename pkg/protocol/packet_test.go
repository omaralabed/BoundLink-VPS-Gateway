package protocol

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	orig := &Packet{
		Version:      Version,
		Flags:        FlagPrimary,
		Class:        ClassLiveMedia,
		SessionID:    42,
		Sequence:     1001,
		Timestamp:    time.UnixMicro(1700000000123456),
		SourceLinkID: 1,
		Proto:        ProtoSRT,
		DestIP:       net.IPv4(203, 0, 113, 10),
		DestPort:     4200,
		Payload:      []byte("hello-stream"),
	}
	buf := make([]byte, HeaderLen+MaxPayload)
	n, err := orig.Encode(buf)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := Decode(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	if dec.SessionID != orig.SessionID || dec.Sequence != orig.Sequence {
		t.Fatalf("mismatch: %+v vs %+v", dec, orig)
	}
	if !bytes.Equal(dec.Payload, orig.Payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestDecodeRejectsBadMagic(t *testing.T) {
	data := make([]byte, HeaderLen)
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReorderBudget(t *testing.T) {
	if ClassLiveMedia.ReorderBudget() != 800*time.Millisecond {
		t.Fatal("live media budget wrong")
	}
}
