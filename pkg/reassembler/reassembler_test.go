package reassembler

import (
	"testing"
	"time"

	"github.com/boundlink/vps/pkg/protocol"
)

func makePkt(seq uint32, linkID uint8, fec bool) *protocol.Packet {
	flags := protocol.FlagPrimary
	if fec {
		flags = protocol.FlagFEC
	}
	return &protocol.Packet{
		Class:        protocol.ClassLiveMedia,
		SessionID:    1,
		Sequence:     seq,
		Flags:        flags,
		SourceLinkID: linkID,
		Timestamp:    time.Now(),
		Payload:      []byte{byte(seq)},
	}
}

func TestInOrderDelivery(t *testing.T) {
	r := New(DefaultConfig())
	var out []*protocol.Packet
	for i := uint32(0); i < 5; i++ {
		res := r.Process(makePkt(i, 1, false), nil)
		out = append(out, res.Ready...)
	}
	if len(out) != 5 {
		t.Fatalf("expected 5 packets, got %d", len(out))
	}
}

func TestOutOfOrderReorder(t *testing.T) {
	r := New(DefaultConfig())
	r.Process(makePkt(1, 1, false), nil)
	res := r.Process(makePkt(0, 1, false), nil)
	if len(res.Ready) != 2 {
		t.Fatalf("expected 2 after reorder, got %d", len(res.Ready))
	}
}

func TestAdaptiveBudgetIncreasesWithLinkSpread(t *testing.T) {
	cfg := DefaultConfig()
	sb := NewSessionBuffer(protocol.ClassLiveMedia, cfg)

	fast := makePkt(0, 1, false)
	fast.Timestamp = time.Now().Add(-40 * time.Millisecond)
	sb.Insert(fast)

	slow := makePkt(2, 2, false)
	slow.Timestamp = time.Now().Add(-600 * time.Millisecond)
	sb.Insert(slow)

	if sb.Budget() <= cfg.LiveReorderMin {
		t.Fatalf("expected adaptive budget > min, got %v", sb.Budget())
	}
}

func TestNackOnGap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LiveReorderMin = 5 * time.Millisecond
	cfg.LiveReorderMax = 50 * time.Millisecond
	sb := NewSessionBuffer(protocol.ClassLiveMedia, cfg)

	sb.Insert(makePkt(0, 1, false))
	sb.Insert(makePkt(2, 1, false)) // gap at seq 1, same link keeps budget at min

	time.Sleep(15 * time.Millisecond)
	res := sb.Insert(makePkt(3, 1, false))
	if len(res.NackSeq) == 0 || res.NackSeq[0] != 1 {
		t.Fatalf("expected nack for seq 1, got %v", res.NackSeq)
	}
}

func TestFECRecoveryAfterTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LiveReorderMin = 10 * time.Millisecond
	cfg.AckResend = false
	sb := NewSessionBuffer(protocol.ClassLiveMedia, cfg)

	sb.Insert(makePkt(0, 1, false))
	sb.Insert(makePkt(2, 1, false))
	sb.Insert(makePkt(1, 1, true))

	time.Sleep(15 * time.Millisecond)
	out := sb.Insert(makePkt(3, 1, false))
	if len(out.Ready) == 0 {
		t.Fatal("expected progress after fec timeout")
	}
}

func TestDuplicateDropped(t *testing.T) {
	r := New(DefaultConfig())
	r.Process(makePkt(0, 1, false), nil)
	res := r.Process(makePkt(0, 1, false), nil)
	if len(res.Ready) != 0 {
		t.Fatalf("duplicate should not re-emit, got %d", len(res.Ready))
	}
}

func TestAckAfterDelivery(t *testing.T) {
	r := New(DefaultConfig())
	res := r.Process(makePkt(0, 1, false), nil)
	if res.AckSeq != 0 {
		t.Fatalf("expected ack 0, got %d", res.AckSeq)
	}
}
