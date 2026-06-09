package reassembler

import (
	"net"
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

func TestFECRecoveryAtHeadGap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AckResend = true
	sb := NewSessionBuffer(protocol.ClassLiveMedia, cfg)

	sb.Insert(makePkt(0, 1, false))
	res := sb.Insert(makePkt(1, 2, true)) // FEC only for missing primary at seq 1
	if len(res.Ready) != 1 || res.Ready[0].Payload[0] != 1 {
		t.Fatalf("expected FEC delivery for seq 1, got %v", res.Ready)
	}
	if !res.Ready[0].IsFEC() {
		t.Fatal("expected FEC packet delivered")
	}
}

func TestFECRecoveryAfterTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LiveReorderMin = 10 * time.Millisecond
	cfg.AckResend = true
	sb := NewSessionBuffer(protocol.ClassLiveMedia, cfg)

	sb.Insert(makePkt(0, 1, false))
	sb.Insert(makePkt(2, 1, false))
	res := sb.Insert(makePkt(1, 2, true))
	if len(res.Ready) == 0 || res.Ready[0].Payload[0] != 1 {
		t.Fatalf("expected FEC seq 1 delivered immediately, got %v", res.Ready)
	}
}

func TestSweepNackWithoutIngress(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LiveReorderMin = 5 * time.Millisecond
	cfg.LiveReorderMax = 50 * time.Millisecond
	r := New(cfg)

	r.Process(makePkt(0, 1, false), nil)
	r.Process(makePkt(2, 1, false), nil)

	time.Sleep(15 * time.Millisecond)
	sweeps := r.Sweep(time.Now())
	found := false
	for _, s := range sweeps {
		if len(s.Result.NackSeq) > 0 && s.Result.NackSeq[0] == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected sweep to emit NACK for seq 1 without new ingress")
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
	if !res.HasAck || res.AckSeq != 0 {
		t.Fatalf("expected HasAck with ack seq 0, got HasAck=%v AckSeq=%d", res.HasAck, res.AckSeq)
	}
}

func TestSessionAddrsPersist(t *testing.T) {
	r := New(DefaultConfig())
	remote := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 10), Port: 45000}
	r.Process(makePkt(0, 1, false), remote)

	addrs := r.SessionAddrs(1)
	if len(addrs) != 1 {
		t.Fatalf("expected 1 addr, got %d", len(addrs))
	}
	if addrs[0].String() != remote.String() {
		t.Fatalf("addr corrupted: got %s want %s", addrs[0], remote)
	}
}
