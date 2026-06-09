// Package reassembler orders incoming tunnel packets per session at the VPS.
package reassembler

import (
	"net"
	"sync"
	"time"

	"github.com/boundlink/vps/pkg/protocol"
)

type pendingPacket struct {
	pkt      *protocol.Packet
	received time.Time
}

// Result is returned after processing one ingress packet.
type Result struct {
	Ready   []*protocol.Packet
	AckSeq  uint32   // highest contiguous seq delivered; 0 = none
	NackSeq []uint32 // missing seqs to request from Pi
}

// SessionBuffer holds out-of-order packets for one session + class.
type SessionBuffer struct {
	cfg         Config
	class       protocol.TrafficClass
	budget      time.Duration
	minBudget   time.Duration
	maxBudget   time.Duration
	nextSeq     uint32
	buffer      map[uint32]pendingPacket
	fecCopies   map[uint32]*protocol.Packet
	linkTransit map[uint8]time.Duration
	gapSince    time.Time
	nackSentAt  time.Time
}

// NewSessionBuffer creates a reorder buffer for a session.
func NewSessionBuffer(class protocol.TrafficClass, cfg Config) *SessionBuffer {
	cfg = NormalizeConfig(cfg)
	minB, maxB := cfg.LiveReorderMin, cfg.LiveReorderMax
	if class != protocol.ClassLiveMedia {
		minB = class.ReorderBudget() / 4
		maxB = class.ReorderBudget()
	}
	return &SessionBuffer{
		cfg:         cfg,
		class:       class,
		minBudget:   minB,
		maxBudget:   maxB,
		budget:      minB,
		buffer:      make(map[uint32]pendingPacket),
		fecCopies:   make(map[uint32]*protocol.Packet),
		linkTransit: make(map[uint8]time.Duration),
	}
}

// Insert adds a packet and returns egress-ready packets plus control signals.
func (sb *SessionBuffer) Insert(pkt *protocol.Packet) Result {
	if !pkt.IsData() {
		return Result{}
	}

	now := time.Now()
	sb.observeTransit(pkt, now)

	if pkt.IsFEC() {
		sb.fecCopies[pkt.Sequence] = pkt
		ready := sb.drainReady(now)
		return sb.finishResult(ready)
	}

	if pkt.Sequence < sb.nextSeq {
		return Result{}
	}

	if pkt.Sequence == sb.nextSeq {
		var ready []*protocol.Packet
		ready = append(ready, pkt)
		sb.nextSeq++
		sb.clearGapState()
		ready = append(ready, sb.drainSequential()...)
		return sb.finishResult(ready)
	}

	sb.buffer[pkt.Sequence] = pendingPacket{pkt: pkt, received: now}
	if sb.gapSince.IsZero() {
		sb.gapSince = now
	}
	ready := sb.drainReady(now)
	return sb.finishResult(ready)
}

func (sb *SessionBuffer) finishResult(ready []*protocol.Packet) Result {
	res := Result{Ready: ready}
	if sb.cfg.AckResend && len(ready) > 0 && sb.nextSeq > 0 {
		res.AckSeq = sb.nextSeq - 1
	}
	if nack := sb.checkNack(time.Now()); len(nack) > 0 {
		res.NackSeq = nack
	}
	return res
}

func (sb *SessionBuffer) observeTransit(pkt *protocol.Packet, now time.Time) {
	if sb.class != protocol.ClassLiveMedia {
		return
	}
	transit := now.Sub(pkt.Timestamp)
	if transit < 0 {
		transit = 0
	}
	id := pkt.SourceLinkID
	if prev, ok := sb.linkTransit[id]; ok {
		sb.linkTransit[id] = time.Duration(float64(prev)*0.8 + float64(transit)*0.2)
	} else {
		sb.linkTransit[id] = transit
	}
	sb.adaptBudget()
}

func (sb *SessionBuffer) adaptBudget() {
	if len(sb.linkTransit) < 2 {
		sb.budget = sb.minBudget
		return
	}
	var minT, maxT time.Duration
	first := true
	for _, t := range sb.linkTransit {
		if first || t < minT {
			minT = t
		}
		if first || t > maxT {
			maxT = t
		}
		first = false
	}
	spread := maxT - minT
	b := sb.minBudget + spread + 50*time.Millisecond
	if b > sb.maxBudget {
		b = sb.maxBudget
	}
	if b < sb.minBudget {
		b = sb.minBudget
	}
	sb.budget = b
}

func (sb *SessionBuffer) drainSequential() []*protocol.Packet {
	var ready []*protocol.Packet
	for {
		pending, ok := sb.buffer[sb.nextSeq]
		if !ok {
			break
		}
		delete(sb.buffer, sb.nextSeq)
		ready = append(ready, pending.pkt)
		sb.nextSeq++
		sb.clearGapState()
	}
	return ready
}

func (sb *SessionBuffer) drainReady(now time.Time) []*protocol.Packet {
	var ready []*protocol.Packet

	if pending, ok := sb.buffer[sb.nextSeq]; ok && !pending.pkt.IsFEC() {
		delete(sb.buffer, sb.nextSeq)
		ready = append(ready, pending.pkt)
		sb.nextSeq++
		sb.clearGapState()
		ready = append(ready, sb.drainSequential()...)
		return ready
	}

	sb.checkGapTimeout(now)

	for seq, pending := range sb.buffer {
		if seq == sb.nextSeq {
			continue
		}
		if now.Sub(pending.received) <= sb.budget {
			continue
		}
		delete(sb.buffer, seq)
		if fec, ok := sb.fecCopies[seq]; ok {
			ready = append(ready, fec)
			delete(sb.fecCopies, seq)
			if seq == sb.nextSeq {
				sb.nextSeq++
				sb.clearGapState()
				ready = append(ready, sb.drainSequential()...)
			}
		}
	}

	ready = append(ready, sb.drainSequential()...)
	return ready
}

func (sb *SessionBuffer) checkGapTimeout(now time.Time) {
	if sb.gapSince.IsZero() {
		return
	}
	elapsed := now.Sub(sb.gapSince)
	if sb.cfg.AckResend {
		if sb.nackSentAt.IsZero() && elapsed >= sb.budget {
			return // NACK emitted via checkNack
		}
		if !sb.nackSentAt.IsZero() && now.Sub(sb.nackSentAt) >= sb.cfg.NackGrace {
			delete(sb.buffer, sb.nextSeq)
			delete(sb.fecCopies, sb.nextSeq)
			sb.nextSeq++
			sb.clearGapState()
		}
		return
	}
	if elapsed >= sb.budget {
		sb.nextSeq++
		sb.clearGapState()
	}
}

func (sb *SessionBuffer) checkNack(now time.Time) []uint32 {
	if !sb.cfg.AckResend || sb.gapSince.IsZero() {
		return nil
	}
	if _, ok := sb.buffer[sb.nextSeq]; ok {
		return nil
	}
	elapsed := now.Sub(sb.gapSince)
	if sb.nackSentAt.IsZero() && elapsed >= sb.budget {
		sb.nackSentAt = now
		return []uint32{sb.nextSeq}
	}
	return nil
}

func (sb *SessionBuffer) clearGapState() {
	sb.gapSince = time.Time{}
	sb.nackSentAt = time.Time{}
}

// Reassembler manages multiple session buffers.
type Reassembler struct {
	cfg          Config
	mu           sync.Mutex
	sessions     map[uint32]*SessionBuffer
	sessionAddrs map[uint32]map[uint8]*net.UDPAddr
}

// New creates a reassembler.
func New(cfg Config) *Reassembler {
	return &Reassembler{
		cfg:          NormalizeConfig(cfg),
		sessions:     make(map[uint32]*SessionBuffer),
		sessionAddrs: make(map[uint32]map[uint8]*net.UDPAddr),
	}
}

// Process ingests a data packet and returns egress-ready packets plus control signals.
func (r *Reassembler) Process(pkt *protocol.Packet, remote *net.UDPAddr) Result {
	if pkt == nil || !pkt.IsData() {
		return Result{}
	}
	r.mu.Lock()
	sb, ok := r.sessions[pkt.SessionID]
	if !ok {
		sb = NewSessionBuffer(pkt.Class, r.cfg)
		r.sessions[pkt.SessionID] = sb
	}
	if remote != nil {
		addrs, ok := r.sessionAddrs[pkt.SessionID]
		if !ok {
			addrs = make(map[uint8]*net.UDPAddr)
			r.sessionAddrs[pkt.SessionID] = addrs
		}
		cp := *remote
		addrs[pkt.SourceLinkID] = &cp
	}
	r.mu.Unlock()
	return sb.Insert(pkt)
}

// SessionAddrs returns known Pi source addresses for a session (all links).
func (r *Reassembler) SessionAddrs(sessionID uint32) []*net.UDPAddr {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.sessionAddrs[sessionID]
	if len(m) == 0 {
		return nil
	}
	out := make([]*net.UDPAddr, 0, len(m))
	for _, a := range m {
		out = append(out, a)
	}
	return out
}

// Budget returns the current adaptive reorder budget (for tests).
func (sb *SessionBuffer) Budget() time.Duration {
	return sb.budget
}
