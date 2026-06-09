# Bound Link Architecture

## Purpose

Bound Link bonds Starlink (low latency) and Ku-band (high latency) into one logical pipe
with a latency-aware scheduler. It supports sub-second live streaming over SRT, RTP, and
WebRTC while still using both links.

## Design Principles

1. **Never send primary live media on a high-latency link when a low-latency link is healthy.**
2. **Ku-band contributes via FEC, backup, and bulk — not as equal partner for Class A traffic.**
3. **Scheduler decisions are driven by measured RTT, loss, and throughput — not static ratios.**
4. **VPS reorder buffer for live traffic is capped at 200 ms to preserve sub-second latency.**

## Components

```
Boat/Remote                          VPS                         Destination
┌──────────────────┐                ┌─────────────────┐
│ boundlink-router │── Starlink ────►│ boundlink-vps   │────► SRT/RTP/WebRTC
│  • classifier    │── Ku-band ─────►│  • reassembler  │      consumers
│  • scheduler     │                │  • FEC merge    │
│  • link monitor  │                │  • egress       │
│  • tunnel client │                │                 │
└──────────────────┘                └─────────────────┘
```

## Traffic Classes

| Class | Name            | Latency budget | Starlink        | Ku-band              |
|-------|-----------------|----------------|-----------------|----------------------|
| A     | LiveMedia       | < 1 s          | Primary (~100%) | FEC / hot standby    |
| B     | ResilientStream | 1–3 s          | ~70% weighted   | ~30% weighted        |
| C     | Bulk            | Best effort    | Capacity-weight | Capacity-weight      |

## Scheduler Logic

### Link health

A link is **healthy** when:
- Gateway responds to probe within timeout
- Loss ≤ configured threshold (default 25%)
- Status is not explicitly `Down`

A link is **degraded** when:
- RTT > `max_latency_ms` OR loss > 10% but ≤ 25%

### Weight formula (Class B and C)

```
weight(link) = throughput_bps × (1 - loss_ratio) / (rtt_ms + base_rtt_ms)
```

Weights are normalized. Packet N goes to link `i` when:
```
cumulative_weight(i-1) < (N mod 1000) / 1000 ≤ cumulative_weight(i)
```

### Class A (LiveMedia)

1. Select primary = lowest RTT among healthy links.
2. If primary RTT > 200 ms, still use it (failover scenario) but log warning.
3. FEC duplicate: send on best alternate healthy link (typically Ku-band).
4. If primary fails mid-stream, promote next-lowest RTT link immediately.

### Reorder buffer (VPS)

| Class | Max buffer |
|-------|------------|
| A     | 200 ms     |
| B     | 2000 ms    |
| C     | 5000 ms    |

Late packets beyond buffer limit are dropped (not retransmitted at tunnel layer;
SRT/WebRTC handle their own recovery).

## Protocol

See `pkg/protocol/packet.go` for wire format.

## Protocol Detection

| Protocol | Detection                          | Default class |
|----------|------------------------------------|---------------|
| SRT      | UDP dest port 4200 (configurable)  | A             |
| RTP      | UDP even port, RTP version 2       | A             |
| WebRTC   | UDP STUN/TURN/media ports          | A             |
| Other    | Config rules or default            | C             |
