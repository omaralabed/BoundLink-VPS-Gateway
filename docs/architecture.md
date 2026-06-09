# Bound Link Architecture

## Purpose

Bound Link bonds multiple WAN links (e.g. Starlink + Ku-band) into one logical upload path
with a latency-aware scheduler. It supports sub-second live streaming over SRT, RTP, and
WebRTC while using both links for throughput.

**Repos:** [boundlink-go](https://github.com/omaralabed/boundlink-go) (Pi Router) ·
[BoundLink-VPS-Gateway](https://github.com/omaralabed/BoundLink-VPS-Gateway) (VPS)

## Components

```
Boat/Remote                          VPS                         Destination
┌──────────────────┐                ┌─────────────────┐
│ boundlink-pi     │── Starlink ────►│ boundlink-vps   │────► SRT/RTP/WebRTC
│  • classifier    │── Ku-band ─────►│  • reassembler  │      consumers
│  • scheduler     │◄── ACK/NACK ────│  • ACK/NACK     │
│  • link monitor  │                │  • egress       │
│  • tunnel client │                │                 │
└──────────────────┘                └─────────────────┘
```

## Traffic Classes

| Class | Name            | Latency target | Scheduling |
|-------|-----------------|----------------|------------|
| A     | LiveMedia       | < 1 s          | See live modes below |
| B     | ResilientStream | 1–3 s          | Quality-weighted (smart) or equal (bond_all) |
| C     | Bulk            | Best effort    | Quality-weighted (smart) or equal (bond_all) |

## Live modes (Class A)

Configured via `scheduler.live_mode` on the Pi.

### `liveu_like` (default — LiveU-style)

- Equal packet rotation across healthy WANs (50/50 for two links).
- Each link carries **unique** packets — no FEC duplicates.
- Combined throughput appears at the receiver (like LiveU Starlink + LAN2).
- `enable_fec` should be `false`.
- Requires VPS adaptive reorder + ACK/NACK resend to handle out-of-order arrival.

### `primary_fec` (classic Smart live)

- Primary on best-quality low-latency link (Starlink at sea).
- Optional FEC copy on most stable alternate link (typically Ku-band).
- Ku-band contributes recovery copies, not equal primary traffic.
- `enable_fec` should be `true`.

### Brain (`aggressive_bond: auto`)

When two **stable, similar** WANs appear (dock/office — not Starlink + Ku-band at sea),
the brain auto-switches live upload to equal bonding (`live-brain-bond`).

Qualifies when:
- All links `StatusUp`, loss below unstable threshold
- RTT spread ≤ `aggressive_max_rtt_spread_ms` (default 150 ms)
- Quality scores within `stable_quality_close_ratio` (default 0.65)

Only applies in `primary_fec` mode — `liveu_like` always bonds equally.

## Link health

A link is **healthy** when status is `Up` or `Degraded` (still schedulable).

| Status    | Condition |
|-----------|-----------|
| Up        | RTT ≤ max, loss ≤ max_loss/2, jitter ≤ 40 ms |
| Degraded  | RTT > max_latency OR loss > max_loss/2 OR high jitter — still carries traffic |
| Down      | Loss ≥ 100% OR loss > max_loss_percent (default 25%) |

Probes use DNS to `probe_target` (default `1.1.1.1:53`) bound per WAN interface.

## Weight formula (Class B and C, smart mode)

```
weight(link) = throughput_bps × (1 - loss_ratio) / (rtt_ms + base_rtt_ms)
```

## VPS reassembly

### Adaptive live reorder

For Class A, the VPS reorder budget scales with measured per-link transit spread:

| Setting | Default | Meaning |
|---------|---------|---------|
| `live_reorder_min_ms` | 200 ms | Minimum wait before NACK / gap handling |
| `live_reorder_max_ms` | 1200 ms | Cap for disparate links (Starlink + Ku-band) |
| `nack_grace_ms` | 200 ms | Extra wait after NACK before skipping a gap |

Budget = `min + (max_transit - min_transit) + 50ms`, clamped to min/max.

### Non-live classes

Uses static class budgets from `TrafficClass.ReorderBudget()` (B: 2 s, C: 5 s).

### Gap recovery

1. Out-of-order packets buffered until `nextSeq` arrives.
2. After budget elapsed: VPS sends **NACK** to Pi (retries up to 3×).
3. Pi resends from packet cache (`tunnel.ack_resend: true`).
4. If primary lost: **FEC copy** delivered at head gap when available.
5. After NACK grace with no recovery: gap skipped (lossy).

Background **sweep** (50 ms) runs NACK/gap logic even when ingress pauses.

### Egress

- Always binds an ephemeral UDP egress socket.
- Per-packet `DestIP`/`DestPort` from Pi (gateway capture) takes priority.
- `egress_addr` in config is fallback when Pi sends no destination.

## BDLK protocol

36-byte header + payload. See `pkg/protocol/packet.go`.

| Field | Purpose |
|-------|---------|
| SessionID / Sequence | Per-stream ordering |
| SourceLinkID | Which WAN link sent this copy |
| DestIP / DestPort | Original destination (Pi → VPS → studio) |
| Flags | Primary, FEC, ACK, NACK, Resend |

Control messages (VPS → Pi): cumulative **ACK** (trim resend cache), **NACK** (request resend).

## Protocol detection (Pi classifier)

| Protocol | Detection | Class |
|----------|-----------|-------|
| SRT      | UDP port (4200, 9000, ingress port) or payload | A |
| RTP      | Port range 5000–5200 + RTP version bits | A |
| WebRTC   | STUN/TURN/media ports | A |
| RDP/VNC/Gaming | Configured ports | B |
| VPN      | Configured ports | C |
| Other    | `default_class` (bulk) | C |
