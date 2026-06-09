# BoundLink VPS Gateway

Go server for your cloud VPS — pairs with **[BoundLink Pi Router](https://github.com/omaralabed/boundlink-go)**.

Runs on your **cloud VPS** (public IP). Receives bounded traffic from the Pi Router,
reassembles packets in order, and forwards to your studio/destination.

## Setup

1. Open UDP port **6500** in firewall
2. Edit `config.yaml` — set `egress_addr` to your studio (optional)
3. Build and run:

```bash
go mod tidy
make build
./boundlink-vps -config config.yaml
```

## Pair with

**[boundlink-go](https://github.com/omaralabed/boundlink-go)** — Pi Router must set `vps_addr` to this server's public IP.

See `docs/architecture.md` for reorder buffer and FEC logic.
