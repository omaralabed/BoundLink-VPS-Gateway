package reassembler

import "time"

// Config tunes adaptive reorder and ack/resend behavior.
type Config struct {
	// LiveReorderMin is the minimum wait before NACK/skip on live gaps.
	LiveReorderMin time.Duration
	// LiveReorderMax caps adaptive reorder budget for disparate links.
	LiveReorderMax time.Duration
	// NackGrace is extra wait after NACK before skipping a live gap.
	NackGrace time.Duration
	// AckResend enables VPS→Pi ACK/NACK control messages.
	AckResend bool
}

// DefaultConfig returns LiveU-style bonding defaults.
func DefaultConfig() Config {
	return Config{
		LiveReorderMin: 200 * time.Millisecond,
		LiveReorderMax: 1200 * time.Millisecond,
		NackGrace:      200 * time.Millisecond,
		AckResend:      true,
	}
}

// Normalize fills zero durations with defaults.
func NormalizeConfig(cfg Config) Config {
	d := DefaultConfig()
	if cfg.LiveReorderMin <= 0 {
		cfg.LiveReorderMin = d.LiveReorderMin
	}
	if cfg.LiveReorderMax <= 0 {
		cfg.LiveReorderMax = d.LiveReorderMax
	}
	if cfg.NackGrace <= 0 {
		cfg.NackGrace = d.NackGrace
	}
	if cfg.LiveReorderMax < cfg.LiveReorderMin {
		cfg.LiveReorderMax = cfg.LiveReorderMin
	}
	return cfg
}
