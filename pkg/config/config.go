// Package config loads VPS YAML configuration.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/boundlink/vps/pkg/reassembler"
)

// ReassemblyConfig YAML wrapper for reassembler settings.
type ReassemblyConfig struct {
	LiveReorderMinMs int  `yaml:"live_reorder_min_ms"`
	LiveReorderMaxMs int  `yaml:"live_reorder_max_ms"`
	NackGraceMs      int  `yaml:"nack_grace_ms"`
	AckResend        bool `yaml:"ack_resend"`
}

// Config is the cloud VPS endpoint configuration.
type Config struct {
	ListenPort  uint16           `yaml:"listen_port"`
	EgressAddr  string           `yaml:"egress_addr"` // studio/destination host:port
	Reassembly  ReassemblyConfig `yaml:"reassembly"`
}

// Default returns working defaults.
func Default() Config {
	return Config{
		ListenPort: 6500,
		Reassembly: ReassemblyConfig{
			LiveReorderMinMs: 200,
			LiveReorderMaxMs: 1200,
			NackGraceMs:      200,
			AckResend:        true,
		},
	}
}

// ReassemblerConfig converts YAML to reassembler.Config.
func (c Config) ReassemblerConfig() reassembler.Config {
	return reassembler.Config{
		LiveReorderMin: time.Duration(c.Reassembly.LiveReorderMinMs) * time.Millisecond,
		LiveReorderMax: time.Duration(c.Reassembly.LiveReorderMaxMs) * time.Millisecond,
		NackGrace:      time.Duration(c.Reassembly.NackGraceMs) * time.Millisecond,
		AckResend:      c.Reassembly.AckResend,
	}
}

// Load reads config from a YAML file.
func Load(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if cfg.ListenPort == 0 {
		return cfg, fmt.Errorf("listen_port is required")
	}
	return cfg, nil
}
