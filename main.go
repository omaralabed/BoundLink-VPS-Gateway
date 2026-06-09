package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/boundlink/vps/pkg/config"
	"github.com/boundlink/vps/pkg/tunnel"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	srv, err := tunnel.NewServer(tunnel.ServerConfig{
		ListenPort: int(cfg.ListenPort),
		EgressAddr: cfg.EgressAddr,
		Reassembly: cfg.ReassemblerConfig(),
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv.StartBackground(ctx)

	log.Printf("boundlink-vps: listening on %s", srv.ListenAddr())

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
		log.Println("boundlink-vps: shutting down")
		cancel()
	case err := <-errCh:
		if err != nil {
			log.Printf("boundlink-vps: run ended: %v", err)
		}
	}
}
