package main

import (
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

	log.Printf("boundlink-vps: listening on %s", srv.ListenAddr())

	go func() {
		if err := srv.Run(); err != nil {
			log.Fatalf("run: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("boundlink-vps: shutting down")
}
