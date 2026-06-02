package main

import (
	"log"

	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/runtime"
	controlplane "github.com/sanskar/log-aggregation-system/internal/services/controlplane"
)

func main() {
	cfg := config.FromEnv("control-plane")
	server, err := controlplane.NewServer(cfg.DataDir)
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.RunHTTP(cfg, server.Handler()); err != nil {
		log.Fatal(err)
	}
}
