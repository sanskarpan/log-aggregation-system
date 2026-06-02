package main

import (
	"log"

	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/runtime"
	"github.com/sanskar/log-aggregation-system/internal/services/compactor"
)

func main() {
	cfg := config.FromEnv("compactor")
	server, err := compactor.NewServer(cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.RunHTTP(cfg, server.Handler()); err != nil {
		log.Fatal(err)
	}
}
