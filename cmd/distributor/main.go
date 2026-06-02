package main

import (
	"log"

	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/runtime"
	"github.com/sanskar/log-aggregation-system/internal/services/distributor"
)

func main() {
	cfg := config.FromEnv("distributor")
	server := distributor.NewServer(cfg.DataDir)
	if err := runtime.RunHTTP(cfg, server.Handler()); err != nil {
		log.Fatal(err)
	}
}
