package main

import (
	"log"

	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/runtime"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
	"github.com/sanskar/log-aggregation-system/internal/services/ingester"
)

func main() {
	cfg := config.FromEnv("ingester")
	if err := runtime.RunHTTP(cfg, servicehttp.NewHandler(ingester.Descriptor())); err != nil {
		log.Fatal(err)
	}
}
