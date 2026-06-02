package main

import (
	"log"

	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/runtime"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
	"github.com/sanskar/log-aggregation-system/internal/services/indexer"
)

func main() {
	cfg := config.FromEnv("indexer")
	if err := runtime.RunHTTP(cfg, servicehttp.NewHandler(indexer.Descriptor())); err != nil {
		log.Fatal(err)
	}
}
