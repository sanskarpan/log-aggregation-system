package main

import (
	"log"

	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/runtime"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
	"github.com/sanskar/log-aggregation-system/internal/services/parser"
)

func main() {
	cfg := config.FromEnv("parser")
	if err := runtime.RunHTTP(cfg, servicehttp.NewHandler(parser.Descriptor())); err != nil {
		log.Fatal(err)
	}
}
