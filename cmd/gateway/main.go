package main

import (
	"context"
	"log"

	"github.com/sanskar/log-aggregation-system/internal/engine/queue"
	"github.com/sanskar/log-aggregation-system/internal/engine/singlenode"
	"github.com/sanskar/log-aggregation-system/internal/engine/wal"
	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/runtime"
	"github.com/sanskar/log-aggregation-system/internal/services/gateway"
)

func main() {
	cfg := config.FromEnv("gateway")
	engine, err := singlenode.NewWithOptions(cfg.DataDir, singlenode.Options{
		ChunkMaxEvents:     cfg.ChunkMaxEvents,
		ChunkMaxBytes:      cfg.ChunkMaxBytes,
		ChunkMaxDuration:   cfg.ChunkMaxDuration,
		QueryCacheEntries:  cfg.QueryCacheEntries,
		QueryCacheTTL:      cfg.QueryCacheTTL,
		ChunkCacheEntries:  cfg.ChunkCacheEntries,
		TenantDSN:          cfg.TenantDSN,
		ManifestDSN:        cfg.ManifestDSN,
		JSONIgnoreError:    cfg.JSONIgnoreError,
		EnableLogfmt:       cfg.EnableLogfmt,
		WALMaxSegmentBytes: cfg.WALMaxSegmentBytes,
		WALSyncMode:        wal.SyncMode(cfg.WALSyncMode),
		SegmentBucket:      cfg.SegmentBucket,
		ObjectStoreRetries: cfg.ObjectStoreRetries,
		ObjectStoreBackoff: cfg.ObjectStoreBackoff,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	if _, err := engine.Replay(context.Background()); err != nil {
		log.Fatal(err)
	}

	var broker queue.Broker
	switch cfg.QueueBackend {
	case "kafka":
		broker = queue.NewKafkaBroker(queue.KafkaConfig{
			Brokers:    cfg.KafkaBrokers,
			GroupID:    "gateway-local-writers",
			Partitions: cfg.QueuePartitions,
			AutoCreate: true,
		})
	default:
		broker = queue.NewMemoryBroker(cfg.QueuePartitions)
	}
	server := gateway.NewQueuedServer(engine, broker)
	if err := runtime.RunHTTP(cfg, server.Handler()); err != nil {
		log.Fatal(err)
	}
}
