package main

import (
	"context"
	"log"

	"github.com/sanskar/log-aggregation-system/internal/engine/singlenode"
	"github.com/sanskar/log-aggregation-system/internal/engine/wal"
	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/runtime"
	queryfrontend "github.com/sanskar/log-aggregation-system/internal/services/queryfrontend"
)

func main() {
	cfg := config.FromEnv("query-frontend")
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
	workers := make([]queryfrontend.QueryExecutor, 0, len(cfg.QuerierURLs))
	for _, workerURL := range cfg.QuerierURLs {
		worker, err := queryfrontend.NewHTTPQuerierClientFromConfig(workerURL, cfg)
		if err != nil {
			log.Fatal(err)
		}
		workers = append(workers, worker)
	}
	server := queryfrontend.NewServer(engine, workers...)
	if err := runtime.RunHTTP(cfg, server.Handler()); err != nil {
		log.Fatal(err)
	}
}
