package config

import (
	"testing"
	"time"
)

func TestFromEnvDefaults(t *testing.T) {
	t.Setenv("SERVICE_HTTP_ADDR", "")
	cfg := FromEnv("gateway")

	if cfg.ServiceName != "gateway" {
		t.Fatalf("unexpected service name: %s", cfg.ServiceName)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("unexpected http addr: %s", cfg.HTTPAddr)
	}
	if cfg.Port != 8080 {
		t.Fatalf("unexpected port: %d", cfg.Port)
	}
	if cfg.DataDir != ".data" {
		t.Fatalf("unexpected data dir: %s", cfg.DataDir)
	}
	if cfg.ChunkMaxEvents != 1000 {
		t.Fatalf("unexpected chunk max events: %d", cfg.ChunkMaxEvents)
	}
	if cfg.ChunkMaxBytes != 256*1024 {
		t.Fatalf("unexpected chunk max bytes: %d", cfg.ChunkMaxBytes)
	}
	if cfg.ChunkMaxDuration != time.Minute {
		t.Fatalf("unexpected chunk max duration: %s", cfg.ChunkMaxDuration)
	}
	if !cfg.JSONIgnoreError {
		t.Fatal("expected json ignore error default to true")
	}
	if !cfg.EnableLogfmt {
		t.Fatal("expected logfmt default to true")
	}
	if cfg.WALMaxSegmentBytes != 8*1024*1024 {
		t.Fatalf("unexpected wal segment bytes: %d", cfg.WALMaxSegmentBytes)
	}
	if cfg.WALSyncMode != "always" {
		t.Fatalf("unexpected wal sync mode: %s", cfg.WALSyncMode)
	}
	if cfg.SegmentBucket != time.Hour {
		t.Fatalf("unexpected segment bucket: %s", cfg.SegmentBucket)
	}
	if cfg.QueuePartitions != 8 {
		t.Fatalf("unexpected queue partitions: %d", cfg.QueuePartitions)
	}
	if cfg.QueueBackend != "memory" {
		t.Fatalf("unexpected queue backend: %s", cfg.QueueBackend)
	}
	if cfg.ObjectStoreRetries != 3 {
		t.Fatalf("unexpected object store retries: %d", cfg.ObjectStoreRetries)
	}
	if cfg.QueryCacheEntries != 256 {
		t.Fatalf("unexpected query cache entries: %d", cfg.QueryCacheEntries)
	}
	if cfg.QueryCacheTTL != 30*time.Second {
		t.Fatalf("unexpected query cache ttl: %s", cfg.QueryCacheTTL)
	}
	if cfg.ChunkCacheEntries != 512 {
		t.Fatalf("unexpected chunk cache entries: %d", cfg.ChunkCacheEntries)
	}
	if cfg.TenantDSN != "" {
		t.Fatalf("unexpected tenant dsn: %s", cfg.TenantDSN)
	}
	if cfg.ManifestDSN != "" {
		t.Fatalf("unexpected manifest dsn: %s", cfg.ManifestDSN)
	}
	if cfg.AuthRequired {
		t.Fatal("expected auth required default to false")
	}
	if cfg.AuthMode != "off" {
		t.Fatalf("unexpected auth mode: %s", cfg.AuthMode)
	}
	if cfg.AuthBearerToken != "" {
		t.Fatalf("unexpected auth bearer token: %s", cfg.AuthBearerToken)
	}
	if cfg.OIDCIssuerURL != "" {
		t.Fatalf("unexpected oidc issuer url: %s", cfg.OIDCIssuerURL)
	}
	if cfg.OIDCJWKSURL != "" {
		t.Fatalf("unexpected oidc jwks url: %s", cfg.OIDCJWKSURL)
	}
	if cfg.OIDCAudience != "" {
		t.Fatalf("unexpected oidc audience: %s", cfg.OIDCAudience)
	}
	if cfg.OIDCScopesClaim != "scope" {
		t.Fatalf("unexpected oidc scopes claim: %s", cfg.OIDCScopesClaim)
	}
	if cfg.AuditLogPath != "" {
		t.Fatalf("unexpected audit log path: %s", cfg.AuditLogPath)
	}
	if cfg.TLSEnabled {
		t.Fatal("expected tls enabled default to false")
	}
	if cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" || cfg.TLSClientCAFile != "" {
		t.Fatalf("unexpected tls server config: %+v", cfg)
	}
	if cfg.TLSRequireClientCert {
		t.Fatal("expected tls client cert default to false")
	}
	if cfg.TLSClientCertFile != "" || cfg.TLSClientKeyFile != "" || cfg.TLSRootCAFile != "" {
		t.Fatalf("unexpected tls client config: %+v", cfg)
	}
	if cfg.TLSServerName != "" {
		t.Fatalf("unexpected tls server name: %s", cfg.TLSServerName)
	}
	if cfg.TLSSkipVerify {
		t.Fatal("expected tls skip verify default to false")
	}
	if cfg.TraceEnabled {
		t.Fatal("expected trace enabled default to false")
	}
	if cfg.TraceExporter != "stdout" {
		t.Fatalf("unexpected trace exporter: %s", cfg.TraceExporter)
	}
	if cfg.TraceEndpoint != "" {
		t.Fatalf("unexpected trace endpoint: %s", cfg.TraceEndpoint)
	}
	if !cfg.TraceInsecure {
		t.Fatal("expected trace insecure default to true")
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("unexpected shutdown timeout: %s", cfg.ShutdownTimeout)
	}
}

func TestFromEnvOverrides(t *testing.T) {
	t.Setenv("SERVICE_HTTP_ADDR", ":9090")
	t.Setenv("HTTP_READ_TIMEOUT", "30s")
	t.Setenv("DATA_DIR", "/tmp/logagg")
	t.Setenv("CHUNK_MAX_EVENTS", "42")
	t.Setenv("CHUNK_MAX_BYTES", "4096")
	t.Setenv("CHUNK_MAX_DURATION", "2m")
	t.Setenv("PIPELINE_JSON_IGNORE_ERROR", "false")
	t.Setenv("PIPELINE_ENABLE_LOGFMT", "false")
	t.Setenv("WAL_MAX_SEGMENT_BYTES", "16384")
	t.Setenv("WAL_SYNC_MODE", "never")
	t.Setenv("SEGMENT_BUCKET_DURATION", "30m")
	t.Setenv("QUEUE_PARTITIONS", "3")
	t.Setenv("QUEUE_BACKEND", "kafka")
	t.Setenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")
	t.Setenv("OBJECT_STORE_RETRIES", "7")
	t.Setenv("OBJECT_STORE_BACKOFF", "125ms")
	t.Setenv("QUERY_CACHE_ENTRIES", "11")
	t.Setenv("QUERY_CACHE_TTL", "2m")
	t.Setenv("CHUNK_CACHE_ENTRIES", "12")
	t.Setenv("TENANT_DSN", "postgres://tenant")
	t.Setenv("MANIFEST_DSN", "postgres://manifest")
	t.Setenv("QUERIER_URLS", "http://querier-1:8080,http://querier-2:8080")
	t.Setenv("AUTH_REQUIRED", "true")
	t.Setenv("AUTH_BEARER_TOKEN", "secret-token")
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("OIDC_ISSUER_URL", "https://issuer.example")
	t.Setenv("OIDC_JWKS_URL", "https://issuer.example/keys")
	t.Setenv("OIDC_AUDIENCE", "logagg")
	t.Setenv("OIDC_SCOPES_CLAIM", "scp")
	t.Setenv("AUDIT_LOG_PATH", "/tmp/logagg/audit.log")
	t.Setenv("TLS_ENABLED", "true")
	t.Setenv("TLS_CERT_FILE", "/tmp/tls/server.crt")
	t.Setenv("TLS_KEY_FILE", "/tmp/tls/server.key")
	t.Setenv("TLS_CLIENT_CA_FILE", "/tmp/tls/ca.crt")
	t.Setenv("TLS_REQUIRE_CLIENT_CERT", "true")
	t.Setenv("TLS_CLIENT_CERT_FILE", "/tmp/tls/client.crt")
	t.Setenv("TLS_CLIENT_KEY_FILE", "/tmp/tls/client.key")
	t.Setenv("TLS_ROOT_CA_FILE", "/tmp/tls/root.crt")
	t.Setenv("TLS_SERVER_NAME", "querier.internal")
	t.Setenv("TLS_SKIP_VERIFY", "true")
	t.Setenv("TRACE_ENABLED", "true")
	t.Setenv("TRACE_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "collector:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")

	cfg := FromEnv("querier")

	if cfg.Port != 9090 {
		t.Fatalf("unexpected port: %d", cfg.Port)
	}
	if cfg.ReadTimeout != 30*time.Second {
		t.Fatalf("unexpected read timeout: %s", cfg.ReadTimeout)
	}
	if cfg.DataDir != "/tmp/logagg" {
		t.Fatalf("unexpected data dir: %s", cfg.DataDir)
	}
	if cfg.ChunkMaxEvents != 42 {
		t.Fatalf("unexpected chunk max events: %d", cfg.ChunkMaxEvents)
	}
	if cfg.ChunkMaxBytes != 4096 {
		t.Fatalf("unexpected chunk max bytes: %d", cfg.ChunkMaxBytes)
	}
	if cfg.ChunkMaxDuration != 2*time.Minute {
		t.Fatalf("unexpected chunk max duration: %s", cfg.ChunkMaxDuration)
	}
	if cfg.JSONIgnoreError {
		t.Fatal("expected json ignore error override to false")
	}
	if cfg.EnableLogfmt {
		t.Fatal("expected logfmt override to false")
	}
	if cfg.WALMaxSegmentBytes != 16384 {
		t.Fatalf("unexpected wal segment bytes: %d", cfg.WALMaxSegmentBytes)
	}
	if cfg.WALSyncMode != "never" {
		t.Fatalf("unexpected wal sync mode: %s", cfg.WALSyncMode)
	}
	if cfg.SegmentBucket != 30*time.Minute {
		t.Fatalf("unexpected segment bucket: %s", cfg.SegmentBucket)
	}
	if cfg.QueuePartitions != 3 {
		t.Fatalf("unexpected queue partitions: %d", cfg.QueuePartitions)
	}
	if cfg.QueueBackend != "kafka" {
		t.Fatalf("unexpected queue backend: %s", cfg.QueueBackend)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "kafka-1:9092" {
		t.Fatalf("unexpected kafka brokers: %+v", cfg.KafkaBrokers)
	}
	if cfg.ObjectStoreRetries != 7 {
		t.Fatalf("unexpected object store retries: %d", cfg.ObjectStoreRetries)
	}
	if cfg.ObjectStoreBackoff != 125*time.Millisecond {
		t.Fatalf("unexpected object store backoff: %s", cfg.ObjectStoreBackoff)
	}
	if cfg.QueryCacheEntries != 11 {
		t.Fatalf("unexpected query cache entries: %d", cfg.QueryCacheEntries)
	}
	if cfg.QueryCacheTTL != 2*time.Minute {
		t.Fatalf("unexpected query cache ttl: %s", cfg.QueryCacheTTL)
	}
	if cfg.ChunkCacheEntries != 12 {
		t.Fatalf("unexpected chunk cache entries: %d", cfg.ChunkCacheEntries)
	}
	if cfg.TenantDSN != "postgres://tenant" {
		t.Fatalf("unexpected tenant dsn: %s", cfg.TenantDSN)
	}
	if cfg.ManifestDSN != "postgres://manifest" {
		t.Fatalf("unexpected manifest dsn: %s", cfg.ManifestDSN)
	}
	if len(cfg.QuerierURLs) != 2 {
		t.Fatalf("unexpected querier urls: %+v", cfg.QuerierURLs)
	}
	if !cfg.AuthRequired {
		t.Fatal("expected auth required override to true")
	}
	if cfg.AuthMode != "oidc" {
		t.Fatalf("unexpected auth mode: %s", cfg.AuthMode)
	}
	if cfg.AuthBearerToken != "secret-token" {
		t.Fatalf("unexpected auth bearer token: %s", cfg.AuthBearerToken)
	}
	if cfg.OIDCIssuerURL != "https://issuer.example" {
		t.Fatalf("unexpected oidc issuer url: %s", cfg.OIDCIssuerURL)
	}
	if cfg.OIDCJWKSURL != "https://issuer.example/keys" {
		t.Fatalf("unexpected oidc jwks url: %s", cfg.OIDCJWKSURL)
	}
	if cfg.OIDCAudience != "logagg" {
		t.Fatalf("unexpected oidc audience: %s", cfg.OIDCAudience)
	}
	if cfg.OIDCScopesClaim != "scp" {
		t.Fatalf("unexpected oidc scopes claim: %s", cfg.OIDCScopesClaim)
	}
	if cfg.AuditLogPath != "/tmp/logagg/audit.log" {
		t.Fatalf("unexpected audit log path: %s", cfg.AuditLogPath)
	}
	if !cfg.TLSEnabled {
		t.Fatal("expected tls enabled override to true")
	}
	if cfg.TLSCertFile != "/tmp/tls/server.crt" || cfg.TLSKeyFile != "/tmp/tls/server.key" {
		t.Fatalf("unexpected tls server config: %+v", cfg)
	}
	if cfg.TLSClientCAFile != "/tmp/tls/ca.crt" {
		t.Fatalf("unexpected tls client ca file: %s", cfg.TLSClientCAFile)
	}
	if !cfg.TLSRequireClientCert {
		t.Fatal("expected tls require client cert override to true")
	}
	if cfg.TLSClientCertFile != "/tmp/tls/client.crt" || cfg.TLSClientKeyFile != "/tmp/tls/client.key" {
		t.Fatalf("unexpected tls client config: %+v", cfg)
	}
	if cfg.TLSRootCAFile != "/tmp/tls/root.crt" {
		t.Fatalf("unexpected tls root ca file: %s", cfg.TLSRootCAFile)
	}
	if cfg.TLSServerName != "querier.internal" {
		t.Fatalf("unexpected tls server name: %s", cfg.TLSServerName)
	}
	if !cfg.TLSSkipVerify {
		t.Fatal("expected tls skip verify override to true")
	}
	if !cfg.TraceEnabled {
		t.Fatal("expected trace enabled override to true")
	}
	if cfg.TraceExporter != "otlp" {
		t.Fatalf("unexpected trace exporter: %s", cfg.TraceExporter)
	}
	if cfg.TraceEndpoint != "collector:4317" {
		t.Fatalf("unexpected trace endpoint: %s", cfg.TraceEndpoint)
	}
	if cfg.TraceInsecure {
		t.Fatal("expected trace insecure override to false")
	}
}
