package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName          string
	HTTPAddr             string
	LogLevel             string
	Environment          string
	DataDir              string
	ChunkMaxEvents       int
	ChunkMaxBytes        int
	ChunkMaxDuration     time.Duration
	JSONIgnoreError      bool
	EnableLogfmt         bool
	WALMaxSegmentBytes   int64
	WALSyncMode          string
	SegmentBucket        time.Duration
	QueuePartitions      int
	QueueBackend         string
	KafkaBrokers         []string
	ObjectStoreRetries   int
	ObjectStoreBackoff   time.Duration
	QueryCacheEntries    int
	QueryCacheTTL        time.Duration
	ChunkCacheEntries    int
	TenantDSN            string
	ManifestDSN          string
	QuerierURLs          []string
	AuthRequired         bool
	AuthMode             string
	AuthBearerToken      string
	OIDCIssuerURL        string
	OIDCJWKSURL          string
	OIDCAudience         string
	OIDCScopesClaim      string
	AuditLogPath         string
	TLSEnabled           bool
	TLSCertFile          string
	TLSKeyFile           string
	TLSClientCAFile      string
	TLSRequireClientCert bool
	TLSClientCertFile    string
	TLSClientKeyFile     string
	TLSRootCAFile        string
	TLSServerName        string
	TLSSkipVerify        bool
	TraceEnabled         bool
	TraceExporter        string
	TraceEndpoint        string
	TraceInsecure        bool
	ShutdownTimeout      time.Duration
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	BuildVersion         string
	Port                 int
}

func FromEnv(serviceName string) Config {
	httpAddr := getenv("SERVICE_HTTP_ADDR", ":8080")
	authRequired := getBool("AUTH_REQUIRED", false)
	authMode := strings.ToLower(getenv("AUTH_MODE", ""))
	if authMode == "" {
		if authRequired {
			authMode = "static"
		} else {
			authMode = "off"
		}
	}
	return Config{
		ServiceName:          serviceName,
		HTTPAddr:             httpAddr,
		LogLevel:             getenv("LOG_LEVEL", "info"),
		Environment:          getenv("APP_ENV", "development"),
		DataDir:              getenv("DATA_DIR", ".data"),
		ChunkMaxEvents:       getInt("CHUNK_MAX_EVENTS", 1000),
		ChunkMaxBytes:        getInt("CHUNK_MAX_BYTES", 256*1024),
		ChunkMaxDuration:     getDuration("CHUNK_MAX_DURATION", time.Minute),
		JSONIgnoreError:      getBool("PIPELINE_JSON_IGNORE_ERROR", true),
		EnableLogfmt:         getBool("PIPELINE_ENABLE_LOGFMT", true),
		WALMaxSegmentBytes:   getInt64("WAL_MAX_SEGMENT_BYTES", 8*1024*1024),
		WALSyncMode:          getenv("WAL_SYNC_MODE", "always"),
		SegmentBucket:        getDuration("SEGMENT_BUCKET_DURATION", time.Hour),
		QueuePartitions:      getInt("QUEUE_PARTITIONS", 8),
		QueueBackend:         getenv("QUEUE_BACKEND", "memory"),
		KafkaBrokers:         splitCSV(getenv("KAFKA_BROKERS", "")),
		ObjectStoreRetries:   getInt("OBJECT_STORE_RETRIES", 3),
		ObjectStoreBackoff:   getDuration("OBJECT_STORE_BACKOFF", 50*time.Millisecond),
		QueryCacheEntries:    getInt("QUERY_CACHE_ENTRIES", 256),
		QueryCacheTTL:        getDuration("QUERY_CACHE_TTL", 30*time.Second),
		ChunkCacheEntries:    getInt("CHUNK_CACHE_ENTRIES", 512),
		TenantDSN:            getenv("TENANT_DSN", ""),
		ManifestDSN:          getenv("MANIFEST_DSN", ""),
		QuerierURLs:          splitCSV(getenv("QUERIER_URLS", "")),
		AuthRequired:         authRequired,
		AuthMode:             authMode,
		AuthBearerToken:      getenv("AUTH_BEARER_TOKEN", ""),
		OIDCIssuerURL:        getenv("OIDC_ISSUER_URL", ""),
		OIDCJWKSURL:          getenv("OIDC_JWKS_URL", ""),
		OIDCAudience:         getenv("OIDC_AUDIENCE", ""),
		OIDCScopesClaim:      getenv("OIDC_SCOPES_CLAIM", "scope"),
		AuditLogPath:         getenv("AUDIT_LOG_PATH", ""),
		TLSEnabled:           getBool("TLS_ENABLED", false),
		TLSCertFile:          getenv("TLS_CERT_FILE", ""),
		TLSKeyFile:           getenv("TLS_KEY_FILE", ""),
		TLSClientCAFile:      getenv("TLS_CLIENT_CA_FILE", ""),
		TLSRequireClientCert: getBool("TLS_REQUIRE_CLIENT_CERT", false),
		TLSClientCertFile:    getenv("TLS_CLIENT_CERT_FILE", ""),
		TLSClientKeyFile:     getenv("TLS_CLIENT_KEY_FILE", ""),
		TLSRootCAFile:        getenv("TLS_ROOT_CA_FILE", ""),
		TLSServerName:        getenv("TLS_SERVER_NAME", ""),
		TLSSkipVerify:        getBool("TLS_SKIP_VERIFY", false),
		TraceEnabled:         getBool("TRACE_ENABLED", false),
		TraceExporter:        getenv("TRACE_EXPORTER", "stdout"),
		TraceEndpoint:        getenv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		TraceInsecure:        getBool("OTEL_EXPORTER_OTLP_INSECURE", true),
		ShutdownTimeout:      getDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
		ReadTimeout:          getDuration("HTTP_READ_TIMEOUT", 15*time.Second),
		WriteTimeout:         getDuration("HTTP_WRITE_TIMEOUT", 15*time.Second),
		BuildVersion:         getenv("BUILD_VERSION", "0.1.0-dev"),
		Port:                 getPort(httpAddr),
	}
}

func getenv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	value := getenv(key, fallback.String())
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return duration
}

func getPort(httpAddr string) int {
	if httpAddr == "" {
		return 8080
	}
	if httpAddr[0] == ':' {
		port, err := strconv.Atoi(httpAddr[1:])
		if err == nil {
			return port
		}
	}
	return 8080
}

func getInt(key string, fallback int) int {
	value := getenv(key, "")
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getInt64(key string, fallback int64) int64 {
	value := getenv(key, "")
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func getBool(key string, fallback bool) bool {
	value := strings.TrimSpace(getenv(key, ""))
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func splitCSV(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
