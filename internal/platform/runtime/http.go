package runtime

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/sanskar/log-aggregation-system/internal/platform/authn"
	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/httpguard"
	"github.com/sanskar/log-aggregation-system/internal/platform/tlsconfig"
	"github.com/sanskar/log-aggregation-system/internal/platform/tracing"
)

func RunHTTP(cfg config.Config, handler http.Handler, providers ...httpguard.MetricProvider) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	authenticator, err := authn.FromConfig(cfg)
	if err != nil {
		return err
	}

	traceShutdown, err := tracing.Setup(ctx, tracing.Config{
		ServiceName: cfg.ServiceName,
		Enabled:     cfg.TraceEnabled,
		Exporter:    cfg.TraceExporter,
		Endpoint:    cfg.TraceEndpoint,
		Insecure:    cfg.TraceInsecure,
	})
	if err != nil {
		return err
	}
	defer traceShutdown(context.Background())

	serverTLS, err := tlsconfig.ServerConfig(cfg)
	if err != nil {
		return err
	}

	guard := httpguard.New(httpguard.Config{
		ServiceName:     cfg.ServiceName,
		AuthRequired:    cfg.AuthRequired,
		Authenticator:   authenticator,
		AuditPath:       cfg.AuditLogPath,
		MetricProviders: providers,
	})

	server := &http.Server{
		Addr:         cfg.HTTPAddr,
		TLSConfig:    serverTLS,
		Handler:      tracing.WrapHandler(cfg.ServiceName, guard.Wrap(handler)),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	errCh := make(chan error, 1)

	go func() {
		log.Printf("%s listening on %s", cfg.ServiceName, cfg.HTTPAddr)
		var serveErr error
		if serverTLS != nil {
			serveErr = server.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			serveErr = server.ListenAndServe()
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
