package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/sanskar/log-aggregation-system/internal/platform/config"
)

func ServerConfig(cfg config.Config) (*tls.Config, error) {
	if !cfg.TLSEnabled && cfg.TLSCertFile == "" && cfg.TLSKeyFile == "" && cfg.TLSClientCAFile == "" {
		return nil, nil
	}
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return nil, errors.New("tls cert and key files are required when tls is enabled")
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, err
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if strings.TrimSpace(cfg.TLSClientCAFile) != "" {
		pool, err := loadCertPool(cfg.TLSClientCAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.ClientCAs = pool
		if cfg.TLSRequireClientCert {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	} else if cfg.TLSRequireClientCert {
		return nil, errors.New("tls client ca file is required when client certs are required")
	}
	return tlsCfg, nil
}

func ClientConfig(cfg config.Config) (*tls.Config, error) {
	if cfg.TLSClientCertFile == "" && cfg.TLSClientKeyFile == "" && cfg.TLSRootCAFile == "" && !cfg.TLSSkipVerify && cfg.TLSServerName == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.TLSSkipVerify,
		ServerName:         cfg.TLSServerName,
	}
	if strings.TrimSpace(cfg.TLSRootCAFile) != "" {
		pool, err := loadCertPool(cfg.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.TLSClientCertFile != "" || cfg.TLSClientKeyFile != "" {
		if cfg.TLSClientCertFile == "" || cfg.TLSClientKeyFile == "" {
			return nil, errors.New("tls client cert and key files are both required")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSClientCertFile, cfg.TLSClientKeyFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

func HTTPClient(cfg config.Config) (*http.Client, error) {
	tlsCfg, err := ClientConfig(cfg)
	if err != nil {
		return nil, err
	}
	client := &http.Client{}
	if tlsCfg == nil {
		return client, nil
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg
	client.Transport = transport
	return client, nil
}

func loadCertPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("failed to parse certificates from %s", path)
	}
	return pool, nil
}
