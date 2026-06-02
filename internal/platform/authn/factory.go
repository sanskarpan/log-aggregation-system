package authn

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/sanskar/log-aggregation-system/internal/platform/config"
)

func FromConfig(cfg config.Config) (Authenticator, error) {
	if !cfg.AuthRequired {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimSpace(cfg.AuthMode)) {
	case "", "static":
		return NewStaticTokenAuthenticator(cfg.AuthBearerToken), nil
	case "oidc":
		return NewOIDCAuthenticator(OIDCConfig{
			IssuerURL:   cfg.OIDCIssuerURL,
			JWKSURL:     cfg.OIDCJWKSURL,
			Audience:    cfg.OIDCAudience,
			ScopesClaim: cfg.OIDCScopesClaim,
			HTTPClient:  &http.Client{},
		})
	default:
		return nil, fmt.Errorf("unsupported auth mode: %s", cfg.AuthMode)
	}
}
