package authn

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type OIDCConfig struct {
	IssuerURL   string
	JWKSURL     string
	Audience    string
	ScopesClaim string
	HTTPClient  *http.Client
}

type OIDCAuthenticator struct {
	cfg    OIDCConfig
	mu     sync.RWMutex
	keys   map[string]*rsa.PublicKey
	client *http.Client
}

type oidcClaims struct {
	jwt.RegisteredClaims
	Scope any `json:"scope,omitempty"`
	Scp   any `json:"scp,omitempty"`
}

func NewOIDCAuthenticator(cfg OIDCConfig) (*OIDCAuthenticator, error) {
	cfg.IssuerURL = strings.TrimSpace(cfg.IssuerURL)
	cfg.JWKSURL = strings.TrimSpace(cfg.JWKSURL)
	cfg.Audience = strings.TrimSpace(cfg.Audience)
	if cfg.IssuerURL == "" || cfg.JWKSURL == "" {
		return nil, fmt.Errorf("issuer url and jwks url are required")
	}
	if cfg.ScopesClaim == "" {
		cfg.ScopesClaim = "scope"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &OIDCAuthenticator{
		cfg:    cfg,
		keys:   map[string]*rsa.PublicKey{},
		client: client,
	}, nil
}

func (a *OIDCAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header == "" {
		return Principal{}, ErrUnauthorized
	}
	tokenString, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		return Principal{}, ErrUnauthorized
	}

	claims := &oidcClaims{}
	parsed, err := jwt.ParseWithClaims(tokenString, claims, a.keyFunc,
		jwt.WithValidMethods([]string{"RS256"}),
	)
	if err != nil || !parsed.Valid {
		return Principal{}, ErrUnauthorized
	}
	if claims.Issuer != a.cfg.IssuerURL {
		return Principal{}, ErrUnauthorized
	}
	if a.cfg.Audience != "" {
		matched := false
		for _, aud := range claims.Audience {
			if aud == a.cfg.Audience {
				matched = true
				break
			}
		}
		if !matched {
			return Principal{}, ErrUnauthorized
		}
	}

	scopes := extractScopes(claims, a.cfg.ScopesClaim)
	return Principal{
		Subject: claims.Subject,
		Scopes:  scopes,
	}, nil
}

func (a *OIDCAuthenticator) keyFunc(token *jwt.Token) (any, error) {
	kid, _ := token.Header["kid"].(string)
	if kid == "" {
		return nil, ErrUnauthorized
	}
	if key := a.lookupKey(kid); key != nil {
		return key, nil
	}
	if err := a.refreshKeys(); err != nil {
		return nil, err
	}
	if key := a.lookupKey(kid); key != nil {
		return key, nil
	}
	return nil, ErrUnauthorized
}

func (a *OIDCAuthenticator) lookupKey(kid string) *rsa.PublicKey {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.keys[kid]
}

func (a *OIDCAuthenticator) refreshKeys() error {
	res, err := a.client.Get(a.cfg.JWKSURL)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
		return fmt.Errorf("jwks fetch failed: %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	var doc struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(res.Body).Decode(&doc); err != nil {
		return err
	}

	next := make(map[string]*rsa.PublicKey, len(doc.Keys))
	for _, key := range doc.Keys {
		if key.Kid == "" || strings.ToUpper(key.Kty) != "RSA" {
			continue
		}
		pub, err := rsaPublicKey(key.N, key.E)
		if err != nil {
			continue
		}
		next[key.Kid] = pub
	}
	if len(next) == 0 {
		return errors.New("jwks contained no usable rsa keys")
	}

	a.mu.Lock()
	a.keys = next
	a.mu.Unlock()
	return nil
}

func rsaPublicKey(nValue, eValue string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nValue)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(eValue)
	if err != nil {
		return nil, err
	}
	if len(eBytes) == 0 {
		return nil, errors.New("empty exponent")
	}
	exp := 0
	for _, b := range eBytes {
		exp = exp<<8 + int(b)
	}
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: exp,
	}, nil
}

func extractScopes(claims *oidcClaims, claimName string) []string {
	var raw any
	switch claimName {
	case "scp":
		raw = claims.Scp
	default:
		raw = claims.Scope
	}
	switch value := raw.(type) {
	case string:
		return splitScopes([]string{value})
	case []string:
		return splitScopes(value)
	case []any:
		values := make([]string, 0, len(value))
		for _, item := range value {
			if s, ok := item.(string); ok {
				values = append(values, s)
			}
		}
		return splitScopes(values)
	default:
		return nil
	}
}
