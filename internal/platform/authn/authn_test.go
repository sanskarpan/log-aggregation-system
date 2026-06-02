package authn

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestStaticTokenAuthenticator(t *testing.T) {
	auth := NewStaticTokenAuthenticator("secret")
	req := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", nil)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("X-Scopes", "ingest:write query:read")

	principal, err := auth.Authenticate(req)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if principal.Subject != "static-token" {
		t.Fatalf("unexpected subject: %s", principal.Subject)
	}
	if len(principal.Scopes) != 2 {
		t.Fatalf("unexpected scopes: %+v", principal.Scopes)
	}
}

func TestOIDCAuthenticator(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	kid := "test-key"
	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{
				{
					"kty": "RSA",
					"kid": kid,
					"n":   base64.RawURLEncoding.EncodeToString(priv.N.Bytes()),
					"e":   base64.RawURLEncoding.EncodeToString(bigEndianExponent(priv.E)),
				},
			},
		})
	}))
	defer jwksServer.Close()

	auth, err := NewOIDCAuthenticator(OIDCConfig{
		IssuerURL:   "https://issuer.example",
		JWKSURL:     jwksServer.URL,
		Audience:    "logagg",
		ScopesClaim: "scope",
	})
	if err != nil {
		t.Fatalf("new oidc authenticator: %v", err)
	}

	now := time.Now().UTC()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":   "https://issuer.example",
		"sub":   "user-1",
		"aud":   []string{"logagg"},
		"exp":   now.Add(time.Hour).Unix(),
		"iat":   now.Unix(),
		"scope": "ingest:write query:read",
	})
	token.Header["kid"] = kid
	signed, err := token.SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/native/v1/search", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	principal, err := auth.Authenticate(req)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if principal.Subject != "user-1" {
		t.Fatalf("unexpected subject: %s", principal.Subject)
	}
	if len(principal.Scopes) != 2 {
		t.Fatalf("unexpected scopes: %+v", principal.Scopes)
	}
}

func bigEndianExponent(e int) []byte {
	if e == 0 {
		return []byte{0}
	}
	var out []byte
	for e > 0 {
		out = append([]byte{byte(e & 0xff)}, out...)
		e >>= 8
	}
	return out
}
