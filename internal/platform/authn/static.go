package authn

import (
	"errors"
	"net/http"
	"strings"
)

var ErrUnauthorized = errors.New("unauthorized")

type StaticTokenAuthenticator struct {
	token string
}

func NewStaticTokenAuthenticator(token string) *StaticTokenAuthenticator {
	return &StaticTokenAuthenticator{token: strings.TrimSpace(token)}
}

func (a *StaticTokenAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	if a.token == "" {
		return Principal{}, ErrUnauthorized
	}
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header == "" {
		return Principal{}, ErrUnauthorized
	}
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || token != a.token {
		return Principal{}, ErrUnauthorized
	}
	return Principal{
		Subject: "static-token",
		Scopes:  splitScopes(r.Header.Values("X-Scopes")),
	}, nil
}

func splitScopes(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	scopes := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		for _, scope := range strings.FieldsFunc(value, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t'
		}) {
			scope = strings.TrimSpace(scope)
			if scope == "" {
				continue
			}
			if _, ok := seen[scope]; ok {
				continue
			}
			seen[scope] = struct{}{}
			scopes = append(scopes, scope)
		}
	}
	return scopes
}
