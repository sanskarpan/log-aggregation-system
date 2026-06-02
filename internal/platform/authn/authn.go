package authn

import "net/http"

type Principal struct {
	Subject string
	Scopes  []string
}

type Authenticator interface {
	Authenticate(*http.Request) (Principal, error)
}
