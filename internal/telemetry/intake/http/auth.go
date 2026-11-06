package intakehttp

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	// ErrMissingToken means no bearer token was presented at all.
	ErrMissingToken = errors.New("missing bearer token")
	// ErrInvalidToken means a token is present but cannot be trusted.
	ErrInvalidToken = errors.New("invalid bearer token")
)

// Authenticator validates the static development device JWT (ADR-003): HS256,
// a secret from the environment, exp checked against an injectable clock, and
// sub as the device id. The check is stateless on purpose: at thousands of
// devices a session lookup per request is the wrong bottleneck. Device
// registration and revocation land in later slices.
type Authenticator struct {
	secret []byte
	now    func() time.Time
}

// NewAuthenticator builds the validator. A nil clock means time.Now.
func NewAuthenticator(secret []byte, now func() time.Time) *Authenticator {
	if now == nil {
		now = time.Now
	}
	return &Authenticator{secret: secret, now: now}
}

// DeviceID validates the Authorization header and returns the device id it
// authenticates and trusts. Callers supply the header, never the request: this
// type is the credential check and the authenticate middleware owns the HTTP.
func (a *Authenticator) DeviceID(header string) (string, error) {
	raw := bearerToken(header)
	if raw == "" {
		return "", ErrMissingToken
	}

	claims := &jwt.RegisteredClaims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, fmt.Errorf("%w: unexpected signing method %s", ErrInvalidToken, t.Method.Alg())
		}
		return a.secret, nil
	},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(a.now),
	)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !token.Valid {
		return "", ErrInvalidToken
	}
	if claims.Subject == "" {
		return "", fmt.Errorf("%w: token has no subject", ErrInvalidToken)
	}
	return claims.Subject, nil
}

// bearerToken extracts the credential from an "Authorization: Bearer x" header.
func bearerToken(header string) string {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
