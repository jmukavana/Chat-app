// Package auth issues and verifies the JWTs that authenticate WebSocket and
// HTTP API callers. Phase 1 uses a symmetric HS256 secret; a real deployment
// would swap this for asymmetric keys fetched from the identity provider.
package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ErrInvalidToken is returned for any token that fails parsing or validation.
var ErrInvalidToken = errors.New("invalid token")

// Claims is the token payload. Subject carries the user id.
type Claims struct {
	jwt.RegisteredClaims
}

// UserID returns the authenticated user id (the JWT subject).
func (c Claims) UserID() string { return c.Subject }

// Verifier validates incoming tokens.
type Verifier struct {
	secret []byte
	issuer string
}

// NewVerifier builds a Verifier for the given HS256 secret and expected issuer.
func NewVerifier(secret, issuer string) *Verifier {
	return &Verifier{secret: []byte(secret), issuer: issuer}
}

// Parse validates the token string and returns its claims.
func (v *Verifier) Parse(tokenString string) (Claims, error) {
	var claims Claims
	_, err := jwt.ParseWithClaims(tokenString, &claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}
		return v.secret, nil
	},
		jwt.WithIssuer(v.issuer),
		jwt.WithValidMethods([]string{"HS256"}),
	)
	if err != nil {
		return Claims{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if claims.Subject == "" {
		return Claims{}, fmt.Errorf("%w: empty subject", ErrInvalidToken)
	}
	return claims, nil
}

// Issuer mints tokens. It exists mainly so the dev-only /v1/dev/token endpoint
// can hand out credentials while there is no real identity provider wired in.
type Issuer struct {
	secret []byte
	issuer string
}

// NewIssuer builds an Issuer for the given HS256 secret and issuer string.
func NewIssuer(secret, issuer string) *Issuer {
	return &Issuer{secret: []byte(secret), issuer: issuer}
}

// Mint returns a signed HS256 token for userID valid for ttl.
func (i *Issuer) Mint(userID string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Issuer:    i.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return tok.SignedString(i.secret)
}
