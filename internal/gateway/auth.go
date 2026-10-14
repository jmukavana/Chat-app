package gateway

import (
	"context"
	"net/http"
	"strings"

	"chat-platform/internal/auth"
)

type ctxKey int

const claimsKey ctxKey = iota

// Authenticate is HTTP middleware that requires a valid bearer JWT and stashes
// the claims on the request context.
func (g *Gateway) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := bearerToken(r)
		if tok == "" {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
			return
		}
		claims, err := g.verifier.Parse(tok)
		if err != nil {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or expired token")
			return
		}
		ctx := context.WithValue(r.Context(), claimsKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// claimsFrom returns the authenticated claims placed by Authenticate.
func claimsFrom(ctx context.Context) (auth.Claims, bool) {
	c, ok := ctx.Value(claimsKey).(auth.Claims)
	return c, ok
}

// bearerToken pulls the token from the Authorization header, falling back to an
// access_token query parameter. Browsers cannot set custom headers on the
// WebSocket handshake, so the query parameter is the practical path for /ws.
func bearerToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(h[len("Bearer "):])
	}
	return r.URL.Query().Get("access_token")
}
