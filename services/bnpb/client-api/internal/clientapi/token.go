package clientapi

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type AccessClaims struct {
	Issuer       string `json:"iss"`
	Audience     string `json:"aud"`
	Subject      string `json:"sub"`
	ClientID     string `json:"client_id"`
	Scope        string `json:"scope"`
	SessionID    string `json:"session_id"`
	TokenVersion int64  `json:"token_version"`
	IssuedAt     int64  `json:"iat"`
	ExpiresAt    int64  `json:"exp"`
	TokenID      string `json:"jti"`
}

var knownScopes = map[string]bool{"hazards:summary": true, "hazards:raw": true, "reports:write": true, "ops:read": true}

func hasScope(scopes, wanted string) bool {
	for _, scope := range strings.Fields(scopes) {
		if scope == wanted {
			return true
		}
	}

	return false
}

func validScopes(scopes string) bool {
	seen := map[string]bool{}
	for _, scope := range strings.Fields(scopes) {
		if seen[scope] || !knownScopes[scope] {
			return false
		}

		seen[scope] = true
	}

	return seen["hazards:summary"] || seen["hazards:raw"]
}

func verifyAccessToken(
	token string,
	key ed25519.PublicKey,
	issuer, audience string,
	now time.Time,
) (AccessClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return AccessClaims{}, errors.New("malformed token")
	}

	if err := verifySignature(parts, key); err != nil {
		return AccessClaims{}, err
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return AccessClaims{}, errors.New("malformed token payload")
	}

	var claims AccessClaims
	if json.Unmarshal(payload, &claims) != nil {
		return AccessClaims{}, errors.New("malformed token claims")
	}

	if err = checkClaims(claims, issuer, audience, now); err != nil {
		return AccessClaims{}, err
	}

	return claims, nil
}

// verifySignature accepts only an EdDSA JWT header and a valid Ed25519 signature.
func verifySignature(parts []string, key ed25519.PublicKey) error {
	enc := base64.RawURLEncoding

	headerBytes, err := enc.DecodeString(parts[0])
	if err != nil {
		return errors.New("malformed token header")
	}

	var header map[string]string
	if json.Unmarshal(headerBytes, &header) != nil || header["alg"] != "EdDSA" || header["typ"] != "JWT" {
		return errors.New("unsupported token algorithm")
	}

	signature, err := enc.DecodeString(parts[2])
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("malformed token signature")
	}

	if !ed25519.Verify(key, []byte(parts[0]+"."+parts[1]), signature) {
		return errors.New("invalid token signature")
	}

	return nil
}

func checkClaims(claims AccessClaims, issuer, audience string, now time.Time) error {
	if claims.Issuer != issuer || claims.Audience != audience || claims.Subject == "" ||
		claims.Subject != claims.ClientID || !validScopes(claims.Scope) ||
		claims.SessionID == "" || claims.TokenVersion < 1 || claims.TokenID == "" {
		return errors.New("invalid token claims")
	}

	if claims.IssuedAt <= 0 || claims.ExpiresAt <= claims.IssuedAt || now.Unix() >= claims.ExpiresAt ||
		claims.IssuedAt > now.Add(time.Minute).Unix() {
		return errors.New("expired or invalid token time")
	}

	return nil
}
