package s3

import (
	"errors"
	"net/http"
	"strings"
)

var (
	errNoAuthHeader   = errors.New("missing Authorization header")
	errInvalidAuthFmt = errors.New("invalid Authorization header format")
	errAccessDenied   = errors.New("access key does not match")
)

// authenticate performs a basic credential check against the Authorization header.
// It supports both AWS4-HMAC-SHA256 (SigV4) style headers and simple "AWS <key>:<sig>" headers.
// For Phase 3 we only verify that the access key matches; full signature verification is deferred.
func (g *Gateway) authenticate(r *http.Request) error {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return errNoAuthHeader
	}

	accessKey, ok := extractAccessKey(auth)
	if !ok {
		return errInvalidAuthFmt
	}

	if accessKey != g.accessKey {
		return errAccessDenied
	}

	return nil
}

// extractAccessKey attempts to pull the access key from an Authorization header value.
// Supported formats:
//
//	AWS4-HMAC-SHA256 Credential=<access-key>/<date>/<region>/s3/aws4_request, ...
//	AWS <access-key>:<signature>
func extractAccessKey(header string) (string, bool) {
	// SigV4 style
	if strings.HasPrefix(header, "AWS4-HMAC-SHA256 ") {
		rest := strings.TrimPrefix(header, "AWS4-HMAC-SHA256 ")
		for _, part := range strings.Split(rest, ",") {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "Credential=") {
				cred := strings.TrimPrefix(part, "Credential=")
				// cred looks like "AKID/20250101/us-east-1/s3/aws4_request"
				slash := strings.IndexByte(cred, '/')
				if slash > 0 {
					return cred[:slash], true
				}
				return cred, true
			}
		}
		return "", false
	}

	// Legacy V2 style: "AWS <access-key>:<signature>"
	if strings.HasPrefix(header, "AWS ") {
		rest := strings.TrimPrefix(header, "AWS ")
		colon := strings.IndexByte(rest, ':')
		if colon > 0 {
			return rest[:colon], true
		}
		return "", false
	}

	return "", false
}
