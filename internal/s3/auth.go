package s3

import (
	"crypto/hmac"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

var (
	errNoAuthHeader      = errors.New("missing Authorization header")
	errInvalidAuthFmt    = errors.New("invalid Authorization header format")
	errAccessDenied      = errors.New("access key does not match")
	errSignatureMismatch = errors.New("signature does not match")
)

// authenticate performs full SigV4 signature verification for AWS4-HMAC-SHA256 requests.
// It supports both AWS4-HMAC-SHA256 (SigV4) style headers and legacy V2 "AWS <key>:<sig>" headers.
// For SigV4, full HMAC-SHA256 signature verification is performed.
func (g *Gateway) authenticate(r *http.Request) error {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return errNoAuthHeader
	}

	// SigV4 style - perform full signature verification
	if strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		return g.verifySigV4(r, auth)
	}

	// Legacy V2 style: "AWS <access-key>:<signature>"
	// For V2, we only verify access key (backward compatibility)
	if strings.HasPrefix(auth, "AWS ") {
		accessKey, ok := extractV2AccessKey(auth)
		if !ok {
			return errInvalidAuthFmt
		}
		if accessKey != g.accessKey {
			return errAccessDenied
		}
		return nil
	}

	return errInvalidAuthFmt
}

// sigV4Parts holds parsed components from AWS4-HMAC-SHA256 Authorization header
type sigV4Parts struct {
	accessKey     string
	dateStamp     string
	region        string
	service       string
	signedHeaders string
	signature     string
}

// verifySigV4 performs full AWS SigV4 signature verification
func (g *Gateway) verifySigV4(r *http.Request, authHeader string) error {
	parts, err := parseSigV4Header(authHeader)
	if err != nil {
		return err
	}

	// Verify access key
	if parts.accessKey != g.accessKey {
		return errAccessDenied
	}

	// Get the X-Amz-Date header
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}

	// Build canonical request
	canonicalRequest := buildCanonicalRequest(r, parts.signedHeaders)

	// Build string to sign
	canonicalRequestHash := sha256Hex([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" +
		parts.dateStamp + "/" + parts.region + "/" + parts.service + "/aws4_request\n" +
		canonicalRequestHash

	// Derive signing key
	signingKey := deriveSigningKey(g.secretKey, parts.dateStamp, parts.region, parts.service)

	// Compute expected signature
	expectedSig := hmacSHA256Hex(signingKey, []byte(stringToSign))

	// Compare signatures using constant-time comparison
	if !hmac.Equal([]byte(parts.signature), []byte(expectedSig)) {
		return errSignatureMismatch
	}

	return nil
}

// parseSigV4Header parses the AWS4-HMAC-SHA256 Authorization header
func parseSigV4Header(header string) (*sigV4Parts, error) {
	// Format: AWS4-HMAC-SHA256 Credential=<key>/<date>/<region>/<service>/aws4_request, SignedHeaders=<headers>, Signature=<sig>
	header = strings.TrimPrefix(header, "AWS4-HMAC-SHA256 ")

	var parts sigV4Parts

	// Split by comma, but be careful of potential commas in values
	segments := splitAuthHeader(header)

	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if strings.HasPrefix(segment, "Credential=") {
			cred := strings.TrimPrefix(segment, "Credential=")
			// cred looks like "AKID/20250101/us-east-1/s3/aws4_request"
			credParts := strings.Split(cred, "/")
			if len(credParts) < 5 {
				return nil, errInvalidAuthFmt
			}
			parts.accessKey = credParts[0]
			parts.dateStamp = credParts[1]
			parts.region = credParts[2]
			parts.service = credParts[3]
		} else if strings.HasPrefix(segment, "SignedHeaders=") {
			parts.signedHeaders = strings.TrimPrefix(segment, "SignedHeaders=")
		} else if strings.HasPrefix(segment, "Signature=") {
			parts.signature = strings.TrimPrefix(segment, "Signature=")
		}
	}

	if parts.accessKey == "" || parts.signedHeaders == "" || parts.signature == "" {
		return nil, errInvalidAuthFmt
	}

	return &parts, nil
}

// splitAuthHeader splits the Authorization header value by comma,
// handling potential quoted values
func splitAuthHeader(header string) []string {
	var segments []string
	var current strings.Builder
	inQuotes := false

	for _, ch := range header {
		switch ch {
		case '"':
			inQuotes = !inQuotes
			current.WriteRune(ch)
		case ',':
			if !inQuotes {
				segments = append(segments, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}
	if current.Len() > 0 {
		segments = append(segments, current.String())
	}

	return segments
}

// buildCanonicalRequest builds the AWS SigV4 canonical request string
func buildCanonicalRequest(r *http.Request, signedHeaders string) string {
	// 1. HTTP Method
	method := r.Method

	// 2. Canonical URI (path)
	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}

	// 3. Canonical Query String
	canonicalQueryString := buildCanonicalQueryString(r.URL.Query())

	// 4. Canonical Headers
	canonicalHeaders := buildCanonicalHeaders(r, signedHeaders)

	// 5. Signed Headers (already provided)

	// 6. Hashed Payload
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		// For requests without payload hash, use UNSIGNED-PAYLOAD
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	return method + "\n" +
		path + "\n" +
		canonicalQueryString + "\n" +
		canonicalHeaders + "\n" +
		signedHeaders + "\n" +
		payloadHash
}

// buildCanonicalQueryString builds the canonical query string for SigV4
func buildCanonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	// Sort keys
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		values := query[k]
		sort.Strings(values)
		for _, v := range values {
			parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
	}

	return strings.Join(parts, "&")
}

// buildCanonicalHeaders builds the canonical headers string for SigV4
func buildCanonicalHeaders(r *http.Request, signedHeaders string) string {
	signedList := strings.Split(signedHeaders, ";")
	var builder strings.Builder

	for _, h := range signedList {
		h = strings.TrimSpace(strings.ToLower(h))
		if h == "" {
			continue
		}

		// Get all values for this header
		values := r.Header.Values(http.CanonicalHeaderKey(h))
		if len(values) == 0 {
			// Try lowercase
			values = r.Header.Values(h)
		}

		// Special handling for Host header - use r.Host as fallback
		if h == "host" && len(values) == 0 {
			values = []string{r.Host}
		}

		// Trim and join multiple values
		var trimmedVals []string
		for _, v := range values {
			trimmedVals = append(trimmedVals, strings.TrimSpace(v))
		}

		builder.WriteString(h)
		builder.WriteString(":")
		builder.WriteString(strings.Join(trimmedVals, ","))
		builder.WriteString("\n")
	}

	return builder.String()
}

// extractV2AccessKey extracts the access key from a V2 Authorization header
func extractV2AccessKey(header string) (string, bool) {
	rest := strings.TrimPrefix(header, "AWS ")
	colon := strings.IndexByte(rest, ':')
	if colon > 0 {
		return rest[:colon], true
	}
	return "", false
}
