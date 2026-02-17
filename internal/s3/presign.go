package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// generatePresignedURL generates a presigned URL for an S3 object.
// The URL includes a signature computed with HMAC-SHA256 using the secret key.
func (g *Gateway) generatePresignedURL(host, bucket, key string, expires time.Duration) string {
	expireSeconds := int64(expires.Seconds())
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credential := fmt.Sprintf("%s/%s/us-east-1/s3/aws4_request", g.accessKey, dateStamp)

	path := fmt.Sprintf("/%s/%s", bucket, key)

	// Build canonical query string (sorted).
	canonicalQueryString := fmt.Sprintf(
		"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=%d&X-Amz-SignedHeaders=host",
		credential, amzDate, expireSeconds,
	)

	// Build canonical request.
	canonicalRequest := fmt.Sprintf("GET\n%s\n%s\nhost:%s\n\nhost\nUNSIGNED-PAYLOAD",
		path, canonicalQueryString, host)

	// Build string to sign.
	canonicalRequestHash := sha256Hex([]byte(canonicalRequest))
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s/us-east-1/s3/aws4_request\n%s",
		amzDate, dateStamp, canonicalRequestHash)

	// Derive signing key.
	signingKey := deriveSigningKey(g.secretKey, dateStamp, "us-east-1", "s3")

	// Compute signature.
	signature := hmacSHA256Hex(signingKey, []byte(stringToSign))

	return fmt.Sprintf("http://%s%s?%s&X-Amz-Signature=%s",
		host, path, canonicalQueryString, signature)
}

// handlePresignedURL validates a presigned URL and serves the object if valid.
func (g *Gateway) handlePresignedURL(w http.ResponseWriter, r *http.Request, bucket, key string) {
	query := r.URL.Query()

	algorithm := query.Get("X-Amz-Algorithm")
	if algorithm != "AWS4-HMAC-SHA256" {
		writeS3Error(w, "AuthorizationQueryParametersError", "unsupported algorithm", http.StatusBadRequest)
		return
	}

	amzDate := query.Get("X-Amz-Date")
	if amzDate == "" {
		writeS3Error(w, "AuthorizationQueryParametersError", "missing X-Amz-Date", http.StatusBadRequest)
		return
	}

	expiresStr := query.Get("X-Amz-Expires")
	expiresSec, err := strconv.ParseInt(expiresStr, 10, 64)
	if err != nil || expiresSec <= 0 {
		writeS3Error(w, "AuthorizationQueryParametersError", "invalid X-Amz-Expires", http.StatusBadRequest)
		return
	}

	// Parse the request date and check expiration.
	requestTime, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		writeS3Error(w, "AuthorizationQueryParametersError", "invalid X-Amz-Date format", http.StatusBadRequest)
		return
	}

	if time.Since(requestTime) > time.Duration(expiresSec)*time.Second {
		writeS3Error(w, "AccessDenied", "Request has expired", http.StatusForbidden)
		return
	}

	providedSig := query.Get("X-Amz-Signature")
	if providedSig == "" {
		writeS3Error(w, "AccessDenied", "missing signature", http.StatusForbidden)
		return
	}

	// Recompute the signature to verify.
	dateStamp := requestTime.Format("20060102")
	credential := fmt.Sprintf("%s/%s/us-east-1/s3/aws4_request", g.accessKey, dateStamp)

	path := fmt.Sprintf("/%s/%s", bucket, key)
	canonicalQueryString := fmt.Sprintf(
		"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=%d&X-Amz-SignedHeaders=host",
		credential, amzDate, expiresSec,
	)

	host := r.Host
	canonicalRequest := fmt.Sprintf("GET\n%s\n%s\nhost:%s\n\nhost\nUNSIGNED-PAYLOAD",
		path, canonicalQueryString, host)

	canonicalRequestHash := sha256Hex([]byte(canonicalRequest))
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s/us-east-1/s3/aws4_request\n%s",
		amzDate, dateStamp, canonicalRequestHash)

	signingKey := deriveSigningKey(g.secretKey, dateStamp, "us-east-1", "s3")
	expectedSig := hmacSHA256Hex(signingKey, []byte(stringToSign))

	if !hmac.Equal([]byte(providedSig), []byte(expectedSig)) {
		writeS3Error(w, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided", http.StatusForbidden)
		return
	}

	// Signature is valid; serve the object.
	g.handleGetObject(w, r, bucket, key)
}

// sha256Hex returns the hex-encoded SHA256 hash of data.
func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// hmacSHA256 computes HMAC-SHA256 of data using the given key.
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// hmacSHA256Hex computes HMAC-SHA256 and returns the hex-encoded result.
func hmacSHA256Hex(key, data []byte) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

// deriveSigningKey derives the AWS SigV4 signing key.
func deriveSigningKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}
