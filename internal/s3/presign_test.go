package s3

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPresignedURL_Generate(t *testing.T) {
	gw, _ := newTestGateway()

	presignedURL := gw.generatePresignedURL("localhost:8080", "test-bucket", "myobject.txt", 15*time.Minute)

	if presignedURL == "" {
		t.Fatal("expected non-empty presigned URL")
	}

	if !strings.Contains(presignedURL, "X-Amz-Algorithm=AWS4-HMAC-SHA256") {
		t.Error("presigned URL should contain X-Amz-Algorithm")
	}
	if !strings.Contains(presignedURL, "X-Amz-Credential=") {
		t.Error("presigned URL should contain X-Amz-Credential")
	}
	if !strings.Contains(presignedURL, "X-Amz-Signature=") {
		t.Error("presigned URL should contain X-Amz-Signature")
	}
	if !strings.Contains(presignedURL, "X-Amz-Expires=900") {
		t.Error("presigned URL should contain X-Amz-Expires=900")
	}
	if !strings.Contains(presignedURL, "/test-bucket/myobject.txt") {
		t.Error("presigned URL should contain the object path")
	}
}

func TestPresignedURL_Access(t *testing.T) {
	gw, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// First, put an object via the normal authenticated path.
	body := []byte("presigned test content")
	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/presigned-key", bytes.NewReader(body))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	gw.ServeHTTP(putW, putReq)

	if putW.Code != http.StatusOK {
		t.Fatalf("put failed: %d", putW.Code)
	}

	// Generate a presigned URL.
	presignedURLStr := gw.generatePresignedURL("localhost", "test-bucket", "presigned-key", 15*time.Minute)

	// Parse the presigned URL and make a request to our gateway with those query params.
	parsedURL, err := url.Parse(presignedURLStr)
	if err != nil {
		t.Fatalf("failed to parse presigned URL: %v", err)
	}

	// Create a request with the presigned query parameters.
	getReq := httptest.NewRequest(http.MethodGet, parsedURL.RequestURI(), nil)
	getReq.Host = "localhost"
	getW := httptest.NewRecorder()

	// We need to bypass authentication for presigned URLs.
	// Test by calling handlePresignedURL directly.
	gw.handlePresignedURL(getW, getReq, "test-bucket", "presigned-key")

	resp := getW.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, getW.Body.String())
	}

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch: got %q, want %q", got, body)
	}
}

func TestPresignedURL_Expired(t *testing.T) {
	gw, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Put an object.
	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/expiring", bytes.NewReader([]byte("data")))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	gw.ServeHTTP(putW, putReq)

	// Generate a presigned URL that has already expired (0 seconds).
	// We simulate this by creating a URL with an X-Amz-Date in the past.
	pastTime := time.Now().UTC().Add(-2 * time.Hour)
	dateStamp := pastTime.Format("20060102")
	amzDate := pastTime.Format("20060102T150405Z")
	credential := testAccessKey + "/" + dateStamp + "/us-east-1/s3/aws4_request"

	queryStr := "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=" + credential +
		"&X-Amz-Date=" + amzDate +
		"&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=invalidsig"

	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket/expiring?"+queryStr, nil)
	getReq.Host = "localhost"
	getW := httptest.NewRecorder()

	gw.handlePresignedURL(getW, getReq, "test-bucket", "expiring")

	if getW.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for expired URL, got %d", getW.Code)
	}
}
