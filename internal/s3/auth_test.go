package s3

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuth_NoHeader(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	// No Authorization header.
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAuth_WrongKey(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=WRONGKEY/20250101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc")
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAuth_CorrectKeyValidSignature(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "localhost" // Set host before setAuthHeaders so signature matches
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	// Should succeed (ListBuckets returns 200, not 403).
	if w.Code == http.StatusForbidden {
		t.Fatalf("expected auth to pass, got 403: %s", w.Body.String())
	}
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAuth_InvalidSignature(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	// Use correct access key but invalid signature
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+testAccessKey+"/20250101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=invalidsignature")
	req.Header.Set("X-Amz-Date", "20250101T000000Z")
	req.Host = "localhost"
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for invalid signature, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAuth_LegacyV2(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "AWS "+testAccessKey+":somesignature")
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code == http.StatusForbidden {
		t.Fatalf("expected auth to pass with V2 header, got 403: %s", w.Body.String())
	}
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAuth_MalformedHeader(t *testing.T) {
	gw, _ := newTestGateway()

	tests := []struct {
		name   string
		header string
	}{
		{
			name:   "missing credential",
			header: "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc",
		},
		{
			name:   "missing signed headers",
			header: "AWS4-HMAC-SHA256 Credential=" + testAccessKey + "/20250101/us-east-1/s3/aws4_request, Signature=abc",
		},
		{
			name:   "missing signature",
			header: "AWS4-HMAC-SHA256 Credential=" + testAccessKey + "/20250101/us-east-1/s3/aws4_request, SignedHeaders=host",
		},
		{
			name:   "unknown auth type",
			header: "Bearer token123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", tt.header)
			w := httptest.NewRecorder()
			gw.ServeHTTP(w, req)

			if w.Code != http.StatusForbidden {
				t.Fatalf("expected 403 for malformed header, got %d: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestParseSigV4Header(t *testing.T) {
	tests := []struct {
		name           string
		header         string
		expectError    bool
		expectedKey    string
		expectedDate   string
		expectedRegion string
	}{
		{
			name:           "valid header",
			header:         "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123",
			expectError:    false,
			expectedKey:    "AKIAIOSFODNN7EXAMPLE",
			expectedDate:   "20250101",
			expectedRegion: "us-east-1",
		},
		{
			name:        "missing credential",
			header:      "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc",
			expectError: true,
		},
		{
			name:        "incomplete credential",
			header:      "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20250101, SignedHeaders=host, Signature=abc",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parts, err := parseSigV4Header(tt.header)
			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if parts.accessKey != tt.expectedKey {
				t.Errorf("expected access key %q, got %q", tt.expectedKey, parts.accessKey)
			}
			if parts.dateStamp != tt.expectedDate {
				t.Errorf("expected date %q, got %q", tt.expectedDate, parts.dateStamp)
			}
			if parts.region != tt.expectedRegion {
				t.Errorf("expected region %q, got %q", tt.expectedRegion, parts.region)
			}
		})
	}
}

func TestBuildCanonicalRequest(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/key?prefix=foo&delimiter=/", nil)
	req.Host = "example.s3.amazonaws.com"
	req.Header.Set("Host", "example.s3.amazonaws.com")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	canonical := buildCanonicalRequest(req, "host")

	// Check method
	if !contains(canonical, "GET\n") {
		t.Error("canonical request should start with GET")
	}
	// Check path
	if !contains(canonical, "/bucket/key\n") {
		t.Error("canonical request should contain path")
	}
	// Check payload hash
	if !contains(canonical, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") {
		t.Error("canonical request should contain payload hash")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s[:len(substr)] == substr || contains(s[1:], substr))
}
