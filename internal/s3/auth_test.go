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

func TestAuth_CorrectKey(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", authHeader())
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
