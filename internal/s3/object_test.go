package s3

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPutObject_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")
	body := []byte("hello, world!")

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/mykey", bytes.NewReader(body))
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()

	g.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	expectedETag := fmt.Sprintf(`"%x"`, md5.Sum(body))
	if got := resp.Header.Get("ETag"); got != expectedETag {
		t.Fatalf("expected ETag %s, got %s", expectedETag, got)
	}
}

func TestGetObject_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")
	body := []byte("the quick brown fox jumps over the lazy dog")

	// Put the object first.
	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/fox.txt", bytes.NewReader(body))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putReq.Header.Set("Content-Type", "text/plain")
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusOK {
		t.Fatalf("put failed: %d", putW.Result().StatusCode)
	}

	// Get the object.
	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket/fox.txt", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	resp := getW.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch: got %q, want %q", got, body)
	}

	if ct := resp.Header.Get("Content-Type"); ct != "text/plain" {
		t.Fatalf("expected Content-Type text/plain, got %s", ct)
	}
}

func TestGetObject_NotFound(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	req := httptest.NewRequest(http.MethodGet, "/test-bucket/nonexistent", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Result().StatusCode)
	}
}

func TestHeadObject_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")
	body := []byte("head test data")

	// Put the object.
	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/headkey", bytes.NewReader(body))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusOK {
		t.Fatalf("put failed: %d", putW.Result().StatusCode)
	}

	// Head the object.
	headReq := httptest.NewRequest(http.MethodHead, "/test-bucket/headkey", nil)
	setAuthHeaders(headReq, headReq.Method, headReq.URL.Path)
	headW := httptest.NewRecorder()
	g.ServeHTTP(headW, headReq)

	resp := headW.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if resp.Header.Get("Content-Length") == "" {
		t.Fatal("expected Content-Length header")
	}
	if resp.Header.Get("ETag") == "" {
		t.Fatal("expected ETag header")
	}

	// HEAD should have no body.
	headBody, _ := io.ReadAll(resp.Body)
	if len(headBody) != 0 {
		t.Fatalf("expected empty body for HEAD, got %d bytes", len(headBody))
	}
}

func TestDeleteObject_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")
	body := []byte("delete me")

	// Put the object.
	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/delkey", bytes.NewReader(body))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusOK {
		t.Fatalf("put failed: %d", putW.Result().StatusCode)
	}

	// Delete the object.
	delReq := httptest.NewRequest(http.MethodDelete, "/test-bucket/delkey", nil)
	setAuthHeaders(delReq, delReq.Method, delReq.URL.Path)
	delW := httptest.NewRecorder()
	g.ServeHTTP(delW, delReq)

	if delW.Result().StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", delW.Result().StatusCode)
	}

	// Verify object is gone.
	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket/delkey", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getW.Result().StatusCode)
	}
}

func TestDeleteObject_NotFound(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Delete a non-existent object -- should still return 204 (idempotent).
	req := httptest.NewRequest(http.MethodDelete, "/test-bucket/ghost", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Result().StatusCode)
	}
}
