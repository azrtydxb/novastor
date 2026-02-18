package s3

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// putTestObject is a helper that puts an object via the gateway's HTTP interface.
func putTestObject(t *testing.T, g *Gateway, bucket, key string, data []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+key, bytes.NewReader(data))
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("putTestObject %s/%s failed: %d", bucket, key, w.Result().StatusCode)
	}
}

func parseListResult(t *testing.T, body []byte) listBucketResult {
	t.Helper()
	var result listBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse ListBucketResult XML: %v\nbody: %s", err, string(body))
	}
	return result
}

func TestListObjectsV2_Basic(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	putTestObject(t, g, "test-bucket", "file1.txt", []byte("one"))
	putTestObject(t, g, "test-bucket", "file2.txt", []byte("two"))
	putTestObject(t, g, "test-bucket", "file3.txt", []byte("three"))

	req := httptest.NewRequest(http.MethodGet, "/test-bucket", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	result := parseListResult(t, body)

	if result.Name != "test-bucket" {
		t.Fatalf("expected Name test-bucket, got %s", result.Name)
	}
	if len(result.Contents) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(result.Contents))
	}
}

func TestListObjectsV2_WithPrefix(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	putTestObject(t, g, "test-bucket", "docs/readme.md", []byte("readme"))
	putTestObject(t, g, "test-bucket", "docs/guide.md", []byte("guide"))
	putTestObject(t, g, "test-bucket", "images/logo.png", []byte("logo"))

	req := httptest.NewRequest(http.MethodGet, "/test-bucket?prefix=docs/", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	result := parseListResult(t, body)

	if len(result.Contents) != 2 {
		t.Fatalf("expected 2 objects with prefix docs/, got %d", len(result.Contents))
	}

	for _, c := range result.Contents {
		if len(c.Key) < 5 || c.Key[:5] != "docs/" {
			t.Fatalf("unexpected key %s not matching prefix docs/", c.Key)
		}
	}
}

func TestListObjectsV2_WithDelimiter(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	putTestObject(t, g, "test-bucket", "photos/2024/jan.jpg", []byte("jan"))
	putTestObject(t, g, "test-bucket", "photos/2024/feb.jpg", []byte("feb"))
	putTestObject(t, g, "test-bucket", "photos/2025/mar.jpg", []byte("mar"))
	putTestObject(t, g, "test-bucket", "photos/top.jpg", []byte("top"))

	req := httptest.NewRequest(http.MethodGet, "/test-bucket?prefix=photos/&delimiter=/", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	result := parseListResult(t, body)

	// "photos/top.jpg" should be in Contents (no sub-delimiter).
	if len(result.Contents) != 1 {
		t.Fatalf("expected 1 content entry, got %d", len(result.Contents))
	}
	if result.Contents[0].Key != "photos/top.jpg" {
		t.Fatalf("expected photos/top.jpg, got %s", result.Contents[0].Key)
	}

	// "photos/2024/" and "photos/2025/" should be in CommonPrefixes.
	if len(result.CommonPrefixes) != 2 {
		t.Fatalf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
	}

	prefixes := make(map[string]bool)
	for _, cp := range result.CommonPrefixes {
		prefixes[cp.Prefix] = true
	}
	if !prefixes["photos/2024/"] || !prefixes["photos/2025/"] {
		t.Fatalf("expected common prefixes photos/2024/ and photos/2025/, got %v", prefixes)
	}
}

func TestListObjectsV2_MaxKeys(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	putTestObject(t, g, "test-bucket", "a.txt", []byte("a"))
	putTestObject(t, g, "test-bucket", "b.txt", []byte("b"))
	putTestObject(t, g, "test-bucket", "c.txt", []byte("c"))

	req := httptest.NewRequest(http.MethodGet, "/test-bucket?max-keys=2", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	result := parseListResult(t, body)

	if result.MaxKeys != 2 {
		t.Fatalf("expected MaxKeys 2, got %d", result.MaxKeys)
	}
	if !result.IsTruncated {
		t.Fatal("expected IsTruncated=true")
	}
	if len(result.Contents) != 2 {
		t.Fatalf("expected 2 content entries, got %d", len(result.Contents))
	}
}

func TestListObjectsV2_EmptyBucket(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	req := httptest.NewRequest(http.MethodGet, "/test-bucket", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	result := parseListResult(t, body)

	if len(result.Contents) != 0 {
		t.Fatalf("expected 0 objects in empty bucket, got %d", len(result.Contents))
	}
	if result.IsTruncated {
		t.Fatal("expected IsTruncated=false for empty bucket")
	}
}
