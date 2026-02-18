package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
)

// --- In-memory mock stores for testing ---

type memBucketStore struct {
	mu      sync.RWMutex
	buckets map[string]*BucketInfo
}

func newMemBucketStore() *memBucketStore {
	return &memBucketStore{buckets: make(map[string]*BucketInfo)}
}

func (m *memBucketStore) PutBucket(_ context.Context, info *BucketInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets[info.Name] = info
	return nil
}

func (m *memBucketStore) GetBucket(_ context.Context, name string) (*BucketInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.buckets[name]
	if !ok {
		return nil, fmt.Errorf("bucket %q not found", name)
	}
	return b, nil
}

func (m *memBucketStore) DeleteBucket(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[name]; !ok {
		return fmt.Errorf("bucket %q not found", name)
	}
	delete(m.buckets, name)
	return nil
}

func (m *memBucketStore) ListBuckets(_ context.Context) ([]*BucketInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*BucketInfo, 0, len(m.buckets))
	for _, b := range m.buckets {
		result = append(result, b)
	}
	return result, nil
}

type memObjectStore struct {
	mu      sync.RWMutex
	objects map[string]*ObjectInfo // keyed by "bucket/key"
}

func newMemObjectStore() *memObjectStore {
	return &memObjectStore{objects: make(map[string]*ObjectInfo)}
}

func (m *memObjectStore) PutObject(_ context.Context, info *ObjectInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[info.Bucket+"/"+info.Key] = info
	return nil
}

func (m *memObjectStore) GetObject(_ context.Context, bucket, key string) (*ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	o, ok := m.objects[bucket+"/"+key]
	if !ok {
		return nil, fmt.Errorf("object %q/%q not found", bucket, key)
	}
	return o, nil
}

func (m *memObjectStore) DeleteObject(_ context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, bucket+"/"+key)
	return nil
}

func (m *memObjectStore) ListObjects(_ context.Context, bucket, prefix string) ([]*ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*ObjectInfo
	for _, o := range m.objects {
		if o.Bucket == bucket {
			if prefix == "" || strings.HasPrefix(o.Key, prefix) {
				result = append(result, o)
			}
		}
	}
	return result, nil
}

type memChunkStore struct {
	mu     sync.RWMutex
	chunks map[string][]byte
	seq    int
}

func newMemChunkStore() *memChunkStore {
	return &memChunkStore{chunks: make(map[string][]byte)}
}

func (m *memChunkStore) PutChunkData(_ context.Context, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	id := fmt.Sprintf("chunk-%d", m.seq)
	cp := make([]byte, len(data))
	copy(cp, data)
	m.chunks[id] = cp
	return id, nil
}

func (m *memChunkStore) GetChunkData(_ context.Context, chunkID string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.chunks[chunkID]
	if !ok {
		return nil, fmt.Errorf("chunk %q not found", chunkID)
	}
	return d, nil
}

func (m *memChunkStore) DeleteChunkData(_ context.Context, chunkID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.chunks, chunkID)
	return nil
}

type memMultipartStore struct {
	mu         sync.RWMutex
	multiparts map[string]*MultipartInfo
}

func newMemMultipartStore() *memMultipartStore {
	return &memMultipartStore{multiparts: make(map[string]*MultipartInfo)}
}

func (m *memMultipartStore) PutMultipart(_ context.Context, info *MultipartInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.multiparts[info.UploadID] = info
	return nil
}

func (m *memMultipartStore) GetMultipart(_ context.Context, uploadID string) (*MultipartInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mp, ok := m.multiparts[uploadID]
	if !ok {
		return nil, fmt.Errorf("multipart upload %q not found", uploadID)
	}
	return mp, nil
}

func (m *memMultipartStore) DeleteMultipart(_ context.Context, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.multiparts, uploadID)
	return nil
}

const testAccessKey = "AKIAIOSFODNN7EXAMPLE"
const testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

func newTestGateway() (*Gateway, *memBucketStore) {
	bs := newMemBucketStore()
	gw := NewGateway(bs, newMemObjectStore(), newMemChunkStore(), newMemMultipartStore(), testAccessKey, testSecretKey)
	return gw, bs
}

const testAmzDate = "20250101T000000Z"

func authHeader() string {
	return authHeaderForRequest("GET", "/", "host", "localhost", testAmzDate)
}

// setAuthHeaders sets all required authentication headers on the request
// This should be used instead of manually setting Authorization header
func setAuthHeaders(req *http.Request, method, path string) {
	host := req.Host
	if host == "" {
		host = "localhost"
	}
	// Build canonical query string for signature
	canonicalQueryString := buildTestCanonicalQueryString(req.URL.Query())
	req.Header.Set("Authorization", authHeaderForRequestWithQuery(method, path, canonicalQueryString, "host", host, testAmzDate))
	req.Header.Set("X-Amz-Date", testAmzDate)
}

// buildTestCanonicalQueryString builds a canonical query string for testing
func buildTestCanonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}
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

// authHeaderForRequestWithQuery generates a valid SigV4 Authorization header with query string
func authHeaderForRequestWithQuery(method, path, queryString, signedHeaders, host, amzDate string) string {
	dateStamp := amzDate[:8]
	credential := fmt.Sprintf("%s/%s/us-east-1/s3/aws4_request", testAccessKey, dateStamp)

	// Build canonical request - must match buildCanonicalRequest format exactly
	canonicalRequest := method + "\n" +
		path + "\n" +
		queryString + "\n" +
		"host:" + host + "\n" +
		"\n" +
		signedHeaders + "\n" +
		"UNSIGNED-PAYLOAD"

	// Build string to sign
	canonicalRequestHash := sha256Hex([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" +
		amzDate + "\n" +
		dateStamp + "/us-east-1/s3/aws4_request\n" +
		canonicalRequestHash

	// Derive signing key
	signingKey := deriveSigningKey(testSecretKey, dateStamp, "us-east-1", "s3")

	// Compute signature
	signature := hmacSHA256Hex(signingKey, []byte(stringToSign))

	return fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s, SignedHeaders=%s, Signature=%s",
		credential, signedHeaders, signature)
}

// authHeaderForRequest generates a valid SigV4 Authorization header for testing
// The canonical request format must match buildCanonicalRequest in auth.go:
//
//	Method + "\n" +
//	Path + "\n" +
//	QueryString + "\n" +
//	CanonicalHeaders + "\n" +
//	SignedHeaders + "\n" +
//	PayloadHash
func authHeaderForRequest(method, path, signedHeaders, host, amzDate string) string {
	dateStamp := amzDate[:8]
	credential := fmt.Sprintf("%s/%s/us-east-1/s3/aws4_request", testAccessKey, dateStamp)

	// Build canonical request - must match buildCanonicalRequest format exactly
	// Format: method\npath\nquery\nheaders\nsignedHeaders\npayloadHash
	// Note: buildCanonicalRequest adds each component with \n, including empty query string
	canonicalRequest := method + "\n" +
		path + "\n" +
		"" + "\n" + // query string (empty, but still has \n)
		"host:" + host + "\n" + // canonical headers
		"\n" + // blank line after headers
		signedHeaders + "\n" +
		"UNSIGNED-PAYLOAD"

	// Build string to sign
	canonicalRequestHash := sha256Hex([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" +
		amzDate + "\n" +
		dateStamp + "/us-east-1/s3/aws4_request\n" +
		canonicalRequestHash

	// Derive signing key
	signingKey := deriveSigningKey(testSecretKey, dateStamp, "us-east-1", "s3")

	// Compute signature
	signature := hmacSHA256Hex(signingKey, []byte(stringToSign))

	return fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s, SignedHeaders=%s, Signature=%s",
		credential, signedHeaders, signature)
}

// seedBucket is a helper that creates a bucket in the store for testing.
func seedBucket(t *testing.T, bs *memBucketStore, name string) {
	t.Helper()
	if err := bs.PutBucket(context.Background(), &BucketInfo{Name: name, CreationDate: 1700000000, Owner: testAccessKey}); err != nil {
		t.Fatalf("failed to seed bucket %q: %v", name, err)
	}
}

func TestCreateBucket_Success(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodPut, "/my-bucket", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if loc := w.Header().Get("Location"); loc != "/my-bucket" {
		t.Fatalf("expected Location /my-bucket, got %q", loc)
	}
}

func TestCreateBucket_AlreadyExists(t *testing.T) {
	gw, bs := newTestGateway()

	bs.PutBucket(context.Background(), &BucketInfo{Name: "existing", CreationDate: 1000, Owner: testAccessKey})

	req := httptest.NewRequest(http.MethodPut, "/existing", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}

	if !strings.Contains(w.Body.String(), "BucketAlreadyOwnedByYou") {
		t.Fatalf("expected BucketAlreadyOwnedByYou error, got: %s", w.Body.String())
	}
}

func TestDeleteBucket_Success(t *testing.T) {
	gw, bs := newTestGateway()

	bs.PutBucket(context.Background(), &BucketInfo{Name: "to-delete", CreationDate: 1000, Owner: testAccessKey})

	req := httptest.NewRequest(http.MethodDelete, "/to-delete", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteBucket_NotFound(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodDelete, "/nonexistent", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}

	if !strings.Contains(w.Body.String(), "NoSuchBucket") {
		t.Fatalf("expected NoSuchBucket error, got: %s", w.Body.String())
	}
}

func TestListBuckets(t *testing.T) {
	gw, bs := newTestGateway()

	bs.PutBucket(context.Background(), &BucketInfo{Name: "alpha", CreationDate: 1700000000, Owner: testAccessKey})
	bs.PutBucket(context.Background(), &BucketInfo{Name: "bravo", CreationDate: 1700000100, Owner: testAccessKey})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result listAllMyBucketsResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	if len(result.Buckets.Bucket) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(result.Buckets.Bucket))
	}

	if result.Owner.ID != "novastor" {
		t.Fatalf("expected owner ID 'novastor', got %q", result.Owner.ID)
	}
}

func TestHeadBucket_Exists(t *testing.T) {
	gw, bs := newTestGateway()

	bs.PutBucket(context.Background(), &BucketInfo{Name: "exists", CreationDate: 1000, Owner: testAccessKey})

	req := httptest.NewRequest(http.MethodHead, "/exists", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHeadBucket_NotFound(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodHead, "/nope", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}
