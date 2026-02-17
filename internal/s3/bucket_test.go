package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
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

func authHeader() string {
	return fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/20250101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc", testAccessKey)
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
	req.Header.Set("Authorization", authHeader())
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
	req.Header.Set("Authorization", authHeader())
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
	req.Header.Set("Authorization", authHeader())
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteBucket_NotFound(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodDelete, "/nonexistent", nil)
	req.Header.Set("Authorization", authHeader())
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
	req.Header.Set("Authorization", authHeader())
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
	req.Header.Set("Authorization", authHeader())
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHeadBucket_NotFound(t *testing.T) {
	gw, _ := newTestGateway()

	req := httptest.NewRequest(http.MethodHead, "/nope", nil)
	req.Header.Set("Authorization", authHeader())
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}
