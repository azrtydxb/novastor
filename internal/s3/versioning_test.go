package s3

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPutBucketVersioning_Enable(t *testing.T) {
	gw, bs := newTestGateway()
	seedBucket(t, bs, "ver-bucket")

	body := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/ver-bucket?versioning", bytes.NewReader([]byte(body)))
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()

	gw.handlePutBucketVersioning(w, req, "ver-bucket")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify the bucket's versioning status was updated.
	bInfo, err := bs.GetBucket(req.Context(), "ver-bucket")
	if err != nil {
		t.Fatalf("failed to get bucket: %v", err)
	}
	if bInfo.Versioning != "Enabled" {
		t.Errorf("expected versioning Enabled, got %q", bInfo.Versioning)
	}
}

func TestGetBucketVersioning(t *testing.T) {
	gw, bs := newTestGateway()
	seedBucket(t, bs, "ver-bucket")

	// Enable versioning first.
	enableBody := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	enableReq := httptest.NewRequest(http.MethodPut, "/ver-bucket?versioning", bytes.NewReader([]byte(enableBody)))
	setAuthHeaders(enableReq, enableReq.Method, enableReq.URL.Path)
	enableW := httptest.NewRecorder()
	gw.handlePutBucketVersioning(enableW, enableReq, "ver-bucket")

	// Now get versioning status.
	getReq := httptest.NewRequest(http.MethodGet, "/ver-bucket?versioning", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	gw.handleGetBucketVersioning(getW, getReq, "ver-bucket")

	if getW.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getW.Code)
	}

	var config versioningConfiguration
	if err := xml.Unmarshal(getW.Body.Bytes(), &config); err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}
	if config.Status != "Enabled" {
		t.Errorf("expected Enabled, got %q", config.Status)
	}
}

func TestVersionedPutAndGet(t *testing.T) {
	gw, bs := newTestGateway()
	seedBucket(t, bs, "ver-bucket")

	vs := newVersionStore()

	// Put two versions of the same object.
	body1 := []byte("version 1 content")
	req1 := httptest.NewRequest(http.MethodPut, "/ver-bucket/myobj", bytes.NewReader(body1))
	setAuthHeaders(req1, req1.Method, req1.URL.Path)
	w1 := httptest.NewRecorder()
	gw.handlePutObjectVersioned(w1, req1, "ver-bucket", "myobj", vs)

	if w1.Code != http.StatusOK {
		t.Fatalf("put v1 failed: %d", w1.Code)
	}
	versionID1 := w1.Header().Get("x-amz-version-id")
	if versionID1 == "" {
		t.Fatal("expected version ID in response")
	}

	body2 := []byte("version 2 content")
	req2 := httptest.NewRequest(http.MethodPut, "/ver-bucket/myobj", bytes.NewReader(body2))
	setAuthHeaders(req2, req2.Method, req2.URL.Path)
	w2 := httptest.NewRecorder()
	gw.handlePutObjectVersioned(w2, req2, "ver-bucket", "myobj", vs)

	if w2.Code != http.StatusOK {
		t.Fatalf("put v2 failed: %d", w2.Code)
	}
	versionID2 := w2.Header().Get("x-amz-version-id")
	if versionID2 == "" {
		t.Fatal("expected version ID in response")
	}
	if versionID1 == versionID2 {
		t.Error("expected different version IDs for different versions")
	}

	// Get the first version by versionId.
	getReq := httptest.NewRequest(http.MethodGet, "/ver-bucket/myobj?versionId="+versionID1, nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	gw.handleGetObjectVersioned(getW, getReq, "ver-bucket", "myobj", versionID1, vs)

	if getW.Code != http.StatusOK {
		t.Fatalf("get v1 failed: %d", getW.Code)
	}
	if !bytes.Equal(getW.Body.Bytes(), body1) {
		t.Fatalf("body mismatch: got %q, want %q", getW.Body.String(), string(body1))
	}
	if getW.Header().Get("x-amz-version-id") != versionID1 {
		t.Errorf("expected version ID %s, got %s", versionID1, getW.Header().Get("x-amz-version-id"))
	}

	// Verify we have 2 versions.
	versions := vs.listVersions("ver-bucket", "myobj")
	if len(versions) != 2 {
		t.Errorf("expected 2 versions, got %d", len(versions))
	}
}
