package s3

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateMultipartUpload_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	req := httptest.NewRequest(http.MethodPost, "/test-bucket/bigfile.bin?uploads", nil)
	setAuthHeaders(req, req.Method, req.URL.Path)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	var result initiateMultipartUploadResult
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	if result.Bucket != "test-bucket" {
		t.Fatalf("expected bucket test-bucket, got %s", result.Bucket)
	}
	if result.Key != "bigfile.bin" {
		t.Fatalf("expected key bigfile.bin, got %s", result.Key)
	}
	if result.UploadID == "" {
		t.Fatal("expected non-empty UploadId")
	}
}

func TestUploadPart_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Create multipart upload.
	createReq := httptest.NewRequest(http.MethodPost, "/test-bucket/parts.bin?uploads", nil)
	setAuthHeaders(createReq, createReq.Method, createReq.URL.Path)
	createW := httptest.NewRecorder()
	g.ServeHTTP(createW, createReq)

	var initResult initiateMultipartUploadResult
	body, _ := io.ReadAll(createW.Result().Body)
	if err := xml.Unmarshal(body, &initResult); err != nil {
		t.Fatalf("failed to parse init XML: %v", err)
	}
	uploadID := initResult.UploadID

	// Upload part 1.
	partData := []byte("part one data here")
	partReq := httptest.NewRequest(http.MethodPut, "/test-bucket/parts.bin?partNumber=1&uploadId="+uploadID, bytes.NewReader(partData))
	setAuthHeaders(partReq, partReq.Method, partReq.URL.Path)
	partW := httptest.NewRecorder()
	g.ServeHTTP(partW, partReq)

	partResp := partW.Result()
	defer partResp.Body.Close()

	if partResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", partResp.StatusCode)
	}

	if partResp.Header.Get("ETag") == "" {
		t.Fatal("expected ETag header on part upload")
	}
}

func TestCompleteMultipartUpload_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Step 1: Create multipart upload.
	createReq := httptest.NewRequest(http.MethodPost, "/test-bucket/assembled.bin?uploads", nil)
	setAuthHeaders(createReq, createReq.Method, createReq.URL.Path)
	createW := httptest.NewRecorder()
	g.ServeHTTP(createW, createReq)

	var initResult initiateMultipartUploadResult
	initBody, _ := io.ReadAll(createW.Result().Body)
	if err := xml.Unmarshal(initBody, &initResult); err != nil {
		t.Fatalf("failed to parse init XML: %v", err)
	}
	uploadID := initResult.UploadID

	// Step 2: Upload part 1.
	part1Data := []byte("aaaa-part-one-data")
	part1Req := httptest.NewRequest(http.MethodPut, "/test-bucket/assembled.bin?partNumber=1&uploadId="+uploadID, bytes.NewReader(part1Data))
	setAuthHeaders(part1Req, part1Req.Method, part1Req.URL.Path)
	part1W := httptest.NewRecorder()
	g.ServeHTTP(part1W, part1Req)

	if part1W.Result().StatusCode != http.StatusOK {
		t.Fatalf("part 1 upload failed: %d", part1W.Result().StatusCode)
	}

	// Step 3: Upload part 2.
	part2Data := []byte("bbbb-part-two-data")
	part2Req := httptest.NewRequest(http.MethodPut, "/test-bucket/assembled.bin?partNumber=2&uploadId="+uploadID, bytes.NewReader(part2Data))
	setAuthHeaders(part2Req, part2Req.Method, part2Req.URL.Path)
	part2W := httptest.NewRecorder()
	g.ServeHTTP(part2W, part2Req)

	if part2W.Result().StatusCode != http.StatusOK {
		t.Fatalf("part 2 upload failed: %d", part2W.Result().StatusCode)
	}

	// Step 4: Complete multipart upload.
	completeReq := httptest.NewRequest(http.MethodPost, "/test-bucket/assembled.bin?uploadId="+uploadID, nil)
	setAuthHeaders(completeReq, completeReq.Method, completeReq.URL.Path)
	completeW := httptest.NewRecorder()
	g.ServeHTTP(completeW, completeReq)

	completeResp := completeW.Result()
	defer completeResp.Body.Close()

	if completeResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(completeResp.Body)
		t.Fatalf("expected 200, got %d: %s", completeResp.StatusCode, string(respBody))
	}

	completeBody, _ := io.ReadAll(completeResp.Body)
	var completeResult completeMultipartUploadResult
	if err := xml.Unmarshal(completeBody, &completeResult); err != nil {
		t.Fatalf("failed to parse complete XML: %v", err)
	}

	if completeResult.Bucket != "test-bucket" {
		t.Fatalf("expected bucket test-bucket, got %s", completeResult.Bucket)
	}
	if completeResult.Key != "assembled.bin" {
		t.Fatalf("expected key assembled.bin, got %s", completeResult.Key)
	}

	// Verify the object exists and contains both parts' data.
	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket/assembled.bin", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	getResp := getW.Result()
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get after complete: expected 200, got %d", getResp.StatusCode)
	}

	gotBody, _ := io.ReadAll(getResp.Body)
	expected := append(part1Data, part2Data...)
	if !bytes.Equal(gotBody, expected) {
		t.Fatalf("body mismatch after multipart complete:\ngot:  %q\nwant: %q", gotBody, expected)
	}
}

func TestAbortMultipartUpload_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Create multipart upload.
	createReq := httptest.NewRequest(http.MethodPost, "/test-bucket/aborted.bin?uploads", nil)
	setAuthHeaders(createReq, createReq.Method, createReq.URL.Path)
	createW := httptest.NewRecorder()
	g.ServeHTTP(createW, createReq)

	var initResult initiateMultipartUploadResult
	initBody, _ := io.ReadAll(createW.Result().Body)
	if err := xml.Unmarshal(initBody, &initResult); err != nil {
		t.Fatalf("failed to parse init XML: %v", err)
	}
	uploadID := initResult.UploadID

	// Abort the upload.
	abortReq := httptest.NewRequest(http.MethodDelete, "/test-bucket/aborted.bin?uploadId="+uploadID, nil)
	setAuthHeaders(abortReq, abortReq.Method, abortReq.URL.Path)
	abortW := httptest.NewRecorder()
	g.ServeHTTP(abortW, abortReq)

	if abortW.Result().StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", abortW.Result().StatusCode)
	}
}
