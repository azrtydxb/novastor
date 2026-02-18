package s3

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- ListParts Tests ---

func TestListParts_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Step 1: Create multipart upload.
	createReq := httptest.NewRequest(http.MethodPost, "/test-bucket/multipart-file.bin?uploads", nil)
	setAuthHeaders(createReq, createReq.Method, createReq.URL.Path)
	createW := httptest.NewRecorder()
	g.ServeHTTP(createW, createReq)

	var initResult initiateMultipartUploadResult
	initBody, _ := io.ReadAll(createW.Result().Body)
	if err := xml.Unmarshal(initBody, &initResult); err != nil {
		t.Fatalf("failed to parse init XML: %v", err)
	}
	uploadID := initResult.UploadID

	// Step 2: Upload multiple parts.
	for i := 1; i <= 3; i++ {
		partData := []byte(strings.Repeat("X", 100*i))
		partReq := httptest.NewRequest(http.MethodPut, "/test-bucket/multipart-file.bin?partNumber="+string(rune('0'+i))+"&uploadId="+uploadID, bytes.NewReader(partData))
		setAuthHeaders(partReq, partReq.Method, partReq.URL.Path)
		partW := httptest.NewRecorder()
		g.ServeHTTP(partW, partReq)

		if partW.Result().StatusCode != http.StatusOK {
			t.Fatalf("part %d upload failed: %d", i, partW.Result().StatusCode)
		}
	}

	// Step 3: List parts.
	listReq := httptest.NewRequest(http.MethodGet, "/test-bucket/multipart-file.bin?uploadId="+uploadID, nil)
	setAuthHeaders(listReq, listReq.Method, listReq.URL.Path)
	listW := httptest.NewRecorder()
	g.ServeHTTP(listW, listReq)

	listResp := listW.Result()
	defer listResp.Body.Close()

	if listResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(listResp.Body)
		t.Fatalf("expected 200, got %d: %s", listResp.StatusCode, string(respBody))
	}

	listBody, _ := io.ReadAll(listResp.Body)
	var listResult listPartsResult
	if err := xml.Unmarshal(listBody, &listResult); err != nil {
		t.Fatalf("failed to parse list XML: %v", err)
	}

	if listResult.Bucket != "test-bucket" {
		t.Fatalf("expected bucket test-bucket, got %s", listResult.Bucket)
	}
	if listResult.UploadID != uploadID {
		t.Fatalf("expected uploadId %s, got %s", uploadID, listResult.UploadID)
	}
	if len(listResult.Parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(listResult.Parts))
	}
	if listResult.Parts[0].PartNumber != 1 {
		t.Fatalf("expected first part number 1, got %d", listResult.Parts[0].PartNumber)
	}
}

func TestListParts_WithPagination(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Create multipart upload.
	createReq := httptest.NewRequest(http.MethodPost, "/test-bucket/paginated.bin?uploads", nil)
	setAuthHeaders(createReq, createReq.Method, createReq.URL.Path)
	createW := httptest.NewRecorder()
	g.ServeHTTP(createW, createReq)

	var initResult initiateMultipartUploadResult
	initBody, _ := io.ReadAll(createW.Result().Body)
	if err := xml.Unmarshal(initBody, &initResult); err != nil {
		t.Fatalf("failed to parse init XML: %v", err)
	}
	uploadID := initResult.UploadID

	// Upload 5 parts.
	for i := 1; i <= 5; i++ {
		partData := []byte(strings.Repeat("A", 100*i))
		partReq := httptest.NewRequest(http.MethodPut, "/test-bucket/paginated.bin?partNumber="+string(rune('0'+i))+"&uploadId="+uploadID, bytes.NewReader(partData))
		setAuthHeaders(partReq, partReq.Method, partReq.URL.Path)
		partW := httptest.NewRecorder()
		g.ServeHTTP(partW, partReq)

		if partW.Result().StatusCode != http.StatusOK {
			t.Fatalf("part %d upload failed: %d", i, partW.Result().StatusCode)
		}
	}

	// List with max-parts=2 starting from part 2.
	listReq := httptest.NewRequest(http.MethodGet, "/test-bucket/paginated.bin?uploadId="+uploadID+"&part-number-marker=1&max-parts=2", nil)
	setAuthHeaders(listReq, listReq.Method, listReq.URL.Path)
	listW := httptest.NewRecorder()
	g.ServeHTTP(listW, listReq)

	listBody, _ := io.ReadAll(listW.Result().Body)
	var listResult listPartsResult
	if err := xml.Unmarshal(listBody, &listResult); err != nil {
		t.Fatalf("failed to parse list XML: %v", err)
	}

	if listResult.PartNumberMarker != 1 {
		t.Fatalf("expected part-number-marker 1, got %d", listResult.PartNumberMarker)
	}
	if !listResult.IsTruncated {
		t.Fatal("expected IsTruncated to be true")
	}
	if len(listResult.Parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(listResult.Parts))
	}
	if listResult.Parts[0].PartNumber != 2 {
		t.Fatalf("expected first part number 2, got %d", listResult.Parts[0].PartNumber)
	}
}

func TestListParts_NoSuchUpload(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	listReq := httptest.NewRequest(http.MethodGet, "/test-bucket/file.bin?uploadId=nonexistent-upload-id", nil)
	setAuthHeaders(listReq, listReq.Method, listReq.URL.Path)
	listW := httptest.NewRecorder()
	g.ServeHTTP(listW, listReq)

	if listW.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", listW.Result().StatusCode)
	}

	if !strings.Contains(listW.Body.String(), "NoSuchUpload") {
		t.Fatalf("expected NoSuchUpload error, got: %s", listW.Body.String())
	}
}

// --- CopyObject Tests ---

func TestCopyObject_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "src-bucket")
	seedBucket(t, bs, "dst-bucket")

	// Create source object.
	srcData := []byte("source object data")
	putReq := httptest.NewRequest(http.MethodPut, "/src-bucket/src-key", bytes.NewReader(srcData))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusOK {
		t.Fatalf("source object creation failed: %d", putW.Result().StatusCode)
	}

	// Copy object.
	copyReq := httptest.NewRequest(http.MethodPut, "/dst-bucket/dst-key", nil)
	copyReq.Header.Set("X-Amz-Copy-Source", "/src-bucket/src-key")
	setAuthHeaders(copyReq, copyReq.Method, copyReq.URL.Path)
	copyW := httptest.NewRecorder()
	g.ServeHTTP(copyW, copyReq)

	copyResp := copyW.Result()
	defer copyResp.Body.Close()

	if copyResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(copyResp.Body)
		t.Fatalf("expected 200, got %d: %s", copyResp.StatusCode, string(respBody))
	}

	copyBody, _ := io.ReadAll(copyResp.Body)
	var copyResult copyObjectResult
	if err := xml.Unmarshal(copyBody, &copyResult); err != nil {
		t.Fatalf("failed to parse copy XML: %v", err)
	}

	if copyResult.ETag == "" {
		t.Fatal("expected non-empty ETag in copy result")
	}

	// Verify the copied object exists.
	getReq := httptest.NewRequest(http.MethodGet, "/dst-bucket/dst-key", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusOK {
		t.Fatalf("get after copy failed: %d", getW.Result().StatusCode)
	}

	gotBody, _ := io.ReadAll(getW.Result().Body)
	if !bytes.Equal(gotBody, srcData) {
		t.Fatalf("body mismatch after copy:\ngot:  %q\nwant: %q", gotBody, srcData)
	}
}

func TestCopyObject_SameBucket(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Create source object.
	srcData := []byte("same bucket data")
	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/src-key", bytes.NewReader(srcData))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusOK {
		t.Fatalf("source object creation failed: %d", putW.Result().StatusCode)
	}

	// Copy within same bucket.
	copyReq := httptest.NewRequest(http.MethodPut, "/test-bucket/dst-key", nil)
	copyReq.Header.Set("X-Amz-Copy-Source", "/test-bucket/src-key")
	setAuthHeaders(copyReq, copyReq.Method, copyReq.URL.Path)
	copyW := httptest.NewRecorder()
	g.ServeHTTP(copyW, copyReq)

	if copyW.Result().StatusCode != http.StatusOK {
		t.Fatalf("copy failed: %d", copyW.Result().StatusCode)
	}

	// Verify both objects exist.
	getSrcReq := httptest.NewRequest(http.MethodGet, "/test-bucket/src-key", nil)
	setAuthHeaders(getSrcReq, getSrcReq.Method, getSrcReq.URL.Path)
	getSrcW := httptest.NewRecorder()
	g.ServeHTTP(getSrcW, getSrcReq)

	getDstReq := httptest.NewRequest(http.MethodGet, "/test-bucket/dst-key", nil)
	setAuthHeaders(getDstReq, getDstReq.Method, getDstReq.URL.Path)
	getDstW := httptest.NewRecorder()
	g.ServeHTTP(getDstW, getDstReq)

	srcBody, _ := io.ReadAll(getSrcW.Result().Body)
	dstBody, _ := io.ReadAll(getDstW.Result().Body)

	if !bytes.Equal(srcBody, srcData) || !bytes.Equal(dstBody, srcData) {
		t.Fatal("source or destination data mismatch after same-bucket copy")
	}
}

func TestCopyObject_NonExistentSource(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	copyReq := httptest.NewRequest(http.MethodPut, "/test-bucket/dst-key", nil)
	copyReq.Header.Set("X-Amz-Copy-Source", "/test-bucket/nonexistent-src")
	setAuthHeaders(copyReq, copyReq.Method, copyReq.URL.Path)
	copyW := httptest.NewRecorder()
	g.ServeHTTP(copyW, copyReq)

	if copyW.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", copyW.Result().StatusCode)
	}

	if !strings.Contains(copyW.Body.String(), "NoSuchKey") {
		t.Fatalf("expected NoSuchKey error, got: %s", copyW.Body.String())
	}
}

func TestCopyObject_MissingCopySourceHeader(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// A PUT without X-Amz-Copy-Source is treated as a regular PutObject.
	// This creates an empty object.
	copyReq := httptest.NewRequest(http.MethodPut, "/test-bucket/dst-key", nil)
	setAuthHeaders(copyReq, copyReq.Method, copyReq.URL.Path)
	copyW := httptest.NewRecorder()
	g.ServeHTTP(copyW, copyReq)

	// Should succeed as regular PutObject (empty object created)
	if copyW.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", copyW.Result().StatusCode)
	}

	// Verify the empty object exists
	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket/dst-key", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getW.Result().StatusCode)
	}
}

// --- Object Tagging Tests ---

func TestPutObjectTagging_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Create an object.
	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/tagged-obj", bytes.NewReader([]byte("data")))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusOK {
		t.Fatalf("object creation failed: %d", putW.Result().StatusCode)
	}

	// Add tags.
	tagXML := `<Tagging><TagSet><Tag><Key>env</Key><Value>production</Value></Tag><Tag><Key>team</Key><Value>storage</Value></Tag></TagSet></Tagging>`
	tagReq := httptest.NewRequest(http.MethodPut, "/test-bucket/tagged-obj?tagging", strings.NewReader(tagXML))
	setAuthHeaders(tagReq, tagReq.Method, tagReq.URL.Path)
	tagW := httptest.NewRecorder()
	g.ServeHTTP(tagW, tagReq)

	if tagW.Result().StatusCode != http.StatusOK {
		t.Fatalf("put tagging failed: %d", tagW.Result().StatusCode)
	}

	// Get tags.
	getTagReq := httptest.NewRequest(http.MethodGet, "/test-bucket/tagged-obj?tagging", nil)
	setAuthHeaders(getTagReq, getTagReq.Method, getTagReq.URL.Path)
	getTagW := httptest.NewRecorder()
	g.ServeHTTP(getTagW, getTagReq)

	if getTagW.Result().StatusCode != http.StatusOK {
		t.Fatalf("get tagging failed: %d", getTagW.Result().StatusCode)
	}

	tagBody, _ := io.ReadAll(getTagW.Result().Body)
	var result tagging
	if err := xml.Unmarshal(tagBody, &result); err != nil {
		t.Fatalf("failed to parse tagging XML: %v", err)
	}

	if len(result.TagSet.Tag) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(result.TagSet.Tag))
	}
}

func TestGetObjectTagging_NoTags(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Create an object without tags.
	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/no-tags", bytes.NewReader([]byte("data")))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	// Get tags (should be empty).
	getTagReq := httptest.NewRequest(http.MethodGet, "/test-bucket/no-tags?tagging", nil)
	setAuthHeaders(getTagReq, getTagReq.Method, getTagReq.URL.Path)
	getTagW := httptest.NewRecorder()
	g.ServeHTTP(getTagW, getTagReq)

	if getTagW.Result().StatusCode != http.StatusOK {
		t.Fatalf("get tagging failed: %d", getTagW.Result().StatusCode)
	}

	tagBody, _ := io.ReadAll(getTagW.Result().Body)
	var result tagging
	if err := xml.Unmarshal(tagBody, &result); err != nil {
		t.Fatalf("failed to parse tagging XML: %v", err)
	}

	if len(result.TagSet.Tag) != 0 {
		t.Fatalf("expected 0 tags, got %d", len(result.TagSet.Tag))
	}
}

func TestGetObjectTagging_NonExistentObject(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	getTagReq := httptest.NewRequest(http.MethodGet, "/test-bucket/nonexistent?tagging", nil)
	setAuthHeaders(getTagReq, getTagReq.Method, getTagReq.URL.Path)
	getTagW := httptest.NewRecorder()
	g.ServeHTTP(getTagW, getTagReq)

	if getTagW.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", getTagW.Result().StatusCode)
	}
}

func TestDeleteObjectTagging_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	// Create an object with tags.
	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/tagged", bytes.NewReader([]byte("data")))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	tagXML := `<Tagging><TagSet><Tag><Key>temp</Key><Value>true</Value></Tag></TagSet></Tagging>`
	tagReq := httptest.NewRequest(http.MethodPut, "/test-bucket/tagged?tagging", strings.NewReader(tagXML))
	setAuthHeaders(tagReq, tagReq.Method, tagReq.URL.Path)
	tagW := httptest.NewRecorder()
	g.ServeHTTP(tagW, tagReq)

	// Delete tags.
	delTagReq := httptest.NewRequest(http.MethodDelete, "/test-bucket/tagged?tagging", nil)
	setAuthHeaders(delTagReq, delTagReq.Method, delTagReq.URL.Path)
	delTagW := httptest.NewRecorder()
	g.ServeHTTP(delTagW, delTagReq)

	if delTagW.Result().StatusCode != http.StatusNoContent {
		t.Fatalf("delete tagging failed: %d", delTagW.Result().StatusCode)
	}

	// Verify tags are gone.
	getTagReq := httptest.NewRequest(http.MethodGet, "/test-bucket/tagged?tagging", nil)
	setAuthHeaders(getTagReq, getTagReq.Method, getTagReq.URL.Path)
	getTagW := httptest.NewRecorder()
	g.ServeHTTP(getTagW, getTagReq)

	tagBody, _ := io.ReadAll(getTagW.Result().Body)
	var result tagging
	if err := xml.Unmarshal(tagBody, &result); err != nil {
		t.Fatalf("failed to parse tagging XML: %v", err)
	}

	if len(result.TagSet.Tag) != 0 {
		t.Fatalf("expected 0 tags after delete, got %d", len(result.TagSet.Tag))
	}
}

func TestPutObjectTagging_InvalidEmptyKey(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/obj", bytes.NewReader([]byte("data")))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	// Try to add a tag with empty key.
	tagXML := `<Tagging><TagSet><Tag><Key></Key><Value>value</Value></Tag></TagSet></Tagging>`
	tagReq := httptest.NewRequest(http.MethodPut, "/test-bucket/obj?tagging", strings.NewReader(tagXML))
	setAuthHeaders(tagReq, tagReq.Method, tagReq.URL.Path)
	tagW := httptest.NewRecorder()
	g.ServeHTTP(tagW, tagReq)

	if tagW.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty tag key, got %d", tagW.Result().StatusCode)
	}

	if !strings.Contains(tagW.Body.String(), "InvalidTag") {
		t.Fatalf("expected InvalidTag error, got: %s", tagW.Body.String())
	}
}

// --- Bucket Lifecycle Tests ---

func TestPutBucketLifecycle_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	lifecycleXML := `<LifecycleConfiguration>
		<Rule>
			<ID>delete-old-versions</ID>
			<Filter>
				<Prefix>logs/</Prefix>
			</Filter>
			<Status>Enabled</Status>
			<Expiration>
				<Days>30</Days>
			</Expiration>
		</Rule>
	</LifecycleConfiguration>`

	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket?lifecycle", strings.NewReader(lifecycleXML))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusOK {
		t.Fatalf("put lifecycle failed: %d", putW.Result().StatusCode)
	}

	// Get lifecycle configuration.
	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket?lifecycle", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusOK {
		t.Fatalf("get lifecycle failed: %d", getW.Result().StatusCode)
	}

	body, _ := io.ReadAll(getW.Result().Body)
	var result lifecycleConfiguration
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse lifecycle XML: %v", err)
	}

	if len(result.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(result.Rules))
	}
	if result.Rules[0].ID != "delete-old-versions" {
		t.Fatalf("expected rule ID 'delete-old-versions', got %s", result.Rules[0].ID)
	}
}

func TestGetBucketLifecycle_NotFound(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "no-lifecycle-bucket")

	getReq := httptest.NewRequest(http.MethodGet, "/no-lifecycle-bucket?lifecycle", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", getW.Result().StatusCode)
	}

	if !strings.Contains(getW.Body.String(), "NoSuchLifecycleConfiguration") {
		t.Fatalf("expected NoSuchLifecycleConfiguration error, got: %s", getW.Body.String())
	}
}

func TestDeleteBucketLifecycle_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	lifecycleXML := `<LifecycleConfiguration>
		<Rule>
			<ID>test-rule</ID>
			<Status>Enabled</Status>
			<Expiration><Days>1</Days></Expiration>
		</Rule>
	</LifecycleConfiguration>`

	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket?lifecycle", strings.NewReader(lifecycleXML))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	// Delete lifecycle.
	delReq := httptest.NewRequest(http.MethodDelete, "/test-bucket?lifecycle", nil)
	setAuthHeaders(delReq, delReq.Method, delReq.URL.Path)
	delW := httptest.NewRecorder()
	g.ServeHTTP(delW, delReq)

	if delW.Result().StatusCode != http.StatusNoContent {
		t.Fatalf("delete lifecycle failed: %d", delW.Result().StatusCode)
	}

	// Verify it's gone.
	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket?lifecycle", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getW.Result().StatusCode)
	}
}

// --- Bucket Encryption Tests ---

func TestPutBucketEncryption_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	encryptionXML := `<ServerSideEncryptionConfiguration>
		<Rule>
			<ApplyServerSideEncryptionByDefault>
				<SSEAlgorithm>AES256</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
		</Rule>
	</ServerSideEncryptionConfiguration>`

	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket?encryption", strings.NewReader(encryptionXML))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusOK {
		t.Fatalf("put encryption failed: %d", putW.Result().StatusCode)
	}

	// Get encryption configuration.
	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket?encryption", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusOK {
		t.Fatalf("get encryption failed: %d", getW.Result().StatusCode)
	}

	body, _ := io.ReadAll(getW.Result().Body)
	var result serverSideEncryptionConfiguration
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse encryption XML: %v", err)
	}

	if result.Rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm != "AES256" {
		t.Fatalf("expected SSEAlgorithm AES256, got %s", result.Rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm)
	}
}

func TestPutBucketEncryption_KMS(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	encryptionXML := `<ServerSideEncryptionConfiguration>
		<Rule>
			<ApplyServerSideEncryptionByDefault>
				<SSEAlgorithm>aws:kms</SSEAlgorithm>
				<KMSMasterKeyID>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</KMSMasterKeyID>
			</ApplyServerSideEncryptionByDefault>
		</Rule>
	</ServerSideEncryptionConfiguration>`

	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket?encryption", strings.NewReader(encryptionXML))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusOK {
		t.Fatalf("put encryption with KMS failed: %d", putW.Result().StatusCode)
	}

	// Get and verify.
	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket?encryption", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	body, _ := io.ReadAll(getW.Result().Body)
	var result serverSideEncryptionConfiguration
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse encryption XML: %v", err)
	}

	if result.Rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm != "aws:kms" {
		t.Fatalf("expected SSEAlgorithm aws:kms, got %s", result.Rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm)
	}
}

func TestPutBucketEncryption_InvalidAlgorithm(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	encryptionXML := `<ServerSideEncryptionConfiguration>
		<Rule>
			<ApplyServerSideEncryptionByDefault>
				<SSEAlgorithm>INVALID</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
		</Rule>
	</ServerSideEncryptionConfiguration>`

	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket?encryption", strings.NewReader(encryptionXML))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	if putW.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid algorithm, got %d", putW.Result().StatusCode)
	}
}

func TestGetBucketEncryption_NotFound(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "no-encryption-bucket")

	getReq := httptest.NewRequest(http.MethodGet, "/no-encryption-bucket?encryption", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", getW.Result().StatusCode)
	}

	if !strings.Contains(getW.Body.String(), "ServerSideEncryptionConfigurationNotFoundError") {
		t.Fatalf("expected ServerSideEncryptionConfigurationNotFoundError, got: %s", getW.Body.String())
	}
}

func TestDeleteBucketEncryption_Success(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	encryptionXML := `<ServerSideEncryptionConfiguration>
		<Rule>
			<ApplyServerSideEncryptionByDefault>
				<SSEAlgorithm>AES256</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
		</Rule>
	</ServerSideEncryptionConfiguration>`

	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket?encryption", strings.NewReader(encryptionXML))
	setAuthHeaders(putReq, putReq.Method, putReq.URL.Path)
	putW := httptest.NewRecorder()
	g.ServeHTTP(putW, putReq)

	// Delete encryption.
	delReq := httptest.NewRequest(http.MethodDelete, "/test-bucket?encryption", nil)
	setAuthHeaders(delReq, delReq.Method, delReq.URL.Path)
	delW := httptest.NewRecorder()
	g.ServeHTTP(delW, delReq)

	if delW.Result().StatusCode != http.StatusNoContent {
		t.Fatalf("delete encryption failed: %d", delW.Result().StatusCode)
	}

	// Verify it's gone.
	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket?encryption", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getW.Result().StatusCode)
	}
}

// --- List Object Versions Tests ---

func TestListObjectVersions_NoVersioning(t *testing.T) {
	g, bs := newTestGateway()
	seedBucket(t, bs, "test-bucket")

	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket?versions", nil)
	setAuthHeaders(getReq, getReq.Method, getReq.URL.Path)
	getW := httptest.NewRecorder()
	g.ServeHTTP(getW, getReq)

	if getW.Result().StatusCode != http.StatusOK {
		t.Fatalf("get versions failed: %d", getW.Result().StatusCode)
	}

	body, _ := io.ReadAll(getW.Result().Body)
	var result listObjectVersionsResult
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse versions XML: %v", err)
	}

	if result.Name != "test-bucket" {
		t.Fatalf("expected bucket name test-bucket, got %s", result.Name)
	}
}
