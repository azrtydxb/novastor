//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	testAccessKey = "test-access-key"
	testSecretKey = "test-secret-key"
)

// listBucketsResult mirrors the S3 ListAllMyBucketsResult XML structure.
type listBucketsResult struct {
	XMLName xml.Name     `xml:"ListAllMyBucketsResult"`
	Buckets []bucketItem `xml:"Buckets>Bucket"`
}

type bucketItem struct {
	Name string `xml:"Name"`
}

// listObjectsV2Result mirrors the S3 ListBucketResult XML structure.
type listObjectsV2Result struct {
	XMLName  xml.Name     `xml:"ListBucketResult"`
	Contents []objectItem `xml:"Contents"`
}

type objectItem struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
}

// TestObjectStorage_BucketAndObject tests the S3 gateway end-to-end:
//  1. Start metadata + chunk services
//  2. Start S3 gateway with real adapters
//  3. Create a bucket via HTTP
//  4. Put an object
//  5. Get the object and verify content
//  6. List objects
//  7. Delete object and bucket
func TestObjectStorage_BucketAndObject(t *testing.T) {
	// Set up cluster.
	tc := newTestCluster(t)
	defer tc.Close()

	// Create S3 adapters backed by the real metadata and chunk services.
	metaAdapter := s3.NewMetadataAdapter(tc.metaClient)

	// Create a LocalChunkStore backed by the agent client.
	chunkStore := agent.NewLocalChunkStore(tc.agentClient)

	// Create the S3 Gateway with real adapters.
	gateway := s3.NewGateway(metaAdapter, metaAdapter, chunkStore, metaAdapter, testAccessKey, testSecretKey)

	// Start an httptest server with the gateway as handler.
	srv := httptest.NewServer(gateway)
	defer srv.Close()

	client := srv.Client()
	baseURL := srv.URL

	// Parse baseURL to get host for signature
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("Failed to parse base URL: %v", err)
	}
	host := parsedURL.Host

	// amzDate is the current timestamp in UTC for SigV4 signing
	amzDate := time.Now().UTC().Format("20060102T150405Z")

	// Helper to create authenticated requests with proper SigV4 signature.
	doReq := func(method, path string, body []byte) (*http.Response, []byte) {
		t.Helper()
		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(context.Background(), method, baseURL+path, bodyReader)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		// Set X-Amz-Date header required for SigV4
		req.Header.Set("X-Amz-Date", amzDate)
		req.Header.Set("Host", host)

		// Generate proper SigV4 Authorization header
		authHeader := generateSigV4Header(method, path, host, amzDate)
		req.Header.Set("Authorization", authHeader)

		if body != nil {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s failed: %v", method, path, err)
		}
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("Failed to read response body: %v", err)
		}
		return resp, respBody
	}

	// Step 1: Create a bucket.
	t.Log("Creating bucket 'test-bucket'...")
	resp, _ := doReq(http.MethodPut, "/test-bucket", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateBucket expected 200, got %d", resp.StatusCode)
	}
	t.Log("Bucket created successfully")

	// Step 2: Head bucket to verify it exists.
	resp, _ = doReq(http.MethodHead, "/test-bucket", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HeadBucket expected 200, got %d", resp.StatusCode)
	}
	t.Log("HeadBucket verified")

	// Step 3: Put an object.
	objectContent := []byte("Hello, NovaStor S3 Gateway! This is an end-to-end test object.")
	t.Log("Putting object 'test-bucket/test-key.txt'...")
	resp, _ = doReq(http.MethodPut, "/test-bucket/test-key.txt", objectContent)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PutObject expected 200, got %d", resp.StatusCode)
	}
	t.Log("Object put successfully")

	// Step 4: Get the object and verify content.
	t.Log("Getting object 'test-bucket/test-key.txt'...")
	resp, body := doReq(http.MethodGet, "/test-bucket/test-key.txt", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetObject expected 200, got %d", resp.StatusCode)
	}
	if !bytes.Equal(body, objectContent) {
		t.Errorf("Object content mismatch: expected %q, got %q", objectContent, body)
	}
	t.Logf("Object content verified: %d bytes", len(body))

	// Step 5: Head object.
	resp, _ = doReq(http.MethodHead, "/test-bucket/test-key.txt", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HeadObject expected 200, got %d", resp.StatusCode)
	}

	// Step 6: List objects in the bucket.
	t.Log("Listing objects in 'test-bucket'...")
	resp, body = doReq(http.MethodGet, "/test-bucket", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListObjects expected 200, got %d", resp.StatusCode)
	}

	var listResult listObjectsV2Result
	if err := xml.Unmarshal(body, &listResult); err != nil {
		t.Fatalf("Failed to parse ListObjects response: %v", err)
	}
	if len(listResult.Contents) != 1 {
		t.Errorf("Expected 1 object in list, got %d", len(listResult.Contents))
	} else if listResult.Contents[0].Key != "test-key.txt" {
		t.Errorf("Expected object key 'test-key.txt', got %q", listResult.Contents[0].Key)
	}
	t.Logf("ListObjects verified: %d objects", len(listResult.Contents))

	// Step 7: List buckets.
	t.Log("Listing all buckets...")
	resp, body = doReq(http.MethodGet, "/", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListBuckets expected 200, got %d", resp.StatusCode)
	}
	var bucketsResult listBucketsResult
	if err := xml.Unmarshal(body, &bucketsResult); err != nil {
		t.Fatalf("Failed to parse ListBuckets response: %v", err)
	}
	if len(bucketsResult.Buckets) != 1 {
		t.Errorf("Expected 1 bucket, got %d", len(bucketsResult.Buckets))
	}
	t.Logf("ListBuckets verified: %d buckets", len(bucketsResult.Buckets))

	// Step 8: Delete the object.
	t.Log("Deleting object 'test-bucket/test-key.txt'...")
	resp, _ = doReq(http.MethodDelete, "/test-bucket/test-key.txt", nil)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteObject expected 200 or 204, got %d", resp.StatusCode)
	}
	t.Log("Object deleted successfully")

	// Step 9: Verify object is gone.
	resp, _ = doReq(http.MethodGet, "/test-bucket/test-key.txt", nil)
	if resp.StatusCode == http.StatusOK {
		t.Error("Expected error when getting deleted object, but got 200")
	}
	t.Log("Object deletion verified")

	// Step 10: Delete the bucket.
	t.Log("Deleting bucket 'test-bucket'...")
	resp, _ = doReq(http.MethodDelete, "/test-bucket", nil)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteBucket expected 200 or 204, got %d", resp.StatusCode)
	}
	t.Log("Bucket deleted successfully")

	// Step 11: Verify bucket is gone.
	resp, _ = doReq(http.MethodHead, "/test-bucket", nil)
	if resp.StatusCode == http.StatusOK {
		t.Error("Expected error when heading deleted bucket, but got 200")
	}
	t.Log("Bucket deletion verified")
}

// TestObjectStorage_AuthDenied verifies that unauthenticated requests are rejected.
func TestObjectStorage_AuthDenied(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.Close()

	metaAdapter := s3.NewMetadataAdapter(tc.metaClient)
	agentClient, err := agent.Dial(
		tc.chunkAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial chunk server: %v", err)
	}
	defer agentClient.Close()

	chunkStore := agent.NewLocalChunkStore(agentClient)
	gateway := s3.NewGateway(metaAdapter, metaAdapter, chunkStore, metaAdapter, testAccessKey, testSecretKey)

	srv := httptest.NewServer(gateway)
	defer srv.Close()

	// Request without auth header.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("Expected 403 Forbidden for unauthenticated request, got %d", resp.StatusCode)
	}
	t.Log("Authentication denial verified")
}

// SigV4 signature generation helpers for E2E tests

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hmacSHA256Hex(key, data []byte) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

func deriveSigningKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

// generateSigV4Header generates a valid AWS SigV4 Authorization header for testing.
// This matches the signature verification logic in internal/s3/auth.go.
func generateSigV4Header(method, path, host, amzDate string) string {
	dateStamp := amzDate[:8]
	credential := fmt.Sprintf("%s/%s/us-east-1/s3/aws4_request", testAccessKey, dateStamp)
	signedHeaders := "host"

	// Build canonical request - must match buildCanonicalRequest in auth.go
	// Format: method\npath\nquery\nheaders\nsignedHeaders\npayloadHash
	canonicalRequest := method + "\n" +
		path + "\n" +
		"" + "\n" + // query string (empty)
		"host:" + host + "\n" + // canonical headers
		"\n" + // blank line after headers
		signedHeaders + "\n" +
		"UNSIGNED-PAYLOAD"

	// Build string to sign
	canonicalRequestHash := sha256Hex([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" +
		amzDate + "\n" +
		dateStamp+"/us-east-1/s3/aws4_request\n" +
		canonicalRequestHash

	// Derive signing key and compute signature
	signingKey := deriveSigningKey(testSecretKey, dateStamp, "us-east-1", "s3")
	signature := hmacSHA256Hex(signingKey, []byte(stringToSign))

	return fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s, SignedHeaders=%s, Signature=%s",
		credential, signedHeaders, signature)
}
