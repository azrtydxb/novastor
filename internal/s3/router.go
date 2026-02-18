package s3

import (
	"net/http"
	"strings"
)

// Gateway is the S3-compatible HTTP gateway.
type Gateway struct {
	buckets    BucketStore
	objects    ObjectStore
	chunks     ChunkStore
	multiparts MultipartStore
	accessKey  string
	secretKey  string
}

// NewGateway creates a new S3 gateway with the provided stores and credentials.
func NewGateway(buckets BucketStore, objects ObjectStore, chunks ChunkStore, multiparts MultipartStore, accessKey, secretKey string) *Gateway {
	return &Gateway{
		buckets:    buckets,
		objects:    objects,
		chunks:     chunks,
		multiparts: multiparts,
		accessKey:  accessKey,
		secretKey:  secretKey,
	}
}

// ServeHTTP dispatches incoming requests to the appropriate S3 handler.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse path-style bucket and key from the URL path.
	bucket, key := parsePath(r.URL.Path)

	// Check for presigned URL access (query-string authentication).
	if r.URL.Query().Has("X-Amz-Algorithm") && bucket != "" && key != "" {
		g.handlePresignedURL(w, r, bucket, key)
		return
	}

	// Authenticate every request.
	if err := g.authenticate(r); err != nil {
		writeS3Error(w, "AccessDenied", err.Error(), http.StatusForbidden)
		return
	}

	switch {
	// Service-level operations (no bucket).
	case bucket == "" && r.Method == http.MethodGet:
		g.handleListBuckets(w, r)
		return

	// Bucket-level operations (bucket but no key).
	case bucket != "" && key == "":
		g.dispatchBucketOp(w, r, bucket)
		return

	// Object-level operations (bucket + key).
	case bucket != "" && key != "":
		g.dispatchObjectOp(w, r, bucket, key)
		return

	default:
		writeS3Error(w, "InvalidRequest", "Could not determine operation", http.StatusBadRequest)
	}
}

// dispatchBucketOp routes requests that target a bucket (no object key).
func (g *Gateway) dispatchBucketOp(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodPut:
		g.handleCreateBucket(w, r, bucket)

	case http.MethodDelete:
		g.handleDeleteBucket(w, r, bucket)

	case http.MethodHead:
		g.handleHeadBucket(w, r, bucket)

	case http.MethodGet:
		g.handleListObjectsV2(w, r, bucket)

	default:
		writeS3Error(w, "MethodNotAllowed", "The specified method is not allowed against this resource", http.StatusMethodNotAllowed)
	}
}

// dispatchObjectOp routes requests that target an object (bucket + key).
func (g *Gateway) dispatchObjectOp(w http.ResponseWriter, r *http.Request, bucket, key string) {
	query := r.URL.Query()

	switch r.Method {
	case http.MethodPut:
		if query.Has("partNumber") && query.Has("uploadId") {
			g.handleUploadPart(w, r, bucket, key)
		} else {
			g.handlePutObject(w, r, bucket, key)
		}

	case http.MethodGet:
		g.handleGetObject(w, r, bucket, key)

	case http.MethodHead:
		g.handleHeadObject(w, r, bucket, key)

	case http.MethodDelete:
		if query.Has("uploadId") {
			g.handleAbortMultipartUpload(w, r, bucket, key)
		} else {
			g.handleDeleteObject(w, r, bucket, key)
		}

	case http.MethodPost:
		if query.Has("uploads") {
			g.handleCreateMultipartUpload(w, r, bucket, key)
		} else if query.Has("uploadId") {
			g.handleCompleteMultipartUpload(w, r, bucket, key)
		} else {
			writeS3Error(w, "InvalidRequest", "Unsupported POST operation", http.StatusBadRequest)
		}

	default:
		writeS3Error(w, "MethodNotAllowed", "The specified method is not allowed against this resource", http.StatusMethodNotAllowed)
	}
}

// parsePath extracts the bucket name and object key from a path-style URL.
// The path is expected to be "/<bucket>/<key...>" or "/<bucket>" or "/".
func parsePath(urlPath string) (bucket, key string) {
	p := strings.TrimPrefix(urlPath, "/")
	if p == "" {
		return "", ""
	}

	slash := strings.IndexByte(p, '/')
	if slash < 0 {
		return p, ""
	}

	return p[:slash], p[slash+1:]
}
