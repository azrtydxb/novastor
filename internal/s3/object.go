package s3

import (
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/piwi3910/novastor/internal/metrics"
)

const chunkSize = 4 * 1024 * 1024 // 4 MB

// handlePutObject handles PUT /<bucket>/<key>.
func (g *Gateway) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	start := time.Now()
	defer func() {
		metrics.S3RequestsTotal.WithLabelValues("PutObject").Inc()
		metrics.S3RequestDuration.WithLabelValues("PutObject").Observe(time.Since(start).Seconds())
	}()

	ctx := r.Context()

	// Verify bucket exists.
	bucketInfo, err := g.buckets.GetBucket(ctx, bucket)
	if err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "failed to read request body", http.StatusInternalServerError)
		return
	}

	objectSize := int64(len(body))

	// Check bucket-level quota if configured.
	if g.quota != nil && bucketInfo.MaxSize > 0 {
		// Get current bucket usage.
		currentUsage, err := g.quota.GetBucketUsage(ctx, bucket)
		if err != nil {
			writeS3Error(w, "InternalError", "failed to check bucket quota", http.StatusInternalServerError)
			return
		}

		// Check if adding this object would exceed the bucket quota.
		if currentUsage+objectSize > bucketInfo.MaxSize {
			writeS3Error(w, "QuotaExceeded",
				fmt.Sprintf("Bucket quota exceeded: current usage %d bytes + object size %d bytes would exceed limit %d bytes",
					currentUsage, objectSize, bucketInfo.MaxSize),
				http.StatusForbidden)
			return
		}
	}

	metrics.S3BytesIn.Add(float64(len(body)))

	// Compute ETag as MD5 hex of the entire body.
	etag := fmt.Sprintf("%x", md5.Sum(body))

	// Split body into chunks and store each one.
	var chunkIDs []string
	for offset := 0; offset < len(body); offset += chunkSize {
		end := offset + chunkSize
		if end > len(body) {
			end = len(body)
		}
		chunkID, err := g.chunks.PutChunkData(ctx, body[offset:end])
		if err != nil {
			writeS3Error(w, "InternalError", "failed to store chunk data", http.StatusInternalServerError)
			return
		}
		chunkIDs = append(chunkIDs, chunkID)
	}

	// Handle zero-length objects (no chunks produced by the loop).
	if len(body) == 0 {
		chunkIDs = []string{}
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	obj := &ObjectInfo{
		Bucket:      bucket,
		Key:         key,
		Size:        objectSize,
		ETag:        etag,
		ContentType: contentType,
		UserMeta:    make(map[string]string),
		ChunkIDs:    chunkIDs,
		ModTime:     time.Now().Unix(),
	}

	if err := g.objects.PutObject(ctx, obj); err != nil {
		writeS3Error(w, "InternalError", "failed to store object metadata", http.StatusInternalServerError)
		return
	}

	// Reserve quota for the object after successful creation.
	if g.quota != nil {
		if err := g.quota.ReserveStorage(ctx, "bucket:"+bucket, objectSize); err != nil {
			// Log but don't fail - the object was created successfully.
			// This may cause quota tracking to be slightly off, but prevents
			// leaving the object in an inconsistent state.
		}
	}

	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

// handleGetObject handles GET /<bucket>/<key>.
func (g *Gateway) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	start := time.Now()
	defer func() {
		metrics.S3RequestsTotal.WithLabelValues("GetObject").Inc()
		metrics.S3RequestDuration.WithLabelValues("GetObject").Observe(time.Since(start).Seconds())
	}()

	ctx := r.Context()

	obj, err := g.objects.GetObject(ctx, bucket, key)
	if err != nil {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.Header().Set("Last-Modified", time.Unix(obj.ModTime, 0).UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)

	bytesWritten := int64(0)
	for _, cid := range obj.ChunkIDs {
		data, err := g.chunks.GetChunkData(ctx, cid)
		if err != nil {
			// Headers already sent; nothing we can do but stop writing.
			return
		}
		n, err := w.Write(data)
		bytesWritten += int64(n)
		if err != nil {
			return
		}
	}
	metrics.S3BytesOut.Add(float64(bytesWritten))
}

// handleHeadObject handles HEAD /<bucket>/<key>.
func (g *Gateway) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	start := time.Now()
	defer func() {
		metrics.S3RequestsTotal.WithLabelValues("HeadObject").Inc()
		metrics.S3RequestDuration.WithLabelValues("HeadObject").Observe(time.Since(start).Seconds())
	}()

	ctx := r.Context()

	obj, err := g.objects.GetObject(ctx, bucket, key)
	if err != nil {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.Header().Set("Last-Modified", time.Unix(obj.ModTime, 0).UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

// handleDeleteObject handles DELETE /<bucket>/<key>.
func (g *Gateway) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	start := time.Now()
	defer func() {
		metrics.S3RequestsTotal.WithLabelValues("DeleteObject").Inc()
		metrics.S3RequestDuration.WithLabelValues("DeleteObject").Observe(time.Since(start).Seconds())
	}()

	ctx := r.Context()

	// Get the object size before deletion for quota release.
	var objectSize int64
	obj, err := g.objects.GetObject(ctx, bucket, key)
	if err == nil {
		objectSize = obj.Size
	}

	// S3 delete is idempotent: succeed even if the object does not exist.
	_ = g.objects.DeleteObject(ctx, bucket, key)

	// Release quota for the deleted object.
	if g.quota != nil && objectSize > 0 {
		if err := g.quota.ReleaseStorage(ctx, "bucket:"+bucket, objectSize); err != nil {
			// Log but don't fail - the object was deleted successfully.
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
