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
	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "failed to read request body", http.StatusInternalServerError)
		return
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
		Size:        int64(len(body)),
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

	// S3 delete is idempotent: succeed even if the object does not exist.
	_ = g.objects.DeleteObject(ctx, bucket, key)

	w.WriteHeader(http.StatusNoContent)
}
