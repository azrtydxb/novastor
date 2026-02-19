package s3

import (
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// --- XML types for multipart upload responses ---

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

// handleCreateMultipartUpload handles POST /<bucket>/<key>?uploads.
func (g *Gateway) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()

	// Verify bucket exists.
	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	uploadID := uuid.New().String()

	info := &MultipartInfo{
		UploadID:     uploadID,
		Bucket:       bucket,
		Key:          key,
		Parts:        []PartInfo{},
		CreationDate: time.Now().Unix(),
	}

	if err := g.multiparts.PutMultipart(ctx, info); err != nil {
		writeS3Error(w, "InternalError", "failed to create multipart upload", http.StatusInternalServerError)
		return
	}

	result := initiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
	writeXMLResponse(w, http.StatusOK, &result)
}

// handleUploadPart handles PUT /<bucket>/<key>?partNumber=N&uploadId=ID.
func (g *Gateway) handleUploadPart(w http.ResponseWriter, r *http.Request, _ string, _ string) {
	ctx := r.Context()

	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")
	if uploadID == "" || partNumberStr == "" {
		writeS3Error(w, "InvalidArgument", "uploadId and partNumber are required", http.StatusBadRequest)
		return
	}

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		writeS3Error(w, "InvalidArgument", "partNumber must be a positive integer", http.StatusBadRequest)
		return
	}

	mpInfo, err := g.multiparts.GetMultipart(ctx, uploadID)
	if err != nil {
		writeS3Error(w, "NoSuchUpload", "The specified multipart upload does not exist.", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "failed to read request body", http.StatusInternalServerError)
		return
	}

	// Store part data as chunks.
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
	if len(body) == 0 {
		chunkIDs = []string{}
	}

	etag := fmt.Sprintf("%x", md5.Sum(body))

	part := PartInfo{
		PartNumber: partNumber,
		Size:       int64(len(body)),
		ETag:       etag,
		ChunkIDs:   chunkIDs,
	}

	// Replace existing part with same number or append.
	replaced := false
	for i, p := range mpInfo.Parts {
		if p.PartNumber == partNumber {
			mpInfo.Parts[i] = part
			replaced = true
			break
		}
	}
	if !replaced {
		mpInfo.Parts = append(mpInfo.Parts, part)
	}

	if err := g.multiparts.PutMultipart(ctx, mpInfo); err != nil {
		writeS3Error(w, "InternalError", "failed to update multipart upload", http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

// handleCompleteMultipartUpload handles POST /<bucket>/<key>?uploadId=ID.
func (g *Gateway) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()

	uploadID := r.URL.Query().Get("uploadId")
	if uploadID == "" {
		writeS3Error(w, "InvalidArgument", "uploadId is required", http.StatusBadRequest)
		return
	}

	mpInfo, err := g.multiparts.GetMultipart(ctx, uploadID)
	if err != nil {
		writeS3Error(w, "NoSuchUpload", "The specified multipart upload does not exist.", http.StatusNotFound)
		return
	}

	// Sort parts by part number.
	sort.Slice(mpInfo.Parts, func(i, j int) bool {
		return mpInfo.Parts[i].PartNumber < mpInfo.Parts[j].PartNumber
	})

	// Assemble all chunk IDs and calculate total size.
	var allChunkIDs []string
	var totalSize int64
	h := md5.New()
	for _, p := range mpInfo.Parts {
		allChunkIDs = append(allChunkIDs, p.ChunkIDs...)
		totalSize += p.Size
		// Include each part's ETag in the combined hash.
		fmt.Fprint(h, p.ETag)
	}

	combinedETag := fmt.Sprintf("%x-%d", h.Sum(nil), len(mpInfo.Parts))

	contentType := "application/octet-stream"

	obj := &ObjectInfo{
		Bucket:      bucket,
		Key:         key,
		Size:        totalSize,
		ETag:        combinedETag,
		ContentType: contentType,
		UserMeta:    make(map[string]string),
		ChunkIDs:    allChunkIDs,
		ModTime:     time.Now().Unix(),
	}

	if err := g.objects.PutObject(ctx, obj); err != nil {
		writeS3Error(w, "InternalError", "failed to store object", http.StatusInternalServerError)
		return
	}

	// Clean up multipart metadata.
	_ = g.multiparts.DeleteMultipart(ctx, uploadID)

	result := completeMultipartUploadResult{
		Bucket: bucket,
		Key:    key,
		ETag:   `"` + combinedETag + `"`,
	}
	writeXMLResponse(w, http.StatusOK, &result)
}

// handleAbortMultipartUpload handles DELETE /<bucket>/<key>?uploadId=ID.
func (g *Gateway) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, _, _ string) {
	ctx := r.Context()

	uploadID := r.URL.Query().Get("uploadId")
	if uploadID == "" {
		writeS3Error(w, "InvalidArgument", "uploadId is required", http.StatusBadRequest)
		return
	}

	_ = g.multiparts.DeleteMultipart(ctx, uploadID)

	w.WriteHeader(http.StatusNoContent)
}
