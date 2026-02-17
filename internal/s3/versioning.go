package s3

import (
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// versionedObject stores a single version of an object.
type versionedObject struct {
	VersionID string
	Info      *ObjectInfo
}

// versionStore manages object versions in memory.
type versionStore struct {
	mu       sync.RWMutex
	versions map[string][]versionedObject // keyed by "bucket/key"
}

func newVersionStore() *versionStore {
	return &versionStore{
		versions: make(map[string][]versionedObject),
	}
}

// addVersion adds a new version for the given bucket/key.
func (vs *versionStore) addVersion(bucket, key string, info *ObjectInfo) string {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	versionID := uuid.New().String()
	info.VersionID = versionID

	compositeKey := bucket + "/" + key
	vs.versions[compositeKey] = append(vs.versions[compositeKey], versionedObject{
		VersionID: versionID,
		Info:      info,
	})
	return versionID
}

// getVersion retrieves a specific version. If versionID is empty, returns the latest.
func (vs *versionStore) getVersion(bucket, key, versionID string) (*ObjectInfo, bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	compositeKey := bucket + "/" + key
	versions, ok := vs.versions[compositeKey]
	if !ok || len(versions) == 0 {
		return nil, false
	}

	if versionID == "" {
		// Return the latest version.
		return versions[len(versions)-1].Info, true
	}

	for _, v := range versions {
		if v.VersionID == versionID {
			return v.Info, true
		}
	}
	return nil, false
}

// listVersions returns all versions for the given bucket/key.
func (vs *versionStore) listVersions(bucket, key string) []versionedObject {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	compositeKey := bucket + "/" + key
	versions := vs.versions[compositeKey]
	result := make([]versionedObject, len(versions))
	copy(result, versions)
	return result
}

// XML types for versioning API responses.

type versioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:"Status,omitempty"`
}

// handlePutBucketVersioning enables or suspends versioning on a bucket.
func (g *Gateway) handlePutBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Verify bucket exists.
	bInfo, err := g.buckets.GetBucket(ctx, bucket)
	if err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "failed to read request body", http.StatusInternalServerError)
		return
	}

	var config versioningConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		writeS3Error(w, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	switch config.Status {
	case "Enabled", "Suspended":
		bInfo.Versioning = config.Status
	default:
		writeS3Error(w, "MalformedXML", "Invalid versioning status", http.StatusBadRequest)
		return
	}

	if err := g.buckets.PutBucket(ctx, bInfo); err != nil {
		writeS3Error(w, "InternalError", "failed to update bucket", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleGetBucketVersioning returns the current versioning state of a bucket.
func (g *Gateway) handleGetBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	bInfo, err := g.buckets.GetBucket(ctx, bucket)
	if err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	config := versioningConfiguration{
		Status: bInfo.Versioning,
	}

	writeXMLResponse(w, http.StatusOK, &config)
}

// handlePutObjectVersioned handles PUT for a versioned bucket,
// generating version IDs and preserving previous versions.
func (g *Gateway) handlePutObjectVersioned(w http.ResponseWriter, r *http.Request, bucket, key string, vs *versionStore) {
	ctx := r.Context()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "failed to read request body", http.StatusInternalServerError)
		return
	}

	etag := fmt.Sprintf("%x", md5.Sum(body))

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

	versionID := vs.addVersion(bucket, key, obj)

	// Also update the primary object store with the latest version.
	if err := g.objects.PutObject(ctx, obj); err != nil {
		writeS3Error(w, "InternalError", "failed to store object metadata", http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("x-amz-version-id", versionID)
	w.WriteHeader(http.StatusOK)
}

// handleGetObjectVersioned handles GET with a ?versionId= parameter.
func (g *Gateway) handleGetObjectVersioned(w http.ResponseWriter, r *http.Request, bucket, key, versionID string, vs *versionStore) {
	obj, found := vs.getVersion(bucket, key, versionID)
	if !found {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
		return
	}

	ctx := r.Context()

	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.Header().Set("Last-Modified", time.Unix(obj.ModTime, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("x-amz-version-id", obj.VersionID)
	w.WriteHeader(http.StatusOK)

	for _, cid := range obj.ChunkIDs {
		data, err := g.chunks.GetChunkData(ctx, cid)
		if err != nil {
			return
		}
		if _, err := w.Write(data); err != nil {
			return
		}
	}
}
