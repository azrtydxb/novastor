package s3

import (
	"encoding/xml"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// --- XML types for ListParts ---

type listPartsResult struct {
	XMLName               xml.Name   `xml:"ListPartsResult"`
	Bucket                string     `xml:"Bucket"`
	Key                   string     `xml:"Key"`
	UploadID              string     `xml:"UploadId"`
	Initiator             initiator  `xml:"Initiator"`
	Owner                 owner      `xml:"Owner"`
	StorageClass          string     `xml:"StorageClass"`
	PartNumberMarker      int        `xml:"PartNumberMarker"`
	NextPartNumberMarker  int        `xml:"NextPartNumberMarker,omitempty"`
	MaxParts              int        `xml:"MaxParts"`
	IsTruncated           bool       `xml:"IsTruncated"`
	Parts                 []listPart `xml:"Part,omitempty"`
}

type initiator struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type listPart struct {
	PartNumber   int    `xml:"PartNumber"`
	Size         int64  `xml:"Size"`
	ETag         string `xml:"ETag"`
	LastModified string `xml:"LastModified"`
}

// handleListParts handles GET /<bucket>/<key>?uploadId=ID.
func (g *Gateway) handleListParts(w http.ResponseWriter, r *http.Request, bucket, key string) {
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

	// Parse query parameters for pagination.
	partNumberMarker := 0
	if pm := r.URL.Query().Get("part-number-marker"); pm != "" {
		if n, err := strconv.Atoi(pm); err == nil && n >= 0 {
			partNumberMarker = n
		}
	}

	maxParts := 1000
	if mp := r.URL.Query().Get("max-parts"); mp != "" {
		if n, err := strconv.Atoi(mp); err == nil && n > 0 {
			maxParts = n
		}
	}

	// Filter and paginate parts.
	var filteredParts []PartInfo
	for _, p := range mpInfo.Parts {
		if p.PartNumber > partNumberMarker {
			filteredParts = append(filteredParts, p)
		}
	}

	isTruncated := false
	var partsToList []PartInfo
	if len(filteredParts) > maxParts {
		isTruncated = true
		partsToList = filteredParts[:maxParts]
	} else {
		partsToList = filteredParts
	}

	// Build response parts.
	parts := make([]listPart, 0, len(partsToList))
	for _, p := range partsToList {
		parts = append(parts, listPart{
			PartNumber:   p.PartNumber,
			Size:         p.Size,
			ETag:         `"` + p.ETag + `"`,
			LastModified: time.Unix(mpInfo.CreationDate, 0).UTC().Format(time.RFC3339),
		})
	}

	nextPartNumberMarker := 0
	if isTruncated && len(partsToList) > 0 {
		nextPartNumberMarker = partsToList[len(partsToList)-1].PartNumber
	}

	result := listPartsResult{
		Bucket:               bucket,
		Key:                  mpInfo.Key,
		UploadID:             uploadID,
		Initiator:            initiator{ID: "novastor", DisplayName: "novastor"},
		Owner:                owner{ID: "novastor", DisplayName: "novastor"},
		StorageClass:         "STANDARD",
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                parts,
	}

	writeXMLResponse(w, http.StatusOK, &result)
}

// --- XML types for CopyObject ---

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// handleCopyObject handles PUT /<bucket>/<key> with x-amz-copy-source header.
func (g *Gateway) handleCopyObject(w http.ResponseWriter, r *http.Request, dstBucket, dstKey string) {
	ctx := r.Context()

	copySource := r.Header.Get("X-Amz-Copy-Source")
	if copySource == "" {
		writeS3Error(w, "InvalidRequest", "Missing x-amz-copy-source header", http.StatusBadRequest)
		return
	}

	// Parse the source path. It can be /bucket/key or /bucket/key?versionId=...
	// Remove leading slash and parse bucket/key.
	srcPath := copySource
	if len(srcPath) > 0 && srcPath[0] == '/' {
		srcPath = srcPath[1:]
	}

	slashIdx := -1
	for i, c := range srcPath {
		if c == '/' {
			slashIdx = i
			break
		}
	}

	if slashIdx < 0 {
		writeS3Error(w, "InvalidArgument", "Invalid copy source format", http.StatusBadRequest)
		return
	}

	srcBucket := srcPath[:slashIdx]
	srcKey := srcPath[slashIdx+1:]

	// Remove query string (versionId) if present.
	queryIdx := strings.Index(srcKey, "?")
	if queryIdx >= 0 {
		srcKey = srcKey[:queryIdx]
	}

	// Verify source bucket exists.
	if _, err := g.buckets.GetBucket(ctx, srcBucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The source bucket does not exist", http.StatusNotFound)
		return
	}

	// Verify destination bucket exists.
	if _, err := g.buckets.GetBucket(ctx, dstBucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The destination bucket does not exist", http.StatusNotFound)
		return
	}

	// Get source object.
	srcObj, err := g.objects.GetObject(ctx, srcBucket, srcKey)
	if err != nil {
		writeS3Error(w, "NoSuchKey", "The source key does not exist", http.StatusNotFound)
		return
	}

	// Create a copy of the object metadata with new bucket/key.
	dstObj := &ObjectInfo{
		Bucket:      dstBucket,
		Key:         dstKey,
		Size:        srcObj.Size,
		ETag:        srcObj.ETag,
		ContentType: srcObj.ContentType,
		UserMeta:    make(map[string]string),
		ChunkIDs:    make([]string, len(srcObj.ChunkIDs)),
		ModTime:     time.Now().Unix(),
	}

	// Copy user metadata.
	for k, v := range srcObj.UserMeta {
		dstObj.UserMeta[k] = v
	}

	// Copy chunk IDs (chunks are shared, not duplicated).
	copy(dstObj.ChunkIDs, srcObj.ChunkIDs)

	// Store the destination object.
	if err := g.objects.PutObject(ctx, dstObj); err != nil {
		writeS3Error(w, "InternalError", "Failed to store object", http.StatusInternalServerError)
		return
	}

	// Handle metadata directive - REPLACE vs COPY.
	metadataDirective := r.Header.Get("X-Amz-Metadata-Directive")
	if metadataDirective == "REPLACE" {
		// Update content-type from request if provided.
		if ct := r.Header.Get("Content-Type"); ct != "" {
			dstObj.ContentType = ct
		}
		// Re-store with updated metadata.
		if err := g.objects.PutObject(ctx, dstObj); err != nil {
			writeS3Error(w, "InternalError", "Failed to update object metadata", http.StatusInternalServerError)
			return
		}
	}

	result := copyObjectResult{
		LastModified: time.Unix(dstObj.ModTime, 0).UTC().Format(time.RFC3339),
		ETag:         `"` + dstObj.ETag + `"`,
	}

	writeXMLResponse(w, http.StatusOK, &result)
}

// --- XML types for Object Tagging ---

type tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	TagSet  tagSet   `xml:"TagSet"`
}

type tagSet struct {
	Tag []tag `xml:"Tag"`
}

type tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// handleGetObjectTagging handles GET /<bucket>/<key>?tagging.
func (g *Gateway) handleGetObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()

	obj, err := g.objects.GetObject(ctx, bucket, key)
	if err != nil {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}

	// Build tag list from UserMeta (we store tags with prefix "tag:").
	var tags []tag
	for k, v := range obj.UserMeta {
		if len(k) > 4 && k[:4] == "tag:" {
			tags = append(tags, tag{
				Key:   k[4:],
				Value: v,
			})
		}
	}

	result := tagging{
		TagSet: tagSet{Tag: tags},
	}

	writeXMLResponse(w, http.StatusOK, &result)
}

// handlePutObjectTagging handles PUT /<bucket>/<key>?tagging.
func (g *Gateway) handlePutObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()

	// Get existing object to update its tags.
	obj, err := g.objects.GetObject(ctx, bucket, key)
	if err != nil {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}

	// Parse request body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "Failed to read request body", http.StatusInternalServerError)
		return
	}

	var tagging tagging
	if err := xml.Unmarshal(body, &tagging); err != nil {
		writeS3Error(w, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	// Clear existing tags.
	if obj.UserMeta == nil {
		obj.UserMeta = make(map[string]string)
	}
	for k := range obj.UserMeta {
		if len(k) > 4 && k[:4] == "tag:" {
			delete(obj.UserMeta, k)
		}
	}

	// Add new tags.
	for _, t := range tagging.TagSet.Tag {
		if t.Key == "" {
			writeS3Error(w, "InvalidTag", "Tag key cannot be empty", http.StatusBadRequest)
			return
		}
		obj.UserMeta["tag:"+t.Key] = t.Value
	}

	// Update the object.
	if err := g.objects.PutObject(ctx, obj); err != nil {
		writeS3Error(w, "InternalError", "Failed to update object", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleDeleteObjectTagging handles DELETE /<bucket>/<key>?tagging.
func (g *Gateway) handleDeleteObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()

	obj, err := g.objects.GetObject(ctx, bucket, key)
	if err != nil {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}

	// Remove all tags.
	if obj.UserMeta != nil {
		for k := range obj.UserMeta {
			if len(k) > 4 && k[:4] == "tag:" {
				delete(obj.UserMeta, k)
			}
		}
	}

	// Update the object.
	if err := g.objects.PutObject(ctx, obj); err != nil {
		writeS3Error(w, "InternalError", "Failed to update object", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- XML types for Bucket Lifecycle ---

type lifecycleConfiguration struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration"`
	Rules   []lifecycleRule `xml:"Rule"`
}

type lifecycleRule struct {
	ID         string              `xml:"ID"`
	Status     string              `xml:"Status"`
	Filter     lifecycleFilter     `xml:"Filter,omitempty"`
	Expiration *lifecycleExpiration `xml:"Expiration,omitempty"`
	Transitions []lifecycleTransition `xml:"Transition,omitempty"`
}

type lifecycleFilter struct {
	Prefix string `xml:"Prefix,omitempty"`
	Tag    tag    `xml:"Tag,omitempty"`
}

type lifecycleExpiration struct {
	Days int `xml:"Days,omitempty"`
}

type lifecycleTransition struct {
	Days int `xml:"Days,omitempty"`
}

// In-memory storage for lifecycle configurations (keyed by bucket name).
var (
	lifecycleStore   = make(map[string]*lifecycleConfiguration)
	lifecycleStoreMu = &dummyLock{}
)

type dummyLock struct{}

func (d *dummyLock) Lock()   {}
func (d *dummyLock) Unlock() {}

// handleGetBucketLifecycle handles GET /<bucket>?lifecycle.
func (g *Gateway) handleGetBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Verify bucket exists.
	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	lifecycleStoreMu.Lock()
	config, ok := lifecycleStore[bucket]
	lifecycleStoreMu.Unlock()

	if !ok {
		writeS3Error(w, "NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist", http.StatusNotFound)
		return
	}

	writeXMLResponse(w, http.StatusOK, config)
}

// handlePutBucketLifecycle handles PUT /<bucket>?lifecycle.
func (g *Gateway) handlePutBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Verify bucket exists.
	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "Failed to read request body", http.StatusInternalServerError)
		return
	}

	var config lifecycleConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		writeS3Error(w, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	// Validate rules.
	for i, rule := range config.Rules {
		if rule.ID == "" {
			rule.ID = uuid.New().String()
			config.Rules[i] = rule
		}
		if rule.Status != "Enabled" && rule.Status != "Disabled" {
			writeS3Error(w, "MalformedXML", "Rule status must be Enabled or Disabled", http.StatusBadRequest)
			return
		}
	}

	lifecycleStoreMu.Lock()
	lifecycleStore[bucket] = &config
	lifecycleStoreMu.Unlock()

	w.WriteHeader(http.StatusOK)
}

// handleDeleteBucketLifecycle handles DELETE /<bucket>?lifecycle.
func (g *Gateway) handleDeleteBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Verify bucket exists.
	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	lifecycleStoreMu.Lock()
	delete(lifecycleStore, bucket)
	lifecycleStoreMu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

// --- XML types for Bucket Encryption ---

type serverSideEncryptionConfiguration struct {
	XMLName xml.Name          `xml:"ServerSideEncryptionConfiguration"`
	Rule    encryptionRule    `xml:"Rule"`
}

type encryptionRule struct {
	ApplyServerSideEncryptionByDefault encryptionSettings `xml:"ApplyServerSideEncryptionByDefault"`
}

type encryptionSettings struct {
	SSEAlgorithm   string `xml:"SSEAlgorithm"`
	KMSMasterKeyID string `xml:"KMSMasterKeyID,omitempty"`
}

// In-memory storage for encryption configurations (keyed by bucket name).
var (
	encryptionStore   = make(map[string]*serverSideEncryptionConfiguration)
	encryptionStoreMu = &dummyLock{}
)

// handleGetBucketEncryption handles GET /<bucket>?encryption.
func (g *Gateway) handleGetBucketEncryption(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Verify bucket exists.
	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	encryptionStoreMu.Lock()
	config, ok := encryptionStore[bucket]
	encryptionStoreMu.Unlock()

	if !ok {
		writeS3Error(w, "ServerSideEncryptionConfigurationNotFoundError", "The server side encryption configuration was not found", http.StatusNotFound)
		return
	}

	writeXMLResponse(w, http.StatusOK, config)
}

// handlePutBucketEncryption handles PUT /<bucket>?encryption.
func (g *Gateway) handlePutBucketEncryption(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Verify bucket exists.
	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "Failed to read request body", http.StatusInternalServerError)
		return
	}

	var config serverSideEncryptionConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		writeS3Error(w, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	// Validate algorithm.
	algo := config.Rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm
	if algo != "AES256" && algo != "aws:kms" {
		writeS3Error(w, "MalformedXML", "SSEAlgorithm must be AES256 or aws:kms", http.StatusBadRequest)
		return
	}

	encryptionStoreMu.Lock()
	encryptionStore[bucket] = &config
	encryptionStoreMu.Unlock()

	w.WriteHeader(http.StatusOK)
}

// handleDeleteBucketEncryption handles DELETE /<bucket>?encryption.
func (g *Gateway) handleDeleteBucketEncryption(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Verify bucket exists.
	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	encryptionStoreMu.Lock()
	delete(encryptionStore, bucket)
	encryptionStoreMu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

// --- XML types for ListObjectVersions ---

type listObjectVersionsResult struct {
	XMLName          xml.Name            `xml:"ListVersionsResult"`
	Name             string              `xml:"Name"`
	Prefix           string              `xml:"Prefix,omitempty"`
	KeyMarker        string              `xml:"KeyMarker,omitempty"`
	VersionIdMarker  string              `xml:"VersionIdMarker,omitempty"`
	MaxKeys          int                 `xml:"MaxKeys"`
	IsTruncated      bool                `xml:"IsTruncated"`
	Versions         []objectVersion     `xml:"Version,omitempty"`
	CommonPrefixes   []commonPrefix      `xml:"CommonPrefixes,omitempty"`
}

type objectVersion struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

// handleListObjectVersions handles GET /<bucket>?versions.
func (g *Gateway) handleListObjectVersions(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Verify bucket exists.
	bInfo, err := g.buckets.GetBucket(ctx, bucket)
	if err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	// If versioning is not enabled, return empty list.
	if bInfo.Versioning != "Enabled" && bInfo.Versioning != "Suspended" {
		// For buckets without versioning, we return an empty versions list.
		result := listObjectVersionsResult{
			Name:        bucket,
			MaxKeys:     1000,
			IsTruncated: false,
		}
		writeXMLResponse(w, http.StatusOK, &result)
		return
	}

	// Parse query parameters.
	prefix := r.URL.Query().Get("prefix")
	keyMarker := r.URL.Query().Get("key-marker")
	versionIdMarker := r.URL.Query().Get("version-id-marker")

	maxKeys := 1000
	if mk := r.URL.Query().Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n > 0 {
			maxKeys = n
		}
	}

	// List all objects.
	objects, err := g.objects.ListObjects(ctx, bucket, prefix)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	// For this implementation, we'll return current versions as version entries.
	// A full implementation would store and return all historical versions.
	var versions []objectVersion
	for _, obj := range objects {
		// Skip objects before key-marker.
		if keyMarker != "" && obj.Key < keyMarker {
			continue
		}

		versions = append(versions, objectVersion{
			Key:          obj.Key,
			VersionID:    obj.VersionID,
			IsLatest:     obj.VersionID != "" || true, // Current version
			LastModified: time.Unix(obj.ModTime, 0).UTC().Format(time.RFC3339),
			ETag:         `"` + obj.ETag + `"`,
			Size:         obj.Size,
		})
	}

	// Truncate if needed.
	isTruncated := false
	if len(versions) > maxKeys {
		isTruncated = true
		versions = versions[:maxKeys]
	}

	result := listObjectVersionsResult{
		Name:            bucket,
		Prefix:          prefix,
		KeyMarker:       keyMarker,
		VersionIdMarker: versionIdMarker,
		MaxKeys:         maxKeys,
		IsTruncated:     isTruncated,
		Versions:        versions,
	}

	writeXMLResponse(w, http.StatusOK, &result)
}
