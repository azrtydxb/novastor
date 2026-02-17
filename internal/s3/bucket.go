package s3

import (
	"encoding/xml"
	"net/http"
	"time"
)

const maxBuckets = 100

// --- XML response types for bucket operations ---

type listAllMyBucketsResult struct {
	XMLName xml.Name       `xml:"ListAllMyBucketsResult"`
	Owner   bucketOwner    `xml:"Owner"`
	Buckets bucketListWrap `xml:"Buckets"`
}

type bucketOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketListWrap struct {
	Bucket []bucketEntry `xml:"Bucket"`
}

type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// handleCreateBucket handles PUT /<bucket>.
func (g *Gateway) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Check bucket limit.
	existing, err := g.buckets.ListBuckets(ctx)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	if len(existing) >= maxBuckets {
		writeS3Error(w, "TooManyBuckets", "You have attempted to create more buckets than allowed", http.StatusBadRequest)
		return
	}

	// Check if bucket already exists.
	if _, err := g.buckets.GetBucket(ctx, bucket); err == nil {
		writeS3Error(w, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", http.StatusConflict)
		return
	}

	info := &BucketInfo{
		Name:         bucket,
		CreationDate: time.Now().Unix(),
		Owner:        g.accessKey,
	}
	if err := g.buckets.PutBucket(ctx, info); err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

// handleDeleteBucket handles DELETE /<bucket>.
func (g *Gateway) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	if err := g.buckets.DeleteBucket(ctx, bucket); err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleListBuckets handles GET /.
func (g *Gateway) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	buckets, err := g.buckets.ListBuckets(ctx)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	entries := make([]bucketEntry, 0, len(buckets))
	for _, b := range buckets {
		entries = append(entries, bucketEntry{
			Name:         b.Name,
			CreationDate: time.Unix(b.CreationDate, 0).UTC().Format(time.RFC3339),
		})
	}

	result := listAllMyBucketsResult{
		Owner: bucketOwner{
			ID:          "novastor",
			DisplayName: "novastor",
		},
		Buckets: bucketListWrap{Bucket: entries},
	}

	writeXMLResponse(w, http.StatusOK, &result)
}

// handleHeadBucket handles HEAD /<bucket>.
func (g *Gateway) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}
