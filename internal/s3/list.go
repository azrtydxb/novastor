package s3

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// --- XML types for ListObjectsV2 ---

type listBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	Contents              []listObject   `xml:"Contents,omitempty"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes,omitempty"`
}

type listObject struct {
	Key          string `xml:"Key"`
	Size         int64  `xml:"Size"`
	ETag         string `xml:"ETag"`
	LastModified string `xml:"LastModified"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// handleListObjectsV2 handles GET /<bucket>?list-type=2.
func (g *Gateway) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()

	// Verify bucket exists.
	if _, err := g.buckets.GetBucket(ctx, bucket); err != nil {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	startAfter := q.Get("start-after")
	continuationToken := q.Get("continuation-token")

	maxKeys := 1000
	if mk := q.Get("max-keys"); mk != "" {
		if v, err := strconv.Atoi(mk); err == nil && v >= 0 {
			maxKeys = v
		}
	}

	// The continuation token in our simple implementation is the start-after key.
	if continuationToken != "" {
		startAfter = continuationToken
	}

	objects, err := g.objects.ListObjects(ctx, bucket, prefix)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	// Filter by startAfter.
	var filtered []*ObjectInfo
	for _, obj := range objects {
		if startAfter != "" && obj.Key <= startAfter {
			continue
		}
		filtered = append(filtered, obj)
	}

	var contents []listObject
	prefixSet := make(map[string]struct{})
	var commonPrefixes []commonPrefix

	for _, obj := range filtered {
		if delimiter != "" {
			// Check if the key (after prefix) contains the delimiter.
			rest := strings.TrimPrefix(obj.Key, prefix)
			idx := strings.Index(rest, delimiter)
			if idx >= 0 {
				// This key should be grouped under a common prefix.
				cp := prefix + rest[:idx+len(delimiter)]
				if _, exists := prefixSet[cp]; !exists {
					prefixSet[cp] = struct{}{}
					commonPrefixes = append(commonPrefixes, commonPrefix{Prefix: cp})
				}
				continue
			}
		}

		contents = append(contents, listObject{
			Key:          obj.Key,
			Size:         obj.Size,
			ETag:         `"` + obj.ETag + `"`,
			LastModified: time.Unix(obj.ModTime, 0).UTC().Format(time.RFC3339),
		})
	}

	// Apply max-keys limit across contents + common prefixes.
	isTruncated := false
	totalItems := len(contents) + len(commonPrefixes)
	if totalItems > maxKeys {
		isTruncated = true
		// Truncate contents first.
		if len(contents) > maxKeys {
			contents = contents[:maxKeys]
		} else {
			remaining := maxKeys - len(contents)
			if remaining < len(commonPrefixes) {
				commonPrefixes = commonPrefixes[:remaining]
			}
		}
	}

	keyCount := len(contents) + len(commonPrefixes)

	result := listBucketResult{
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		MaxKeys:        maxKeys,
		IsTruncated:    isTruncated,
		KeyCount:       keyCount,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}

	if continuationToken != "" {
		result.ContinuationToken = continuationToken
	}

	writeXMLResponse(w, http.StatusOK, &result)
}
