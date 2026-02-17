# Phase 3: Object Storage — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an S3-compatible HTTP gateway that stores objects as chunks in the shared engine, with bucket management, multipart upload, and basic IAM.

**Architecture:** Stateless S3 gateway (horizontally scalable Deployment) translates S3 API operations into metadata lookups + chunk read/write operations. Object metadata stored in the Raft metadata service. Objects are split into chunks using the chunk engine's SplitData.

**Tech Stack:** net/http (S3 API), AWS Signature V4 verification, XML encoding for S3 responses.

---

### Task 1: Object Metadata in Metadata Service

**Files:**
- Create: `internal/metadata/object_meta.go`
- Create: `internal/metadata/object_meta_test.go`

Add ObjectMeta (bucket, key, size, etag, content-type, user metadata, chunk IDs, version) and BucketMeta (name, creation date, versioning state) to the Raft store. CRUD operations for both.

### Task 2: S3 API Router and Auth

**Files:**
- Create: `internal/s3/router.go`
- Create: `internal/s3/auth.go`
- Create: `internal/s3/auth_test.go`

HTTP router that dispatches S3 requests by method + path pattern to handlers. AWS Signature V4 request verification. Access key management via Kubernetes Secrets.

### Task 3: Bucket Operations

**Files:**
- Create: `internal/s3/bucket.go`
- Create: `internal/s3/bucket_test.go`

CreateBucket, DeleteBucket, ListBuckets, HeadBucket. Enforce max bucket limit from ObjectStore CRD. S3-compatible XML responses.

### Task 4: Object Put and Get

**Files:**
- Create: `internal/s3/object.go`
- Create: `internal/s3/object_test.go`

PutObject: split data into chunks via chunk engine, store chunk IDs in object metadata. GetObject: look up metadata, read chunks, stream response with proper Content-Length/ETag. HeadObject. DeleteObject: remove metadata, garbage-collect unreferenced chunks.

### Task 5: Multipart Upload

**Files:**
- Create: `internal/s3/multipart.go`
- Create: `internal/s3/multipart_test.go`

CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts. Each part becomes one or more chunks. CompleteMultipartUpload assembles the chunk ID list.

### Task 6: List Objects

**Files:**
- Create: `internal/s3/list.go`
- Create: `internal/s3/list_test.go`

ListObjectsV2 with prefix, delimiter, continuation token, max-keys. Supports directory-like browsing with CommonPrefixes.

### Task 7: S3 Gateway Binary

**Files:**
- Modify: `cmd/s3gw/main.go`

Wire up router, auth, metadata client, chunk client. HTTP server with graceful shutdown. Prometheus metrics endpoint.

### Task 8: S3 Gateway Operator Reconciler

**Files:**
- Create: `internal/operator/objectstore_controller.go`
- Create: `internal/operator/objectstore_controller_test.go`

Reconcile ObjectStore CRDs: create/update S3 gateway Deployment, Service, and access credentials Secret. Update status with endpoint.

### Task 9: Final Verification

S3 compatibility testing with AWS CLI and MinIO client. Full test suite, lint, build.
