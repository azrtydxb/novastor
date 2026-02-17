# Phase 4: File Storage — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an NFS v4.1 gateway providing shared filesystem access (ReadWriteMany) backed by the chunk engine, with directory tree management, file locking, and POSIX-like semantics.

**Architecture:** NFS gateway Deployment translates NFS operations into metadata lookups (directory tree, inodes) + chunk read/write. File metadata (inodes, directory entries, xattrs) stored in the Raft metadata service. File data split into chunks.

**Tech Stack:** go-nfs library for NFS v4.1 server, POSIX-like inode model in metadata.

---

### Task 1: File Metadata in Metadata Service

**Files:**
- Create: `internal/metadata/file_meta.go`
- Create: `internal/metadata/file_meta_test.go`

Add InodeMeta (inode number, type, size, mode, uid, gid, timestamps, chunk IDs, xattrs) and DirEntry (name, inode, type) to the Raft store. Operations: CreateInode, GetInode, UpdateInode, DeleteInode, ListDirectory, CreateDirEntry, DeleteDirEntry, LookupDirEntry.

### Task 2: File Gateway — POSIX Operations

**Files:**
- Create: `internal/filer/fs.go`
- Create: `internal/filer/fs_test.go`

Implement a virtual filesystem layer that maps POSIX operations (open, read, write, mkdir, rmdir, readdir, stat, chmod, chown, rename, link, symlink, unlink) to metadata service + chunk engine operations.

### Task 3: NFS Server

**Files:**
- Create: `internal/filer/nfs.go`
- Create: `internal/filer/nfs_test.go`

NFS v4.1 server using go-nfs library. Exports the virtual filesystem. Handles file handle mapping, attribute caching, compound operations.

### Task 4: File Locking and Leases

**Files:**
- Create: `internal/filer/lock.go`
- Create: `internal/filer/lock_test.go`

Advisory file locking (byte-range locks) stored in metadata service. Lease management for NFS delegation. Lock cleanup on client disconnect.

### Task 5: File Gateway Binary

**Files:**
- Modify: `cmd/filer/main.go`

Wire up NFS server, metadata client, chunk client. Listen on NFS port (2049). Prometheus metrics. Graceful shutdown.

### Task 6: SharedFilesystem Operator Reconciler

**Files:**
- Create: `internal/operator/sharedfs_controller.go`
- Create: `internal/operator/sharedfs_controller_test.go`

Reconcile SharedFilesystem CRDs: create/update filer Deployment, Service (ClusterIP on port 2049), root inode. Update status with endpoint. Support CSI RWX volumes pointing at NFS endpoint.

### Task 7: RWX PVC Support via CSI

**Files:**
- Modify: `internal/csi/controller.go`
- Modify: `internal/csi/node.go`

Extend CSI driver to handle ReadWriteMany volumes by provisioning SharedFilesystem + NFS mount instead of direct block attach.

### Task 8: Final Verification

Mount NFS from multiple pods simultaneously, verify read/write/lock behavior. Full test suite, lint, build.
