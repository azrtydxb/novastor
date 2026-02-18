package nvmeof

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// memoryDirEntry implements fs.DirEntry for in-memory directory entries.
type memoryDirEntry struct {
	name  string
	isDir bool
}

func (e memoryDirEntry) Name() string               { return e.name }
func (e memoryDirEntry) IsDir() bool                { return e.isDir }
func (e memoryDirEntry) Type() fs.FileMode          { return 0 }
func (e memoryDirEntry) Info() (fs.FileInfo, error) { return nil, nil }

// call records a single ConfigFS operation for verification.
type call struct {
	Method string
	Path   string
	Data   string // for WriteFile and Symlink (oldname stored here)
}

// MemoryConfigFS is an in-memory implementation of ConfigFS that records all
// operations for test verification.
type MemoryConfigFS struct {
	mu    sync.Mutex
	dirs  map[string]bool
	files map[string]string
	links map[string]string // newname -> oldname
	Calls []call
}

// NewMemoryConfigFS returns a ready-to-use MemoryConfigFS.
func NewMemoryConfigFS() *MemoryConfigFS {
	return &MemoryConfigFS{
		dirs:  make(map[string]bool),
		files: make(map[string]string),
		links: make(map[string]string),
	}
}

func (m *MemoryConfigFS) record(method, path, data string) {
	m.Calls = append(m.Calls, call{Method: method, Path: path, Data: data})
}

func (m *MemoryConfigFS) MkdirAll(path string, _ os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("MkdirAll", path, "")
	m.dirs[path] = true
	// Also mark all parent directories as existing.
	for p := filepath.Dir(path); p != path; p = filepath.Dir(p) {
		m.dirs[p] = true
		path = p
	}
	return nil
}

func (m *MemoryConfigFS) WriteFile(path string, data []byte, _ os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("WriteFile", path, string(data))
	m.files[path] = string(data)
	return nil
}

func (m *MemoryConfigFS) Remove(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, isDir := m.dirs[path]
	_, isFile := m.files[path]
	_, isLink := m.links[path]
	if !isDir && !isFile && !isLink {
		m.record("Remove", path, "")
		return fmt.Errorf("remove %s: no such file or directory", path)
	}
	m.record("Remove", path, "")
	delete(m.dirs, path)
	delete(m.files, path)
	delete(m.links, path)
	return nil
}

func (m *MemoryConfigFS) Symlink(oldname, newname string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("Symlink", newname, oldname)
	m.links[newname] = oldname
	return nil
}

func (m *MemoryConfigFS) ReadDir(path string) ([]fs.DirEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("ReadDir", path, "")

	// Collect unique direct children from dirs, files, and links.
	children := make(map[string]bool)
	prefix := path + "/"

	for p := range m.dirs {
		if strings.HasPrefix(p, prefix) {
			rel := strings.TrimPrefix(p, prefix)
			parts := strings.SplitN(rel, "/", 2)
			children[parts[0]] = true // true = directory
		}
	}
	for p := range m.files {
		if strings.HasPrefix(p, prefix) {
			rel := strings.TrimPrefix(p, prefix)
			parts := strings.SplitN(rel, "/", 2)
			if len(parts) == 1 {
				if _, exists := children[parts[0]]; !exists {
					children[parts[0]] = false
				}
			}
		}
	}
	for p := range m.links {
		if strings.HasPrefix(p, prefix) {
			rel := strings.TrimPrefix(p, prefix)
			parts := strings.SplitN(rel, "/", 2)
			if len(parts) == 1 {
				if _, exists := children[parts[0]]; !exists {
					children[parts[0]] = false
				}
			}
		}
	}

	var entries []fs.DirEntry
	for name, isDir := range children {
		entries = append(entries, memoryDirEntry{name: name, isDir: isDir})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	return entries, nil
}

// hasDir checks whether a directory was created.
func (m *MemoryConfigFS) hasDir(path string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dirs[path]
}

// hasFile checks whether a file was written and optionally verifies content.
func (m *MemoryConfigFS) hasFile(path, wantContent string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	content, ok := m.files[path]
	if !ok {
		return false
	}
	if wantContent != "" {
		return content == wantContent
	}
	return true
}

// hasLink checks whether a symlink was created with the expected target.
func (m *MemoryConfigFS) hasLink(newname, oldname string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	target, ok := m.links[newname]
	return ok && target == oldname
}

func TestCreateTarget(t *testing.T) {
	tests := []struct {
		name     string
		volumeID string
		port     int
		size     int64
	}{
		{
			name:     "basic volume",
			volumeID: "vol-001",
			port:     4420,
			size:     1073741824, // 1 GiB
		},
		{
			name:     "large volume",
			volumeID: "vol-large-abc",
			port:     4421,
			size:     10737418240, // 10 GiB
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memFS := NewMemoryConfigFS()
			tm := NewTargetManagerWithFS(memFS, "/sys/kernel/config/nvmet")

			err := tm.CreateTarget(tt.volumeID, tt.port, tt.size)
			if err != nil {
				t.Fatalf("CreateTarget() returned unexpected error: %v", err)
			}

			base := "/sys/kernel/config/nvmet"
			subsysDir := filepath.Join(base, "subsystems", "novastor-"+tt.volumeID)
			nsDir := filepath.Join(subsysDir, "namespaces", "1")
			portDir := filepath.Join(base, "ports", fmt.Sprintf("%d", tt.port))

			// Verify subsystem directory was created.
			if !memFS.hasDir(subsysDir) {
				t.Errorf("expected subsystem directory %s to exist", subsysDir)
			}

			// Verify attr_allow_any_host was set.
			attrPath := filepath.Join(subsysDir, "attr_allow_any_host")
			if !memFS.hasFile(attrPath, "1") {
				t.Errorf("expected attr_allow_any_host to be set to '1'")
			}

			// Verify namespace directory was created.
			if !memFS.hasDir(nsDir) {
				t.Errorf("expected namespace directory %s to exist", nsDir)
			}

			// Verify device_size was written.
			sizePath := filepath.Join(nsDir, "device_size")
			wantSize := fmt.Sprintf("%d", tt.size)
			if !memFS.hasFile(sizePath, wantSize) {
				t.Errorf("expected device_size to be %s", wantSize)
			}

			// Verify namespace enable was written.
			enablePath := filepath.Join(nsDir, "enable")
			if !memFS.hasFile(enablePath, "1") {
				t.Error("expected namespace enable to be set to '1'")
			}

			// Verify port directory was created.
			if !memFS.hasDir(portDir) {
				t.Errorf("expected port directory %s to exist", portDir)
			}

			// Verify transport type.
			trTypePath := filepath.Join(portDir, "addr_trtype")
			if !memFS.hasFile(trTypePath, "tcp") {
				t.Error("expected addr_trtype to be 'tcp'")
			}

			// Verify address family.
			adrFamPath := filepath.Join(portDir, "addr_adrfam")
			if !memFS.hasFile(adrFamPath, "ipv4") {
				t.Error("expected addr_adrfam to be 'ipv4'")
			}

			// Verify service ID.
			trSvcIDPath := filepath.Join(portDir, "addr_trsvcid")
			wantSvcID := fmt.Sprintf("%d", tt.port)
			if !memFS.hasFile(trSvcIDPath, wantSvcID) {
				t.Errorf("expected addr_trsvcid to be %s", wantSvcID)
			}

			// Verify symlink.
			linkPath := filepath.Join(portDir, "subsystems", "novastor-"+tt.volumeID)
			if !memFS.hasLink(linkPath, subsysDir) {
				t.Error("expected symlink from port/subsystems to subsystem directory")
			}
		})
	}
}

func TestDeleteTarget(t *testing.T) {
	memFS := NewMemoryConfigFS()
	tm := NewTargetManagerWithFS(memFS, "/sys/kernel/config/nvmet")

	volumeID := "vol-delete-test"
	port := 4420

	// First create a target so there is something to delete.
	if err := tm.CreateTarget(volumeID, port, 1073741824); err != nil {
		t.Fatalf("CreateTarget() setup failed: %v", err)
	}

	err := tm.DeleteTarget(volumeID)
	if err != nil {
		t.Fatalf("DeleteTarget() returned unexpected error: %v", err)
	}

	base := "/sys/kernel/config/nvmet"
	subsysDir := filepath.Join(base, "subsystems", "novastor-"+volumeID)
	nsDir := filepath.Join(subsysDir, "namespaces", "1")
	nsParent := filepath.Join(subsysDir, "namespaces")
	linkPath := filepath.Join(base, "ports", fmt.Sprintf("%d", port), "subsystems", "novastor-"+volumeID)

	// Verify the symlink was removed.
	if memFS.hasLink(linkPath, subsysDir) {
		t.Error("expected symlink to be removed")
	}

	// Verify namespace directory was removed.
	if memFS.hasDir(nsDir) {
		t.Error("expected namespace directory to be removed")
	}

	// Verify namespaces parent was removed.
	if memFS.hasDir(nsParent) {
		t.Error("expected namespaces parent directory to be removed")
	}

	// Verify subsystem directory was removed.
	if memFS.hasDir(subsysDir) {
		t.Error("expected subsystem directory to be removed")
	}
}

func TestListTargets(t *testing.T) {
	tests := []struct {
		name      string
		volumeIDs []string
		want      []string
	}{
		{
			name:      "no targets",
			volumeIDs: nil,
			want:      nil,
		},
		{
			name:      "single target",
			volumeIDs: []string{"vol-001"},
			want:      []string{"vol-001"},
		},
		{
			name:      "multiple targets",
			volumeIDs: []string{"vol-aaa", "vol-bbb", "vol-ccc"},
			want:      []string{"vol-aaa", "vol-bbb", "vol-ccc"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memFS := NewMemoryConfigFS()
			tm := NewTargetManagerWithFS(memFS, "/sys/kernel/config/nvmet")

			for i, vid := range tt.volumeIDs {
				if err := tm.CreateTarget(vid, 4420+i, 1073741824); err != nil {
					t.Fatalf("CreateTarget(%s) setup failed: %v", vid, err)
				}
			}

			got, err := tm.ListTargets()
			if err != nil {
				t.Fatalf("ListTargets() returned unexpected error: %v", err)
			}

			sort.Strings(got)
			sort.Strings(tt.want)

			if len(got) != len(tt.want) {
				t.Fatalf("ListTargets() returned %d targets, want %d", len(got), len(tt.want))
			}

			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("ListTargets()[%d] = %s, want %s", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestNewTargetManager(t *testing.T) {
	tm := NewTargetManager()
	if tm.basePath != DefaultBasePath {
		t.Errorf("NewTargetManager().basePath = %s, want %s", tm.basePath, DefaultBasePath)
	}
	if _, ok := tm.configFS.(RealConfigFS); !ok {
		t.Error("NewTargetManager().configFS should be RealConfigFS")
	}
}

// Ensure MemoryConfigFS satisfies the ConfigFS interface at compile time.
var _ ConfigFS = (*MemoryConfigFS)(nil)

// Ensure RealConfigFS satisfies the ConfigFS interface at compile time.
var _ ConfigFS = RealConfigFS{}

// Ensure memoryDirEntry satisfies fs.DirEntry at compile time.
var _ fs.DirEntry = memoryDirEntry{}

// TestCreateTargetConcurrent verifies that concurrent CreateTarget calls do
// not race on shared MemoryConfigFS state.
func TestCreateTargetConcurrent(t *testing.T) {
	memFS := NewMemoryConfigFS()
	tm := NewTargetManagerWithFS(memFS, "/sys/kernel/config/nvmet")

	const n = 10
	errs := make(chan error, n)
	for i := range n {
		go func(idx int) {
			errs <- tm.CreateTarget(fmt.Sprintf("vol-concurrent-%d", idx), 5000+idx, 1073741824)
		}(i)
	}

	for range n {
		select {
		case err := <-errs:
			if err != nil {
				t.Errorf("concurrent CreateTarget() returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for concurrent CreateTarget calls")
		}
	}

	got, err := tm.ListTargets()
	if err != nil {
		t.Fatalf("ListTargets() returned unexpected error: %v", err)
	}
	if len(got) != n {
		t.Errorf("ListTargets() returned %d targets, want %d", len(got), n)
	}
}
