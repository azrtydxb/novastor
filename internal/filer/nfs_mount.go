package filer

import (
	"net"

	"go.uber.org/zap"

	"github.com/piwi3910/novastor/internal/logging"
)

// MOUNT v3 protocol constants (RFC 1813, appendix I).
const (
	mountProg    uint32 = 100005
	mountVersion uint32 = 3

	// MOUNT v3 procedures.
	mountProcNull    uint32 = 0
	mountProcMnt     uint32 = 1
	mountProcDump    uint32 = 2
	mountProcUmnt    uint32 = 3
	mountProcUmntAll uint32 = 4
	mountProcExport  uint32 = 5

	// MOUNT v3 status codes.
	mntOK        uint32 = 0
	mntErrPerm   uint32 = 1
	mntErrNoEnt  uint32 = 2
	mntErrIO     uint32 = 5
	mntErrAccess uint32 = 13
	mntErrNotDir uint32 = 20
	mntErrInval  uint32 = 22
)

// mountHandler implements the MOUNT v3 program.
type mountHandler struct {
	handles    *handleManager
	exportPath string
}

func newMountHandler(handles *handleManager, exportPath string) *mountHandler {
	return &mountHandler{
		handles:    handles,
		exportPath: exportPath,
	}
}

func (m *mountHandler) Program() uint32 { return mountProg }
func (m *mountHandler) Version() uint32 { return mountVersion }

func (m *mountHandler) HandleProc(proc uint32, _ uint32, payload []byte, _ net.Conn) ([]byte, error) {
	switch proc {
	case mountProcNull:
		return m.handleNull()
	case mountProcMnt:
		return m.handleMnt(payload)
	case mountProcDump:
		return m.handleDump()
	case mountProcUmnt:
		return m.handleUmnt(payload)
	case mountProcUmntAll:
		return m.handleUmntAll()
	case mountProcExport:
		return m.handleExport()
	default:
		logging.L.Warn("mount: unknown procedure", zap.Uint32("proc", proc))
		return nil, nil
	}
}

// handleNull implements MOUNTPROC3_NULL - a no-op for ping/health checks.
func (m *mountHandler) handleNull() ([]byte, error) {
	return nil, nil
}

// handleMnt implements MOUNTPROC3_MNT - mounts the export and returns a root file handle.
func (m *mountHandler) handleMnt(payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	dirPath, readErr := r.readString()
	if readErr != nil {
		return m.mountError(mntErrInval), nil
	}

	logging.L.Info("mount request", zap.String("path", dirPath))

	// Accept any mount path for the single export; NovaStor exposes one filesystem.
	// A more restrictive implementation could validate against m.exportPath.

	rootHandle := m.handles.getOrCreateHandle(RootIno)

	w := newXDRWriter()
	// fhs_status: MNT3_OK
	w.writeUint32(mntOK)
	// fhandle3: opaque file handle
	w.writeOpaque(rootHandle)
	// auth_flavors: list of supported auth flavors (just AUTH_NONE)
	w.writeUint32(1)        // count
	w.writeUint32(authNone) // AUTH_NONE

	return w.Bytes(), nil
}

// handleDump implements MOUNTPROC3_DUMP - returns the list of mounted clients.
func (m *mountHandler) handleDump() ([]byte, error) {
	w := newXDRWriter()
	// Return empty list (no active mounts tracked, bool false = end of list).
	w.writeBool(false)
	return w.Bytes(), nil
}

// handleUmnt implements MOUNTPROC3_UMNT - unmount notification.
func (m *mountHandler) handleUmnt(payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	dirPath, _ := r.readString()
	logging.L.Info("unmount request", zap.String("path", dirPath))
	// No state to clean up; return void.
	return nil, nil
}

// handleUmntAll implements MOUNTPROC3_UMNTALL - unmount all notification.
func (m *mountHandler) handleUmntAll() ([]byte, error) {
	logging.L.Info("unmount all request")
	return nil, nil
}

// handleExport implements MOUNTPROC3_EXPORT - returns the export list.
func (m *mountHandler) handleExport() ([]byte, error) {
	w := newXDRWriter()
	// One export entry: value follows = true.
	w.writeBool(true)
	w.writeString(m.exportPath)
	// groups: empty list (no host restrictions).
	w.writeBool(false)
	// No more exports.
	w.writeBool(false)
	return w.Bytes(), nil
}

// mountError builds a mount reply with the given error status.
func (m *mountHandler) mountError(status uint32) []byte {
	w := newXDRWriter()
	w.writeUint32(status)
	return w.Bytes()
}
