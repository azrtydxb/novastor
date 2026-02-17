package csi

import "syscall"

// Mount performs a bind mount from source to target using Linux syscall.
func (m *RealMounter) Mount(source, target string) error {
	return syscall.Mount(source, target, "", syscall.MS_BIND, "")
}

// Unmount unmounts the given target path using Linux syscall.
func (m *RealMounter) Unmount(target string) error {
	return syscall.Unmount(target, 0)
}
