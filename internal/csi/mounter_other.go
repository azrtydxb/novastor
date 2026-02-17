//go:build !linux

package csi

import "fmt"

// Mount is not supported on non-Linux platforms.
func (m *RealMounter) Mount(source, target string) error {
	return fmt.Errorf("bind mount not supported on this platform")
}

// Unmount is not supported on non-Linux platforms.
func (m *RealMounter) Unmount(target string) error {
	return fmt.Errorf("unmount not supported on this platform")
}
