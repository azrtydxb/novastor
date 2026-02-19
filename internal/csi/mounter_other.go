//go:build !linux

package csi

import "fmt"

// Mount is not supported on non-Linux platforms.
func (m *RealMounter) Mount(_ string, _ string) error {
	return fmt.Errorf("bind mount not supported on this platform")
}

// Unmount is not supported on non-Linux platforms.
func (m *RealMounter) Unmount(_ string) error {
	return fmt.Errorf("unmount not supported on this platform")
}
