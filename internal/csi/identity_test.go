package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func TestGetPluginInfo(t *testing.T) {
	srv := NewIdentityServer()
	resp, err := srv.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo returned error: %v", err)
	}
	if resp.GetName() != pluginName {
		t.Errorf("expected plugin name %q, got %q", pluginName, resp.GetName())
	}
	if resp.GetVendorVersion() != Version {
		t.Errorf("expected version %q, got %q", Version, resp.GetVendorVersion())
	}
}

func TestGetPluginCapabilities(t *testing.T) {
	srv := NewIdentityServer()
	resp, err := srv.GetPluginCapabilities(context.Background(), &csi.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetPluginCapabilities returned error: %v", err)
	}

	caps := resp.GetCapabilities()
	if len(caps) != 2 {
		t.Fatalf("expected 2 capabilities, got %d", len(caps))
	}

	expected := map[csi.PluginCapability_Service_Type]bool{
		csi.PluginCapability_Service_CONTROLLER_SERVICE:               false,
		csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS: false,
	}

	for _, cap := range caps {
		svc := cap.GetService()
		if svc == nil {
			t.Fatal("expected service capability, got nil")
		}
		if _, ok := expected[svc.GetType()]; !ok {
			t.Errorf("unexpected capability type: %v", svc.GetType())
		}
		expected[svc.GetType()] = true
	}

	for capType, found := range expected {
		if !found {
			t.Errorf("missing expected capability: %v", capType)
		}
	}
}

func TestProbe(t *testing.T) {
	srv := NewIdentityServer()
	resp, err := srv.Probe(context.Background(), &csi.ProbeRequest{})
	if err != nil {
		t.Fatalf("Probe returned error: %v", err)
	}
	if !resp.GetReady().GetValue() {
		t.Error("expected Probe to report ready=true")
	}
}
