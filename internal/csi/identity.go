package csi

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Version is set at build time via -ldflags.
var Version = "0.0.0"

const pluginName = "novastor.csi.novastor.io"

// IdentityServer implements the CSI Identity service.
type IdentityServer struct {
	csi.UnimplementedIdentityServer
}

// NewIdentityServer returns a ready IdentityServer.
func NewIdentityServer() *IdentityServer {
	return &IdentityServer{}
}

// GetPluginInfo returns the name and version of this CSI plugin.
func (s *IdentityServer) GetPluginInfo(_ context.Context, _ *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          pluginName,
		VendorVersion: Version,
	}, nil
}

// GetPluginCapabilities advertises the capabilities of this plugin.
func (s *IdentityServer) GetPluginCapabilities(_ context.Context, _ *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
					},
				},
			},
		},
	}, nil
}

// Probe indicates the plugin is healthy and ready.
func (s *IdentityServer) Probe(_ context.Context, _ *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{
		Ready: wrapperspb.Bool(true),
	}, nil
}
