package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.dataProtection.mode`
// +kubebuilder:printcolumn:name="Nodes",type=integer,JSONPath=`.status.nodeCount`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.status.totalCapacity`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// StoragePool defines a set of storage nodes and their data protection policy.
type StoragePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              StoragePoolSpec   `json:"spec,omitempty"`
	Status            StoragePoolStatus `json:"status,omitempty"`
}

type StoragePoolSpec struct {
	// NodeSelector selects which nodes belong to this pool.
	NodeSelector *metav1.LabelSelector `json:"nodeSelector,omitempty"`
	// DeviceFilter specifies which devices on selected nodes to use.
	DeviceFilter *DeviceFilter `json:"deviceFilter,omitempty"`
	// DataProtection configures how data is protected in this pool.
	DataProtection DataProtectionSpec `json:"dataProtection"`
}

type DeviceFilter struct {
	// +kubebuilder:validation:Enum=nvme;ssd;hdd
	Type    string `json:"type,omitempty"`
	MinSize string `json:"minSize,omitempty"`
}

type DataProtectionSpec struct {
	// +kubebuilder:validation:Enum=replication;erasureCoding
	Mode          string             `json:"mode"`
	Replication   *ReplicationSpec   `json:"replication,omitempty"`
	ErasureCoding *ErasureCodingSpec `json:"erasureCoding,omitempty"`
}

type ReplicationSpec struct {
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	Factor      int `json:"factor"`
	WriteQuorum int `json:"writeQuorum,omitempty"`
}

type ErasureCodingSpec struct {
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=2
	DataShards int `json:"dataShards"`
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	ParityShards int `json:"parityShards"`
}

type StoragePoolStatus struct {
	Phase          string             `json:"phase,omitempty"`
	NodeCount      int                `json:"nodeCount,omitempty"`
	TotalCapacity  string             `json:"totalCapacity,omitempty"`
	UsedCapacity   string             `json:"usedCapacity,omitempty"`
	DataProtection string             `json:"dataProtection,omitempty"`
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type StoragePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePool `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Size",type=string,JSONPath=`.spec.size`
// +kubebuilder:printcolumn:name="Access",type=string,JSONPath=`.spec.accessMode`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// BlockVolume represents a block storage volume.
type BlockVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              BlockVolumeSpec   `json:"spec,omitempty"`
	Status            BlockVolumeStatus `json:"status,omitempty"`
}

type BlockVolumeSpec struct {
	Pool string `json:"pool"`
	Size string `json:"size"`
	// +kubebuilder:validation:Enum=ReadWriteOnce;ReadOnlyMany
	AccessMode string `json:"accessMode"`
}

type BlockVolumeStatus struct {
	Phase      string             `json:"phase,omitempty"`
	Pool       string             `json:"pool,omitempty"`
	AccessMode string             `json:"accessMode,omitempty"`
	NodeID     string             `json:"nodeID,omitempty"`
	ChunkCount int                `json:"chunkCount,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type BlockVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BlockVolume `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.spec.capacity`
// +kubebuilder:printcolumn:name="Protocol",type=string,JSONPath=`.spec.export.protocol`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// SharedFilesystem represents a shared filesystem.
type SharedFilesystem struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SharedFilesystemSpec   `json:"spec,omitempty"`
	Status            SharedFilesystemStatus `json:"status,omitempty"`
}

type SharedFilesystemSpec struct {
	Pool     string `json:"pool"`
	Capacity string `json:"capacity"`
	// +kubebuilder:validation:Enum=ReadWriteMany;ReadOnlyMany
	AccessMode string      `json:"accessMode"`
	Export     *ExportSpec `json:"export,omitempty"`
	// Image overrides the NFS filer container image. Defaults to novastor/novastor-filer:v0.1.0.
	// +optional
	Image string `json:"image,omitempty"`
}

type ExportSpec struct {
	// +kubebuilder:validation:Enum=nfs
	Protocol string `json:"protocol"`
}

type SharedFilesystemStatus struct {
	Phase      string             `json:"phase,omitempty"`
	Endpoint   string             `json:"endpoint,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type SharedFilesystemList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SharedFilesystem `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Port",type=integer,JSONPath=`.spec.endpoint.service.port`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// ObjectStore represents an S3-compatible object store.
type ObjectStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ObjectStoreSpec   `json:"spec,omitempty"`
	Status            ObjectStoreStatus `json:"status,omitempty"`
}

type ObjectStoreSpec struct {
	Pool         string             `json:"pool"`
	Endpoint     ObjectEndpointSpec `json:"endpoint"`
	BucketPolicy *BucketPolicySpec  `json:"bucketPolicy,omitempty"`
	// Image overrides the S3 gateway container image. Defaults to novastor/novastor-s3gw:v0.1.0.
	// +optional
	Image string `json:"image,omitempty"`
}

type ObjectEndpointSpec struct {
	Service ObjectServiceSpec `json:"service"`
}

type ObjectServiceSpec struct {
	Port int32 `json:"port"`
}

type BucketPolicySpec struct {
	MaxBuckets int    `json:"maxBuckets,omitempty"`
	// +kubebuilder:validation:Enum=enabled;disabled;suspended
	Versioning string `json:"versioning,omitempty"`
}

type ObjectStoreStatus struct {
	Phase      string             `json:"phase,omitempty"`
	Endpoint   string             `json:"endpoint,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type ObjectStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ObjectStore `json:"items"`
}
