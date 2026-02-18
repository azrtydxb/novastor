# RBAC Configuration

NovaStor requires specific Kubernetes RBAC permissions to manage its Custom Resources, PersistentVolumes, and node-level operations. When `rbac.create` is set to `true` in the Helm chart (the default), all necessary RBAC resources are created automatically.

## ServiceAccounts

The Helm chart creates a single shared ServiceAccount used by all NovaStor components:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: novastor
  namespace: novastor-system
  labels:
    app.kubernetes.io/name: novastor
```

Override the name with `serviceAccount.name` in Helm values. Add annotations (e.g., for workload identity) with `serviceAccount.annotations`.

## Required Permissions

### Controller / Operator

The controller requires the broadest set of permissions to reconcile CRDs and manage dependent resources.

| API Group | Resources | Verbs |
|---|---|---|
| `novastor.io` | `storagepools` | get, list, watch, create, update, patch, delete |
| `novastor.io` | `storagepools/status` | get, update, patch |
| `novastor.io` | `storagepools/finalizers` | update |
| `novastor.io` | `blockvolumes` | get, list, watch, create, update, patch, delete |
| `novastor.io` | `blockvolumes/status` | get, update, patch |
| `novastor.io` | `blockvolumes/finalizers` | update |
| `novastor.io` | `sharedfilesystems` | get, list, watch, create, update, patch, delete |
| `novastor.io` | `sharedfilesystems/status` | get, update, patch |
| `novastor.io` | `sharedfilesystems/finalizers` | update |
| `novastor.io` | `objectstores` | get, list, watch, create, update, patch, delete |
| `novastor.io` | `objectstores/status` | get, update, patch |
| `novastor.io` | `objectstores/finalizers` | update |
| `""` (core) | `persistentvolumes` | get, list, watch, create, update, patch, delete |
| `""` (core) | `nodes` | get, list, watch |
| `""` (core) | `events` | create, patch |
| `apps` | `deployments` | get, list, watch, create, update, patch, delete |
| `apps` | `statefulsets` | get, list, watch |
| `""` (core) | `services` | get, list, watch, create, update, patch, delete |
| `coordination.k8s.io` | `leases` | get, list, watch, create, update, patch, delete |

### CSI Driver

The CSI controller requires permissions to manage volumes and interact with the Kubernetes storage subsystem.

| API Group | Resources | Verbs |
|---|---|---|
| `""` (core) | `persistentvolumes` | get, list, watch, create, update, patch, delete |
| `""` (core) | `persistentvolumeclaims` | get, list, watch, update |
| `storage.k8s.io` | `storageclasses` | get, list, watch |
| `storage.k8s.io` | `csinodes` | get, list, watch |
| `storage.k8s.io` | `volumeattachments` | get, list, watch |
| `snapshot.storage.k8s.io` | `volumesnapshots` | get, list |
| `snapshot.storage.k8s.io` | `volumesnapshotcontents` | get, list, watch, create, update, patch, delete |
| `snapshot.storage.k8s.io` | `volumesnapshotclasses` | get, list, watch |
| `""` (core) | `events` | get, list, watch, create, update, patch |
| `""` (core) | `nodes` | get, list, watch |

### Node Agent

The agent DaemonSet requires privileged access to the host for device management:

| API Group | Resources | Verbs |
|---|---|---|
| `""` (core) | `nodes` | get |

The agent also requires the following security context:

```yaml
securityContext:
  privileged: true
hostPID: true
```

And host path mounts for `/dev`, `/sys`, and `/var/lib/novastor/chunks`.

## ClusterRole Example

If you manage RBAC separately (with `rbac.create: false`), create a ClusterRole and binding manually:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: novastor-controller
rules:
  # NovaStor CRDs
  - apiGroups: ["novastor.io"]
    resources:
      - storagepools
      - blockvolumes
      - sharedfilesystems
      - objectstores
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["novastor.io"]
    resources:
      - storagepools/status
      - blockvolumes/status
      - sharedfilesystems/status
      - objectstores/status
    verbs: ["get", "update", "patch"]
  - apiGroups: ["novastor.io"]
    resources:
      - storagepools/finalizers
      - blockvolumes/finalizers
      - sharedfilesystems/finalizers
      - objectstores/finalizers
    verbs: ["update"]
  # Core resources
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
  # Deployments for NFS/S3 gateways
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  # Leader election
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: novastor-controller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: novastor-controller
subjects:
  - kind: ServiceAccount
    name: novastor
    namespace: novastor-system
```

## Security Recommendations

1. **Principle of Least Privilege**: When managing RBAC manually, create separate ClusterRoles for the controller and CSI driver rather than a single combined role.

2. **Namespace Isolation**: Deploy NovaStor in a dedicated namespace (`novastor-system`) and restrict access to that namespace.

3. **ServiceAccount Tokens**: Disable automounting of ServiceAccount tokens for pods that do not need Kubernetes API access (e.g., the S3 gateway and NFS filer).

4. **Network Policies**: Restrict network access between components:
    - Only the controller should connect to the Kubernetes API
    - Only gateways and agents should connect to the metadata service
    - Only the CSI driver and gateways should connect to agents

5. **Pod Security Standards**: The agent DaemonSet requires `privileged` security context. All other components should run with `restricted` or `baseline` Pod Security Standards.
