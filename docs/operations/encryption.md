# Encryption at Rest

NovaStor supports encryption at rest to protect stored data. This page covers the available key management options, how to enable encryption, and key rotation procedures.

## Overview

Encryption at rest protects chunk data stored on disk. When enabled, every chunk is encrypted before being written to the local store and decrypted when read. Encryption is transparent to the access layers (block, file, object) -- they interact with plaintext data while the chunk engine handles encryption internally.

### Encryption Properties

| Property | Value |
|---|---|
| Algorithm | AES-256-GCM |
| Key size | 256 bits (32 bytes) |
| Nonce | Randomly generated per chunk |
| Authentication | GCM provides authenticated encryption (integrity + confidentiality) |
| Scope | Per-chunk encryption (each chunk has a unique nonce) |

## Key Management Options

NovaStor supports three key management approaches, selected via the `--encryption-key-source` flag on the Node Agent.

### Static Key

A fixed encryption key provided directly or via an environment variable. Suitable for development and testing.

```bash
novastor-agent \
  --encryption-key-source=static \
  --encryption-key="base64-encoded-32-byte-key"
```

Or via environment variable:

```bash
export NOVASTOR_ENCRYPTION_KEY="base64-encoded-32-byte-key"
novastor-agent --encryption-key-source=static
```

Generate a key:

```bash
openssl rand -base64 32
```

!!! warning "Static Key Limitations"
    The static key approach stores the key in memory and configuration. It does not support key rotation without re-encrypting all chunks. Use only for development or environments where a KMS is not available.

### Derived Key

A key derived from a passphrase using Argon2id key derivation. The passphrase is provided via environment variable, and a per-node salt is stored alongside the chunk data.

```bash
export NOVASTOR_ENCRYPTION_PASSPHRASE="your-strong-passphrase"
novastor-agent --encryption-key-source=derived
```

| Parameter | Value |
|---|---|
| KDF | Argon2id |
| Memory | 64 MiB |
| Iterations | 3 |
| Parallelism | 4 |
| Salt | 16 bytes, randomly generated per node, stored in `<data-dir>/encryption-salt` |

### File-Based Key

The encryption key is read from a file on disk. This approach integrates well with Kubernetes Secrets mounted as volumes or with external secret management systems (Vault, AWS Secrets Manager, etc.).

```bash
novastor-agent \
  --encryption-key-source=file \
  --encryption-key-file=/etc/novastor/encryption.key
```

#### Kubernetes Secret Example

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: novastor-encryption-key
  namespace: novastor-system
type: Opaque
data:
  encryption.key: <base64-encoded-32-byte-key>
```

Mount the secret in the agent DaemonSet via Helm values or a patch:

```yaml
agent:
  extraVolumes:
    - name: encryption-key
      secret:
        secretName: novastor-encryption-key
  extraVolumeMounts:
    - name: encryption-key
      mountPath: /etc/novastor
      readOnly: true
```

## Enabling Encryption

### New Deployments

For new deployments, enable encryption during initial setup by configuring the agent with the appropriate key source:

```yaml
# values.yaml
agent:
  extraArgs:
    - --encryption-key-source=file
    - --encryption-key-file=/etc/novastor/encryption.key
```

All chunks written after enabling encryption will be encrypted. The chunk engine stores a header flag indicating whether each chunk is encrypted, so mixed encrypted/unencrypted stores are supported during migration.

### Existing Deployments

To enable encryption on an existing deployment:

1. Configure the encryption key on all agents
2. Restart the agents with `--encryption-key-source` set
3. New chunks will be encrypted; existing chunks remain unencrypted
4. Run a background migration to re-encrypt existing chunks (planned for a future release)

!!! note "Migration"
    Full online re-encryption of existing chunks is planned for a future release. Until then, new chunks are encrypted while existing chunks remain in plaintext. Both types coexist transparently.

## Key Rotation

Key rotation replaces the active encryption key with a new one. The process differs by key source.

### File-Based Key Rotation

1. Generate a new encryption key
2. Update the Kubernetes Secret with the new key
3. The agent periodically checks the key file for changes (based on `--tls-rotation-interval`, default 5 minutes)
4. New chunks are encrypted with the new key
5. Existing chunks remain encrypted with the old key until they are re-written (e.g., during scrub-and-rewrite or migration)

```bash
# Generate new key
openssl rand -base64 32 > new-key.txt

# Update the Kubernetes Secret
kubectl -n novastor-system create secret generic novastor-encryption-key \
  --from-file=encryption.key=new-key.txt \
  --dry-run=client -o yaml | kubectl apply -f -

# Verify agents picked up the new key (check agent logs)
kubectl -n novastor-system logs -l app.kubernetes.io/component=agent --tail=20
```

### Derived Key Rotation

1. Set the new passphrase in the environment variable
2. Restart all agent pods (rolling restart via DaemonSet)
3. A new salt is generated, and all new chunks use the new derived key

```bash
# Update the passphrase secret
kubectl -n novastor-system create secret generic novastor-encryption-passphrase \
  --from-literal=passphrase="new-strong-passphrase" \
  --dry-run=client -o yaml | kubectl apply -f -

# Rolling restart of agents
kubectl -n novastor-system rollout restart daemonset/novastor-agent
```

## Verifying Encryption

Check that encryption is active by examining agent logs at startup:

```
INFO  encryption enabled  {"keySource": "file", "keyFile": "/etc/novastor/encryption.key"}
```

Verify at the storage level by inspecting raw chunk files -- encrypted chunks contain a magic header (`NVSE`) followed by the nonce and ciphertext, making them unreadable without the key.

## Security Considerations

1. **Key Storage**: Never store encryption keys in version control, container images, or environment variables visible in pod specs. Use Kubernetes Secrets or an external KMS.

2. **Key Backup**: Always maintain a secure backup of your encryption key. If the key is lost, encrypted data is unrecoverable.

3. **Memory Protection**: Encryption keys are held in process memory during the agent's lifetime. Consider using memory-locked allocations (`mlock`) in high-security environments.

4. **Transport Security**: Encryption at rest protects data on disk. For data in transit, enable mTLS between components (see `--tls-ca`, `--tls-cert`, `--tls-key` flags).

5. **Metadata**: Metadata (volume names, chunk IDs, placement maps) is not encrypted. Only chunk data content is encrypted.
