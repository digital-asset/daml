# daml-prebuilt image: disk cache fix (DONE)

## Problem discovered 2026-06-24

The baked disk cache had **zero cache hits** by default because Bazel's action cache keys include
paths from the output_base directory. The output_base path is computed as:
```
output_base = output_user_root / md5(workspace_path)
```

The image builds at `/opt/daml/sdk` → output_base `/opt/daml-build-out/48699ba...`, but sandboxes
mount workspaces at user-dependent paths like `/home/<user>/daml/sdk` → different hash → cache miss.

## Solution (IMPLEMENTED 2026-06-24)

**Use `--output_base` instead of `--output_user_root`.**

`--output_base` sets the EXACT path, bypassing the hash computation. `daml-bazel-prepare` now:
1. Detects daml-prebuilt mode (`/etc/daml-prebuilt.ref` exists)
2. Finds the baked output_base directory dynamically
3. Emits `startup --output_base=<exact-path>` instead of `startup --output_user_root=<parent>`

### Verification

```bash
cd /home/<user>/daml/sdk
eval "$(dev-env/bin/dade assist)"
daml-bazel-prepare
bazel build //compiler/damlc:damlc
# Result: disk cache hits regardless of workspace path
```

## Background: Why bind mounts were NOT the answer

The original workaround used bind mounts to make `/opt/daml` appear at runtime. This worked but:
- Required `sudo mount --bind`
- Made users work from `/opt/daml/sdk` instead of their natural cwd
- More complex setup

The `--output_base` approach is cleaner: it tells Bazel exactly where to find the baked build
artifacts, without needing to manipulate the filesystem.

## Files modified

1. `sbx-templates/docker/daml-prebuilt.Dockerfile` — §4 (daml-bazel-prepare script)
2. `sbx-templates/docker/daml-prebuilt-claude.md` — updated docs
