# daml-prebuilt: Disk cache path mismatch (RESOLVED)

> **STATUS:** Fixed in `daml-bazel-prepare`. The script now uses `--output_base` (exact path) instead
> of `--output_user_root` when in daml-prebuilt mode, so cache hits work from any workspace path.

## The problem (historical)

Bazel's disk cache stores actions by a **hash of the action definition**. The action definition includes:
- The command to run
- The input files (by content hash)
- Environment variables
- **But also**: certain generated files that contain **absolute paths**

The culprit is primarily **runfiles manifests**. When Bazel builds, it creates manifest files that list symlinks for runtime dependencies. These manifests contain **absolute paths** like:

```
# During image build:
/opt/daml/sdk/bazel-out/k8-opt/bin/some/target.runfiles/...
/opt/daml-build-out/48699ba.../execroot/...

# At runtime:  
/home/roger.bosman/daml/sdk/bazel-out/k8-opt/bin/some/target.runfiles/...
/var/lib/docker/daml-bazel-out/d2bc5e3.../execroot/...
```

These manifests are **inputs** to subsequent actions. Since the paths differ, the **content** of the manifest differs, which changes the **content hash**, which changes the **action hash**, which causes a **cache miss**.

## What's NOT the problem

- The disk cache location (`/opt/daml-bazel-cache/disk`) - this is fine, same at build and runtime
- The source file content - identical
- The nix toolchain paths - identical (same `/nix/store/...` paths)

## What IS the problem

Two paths that get embedded into intermediate build artifacts:

1. **Workspace path**: `/opt/daml/sdk` vs `/home/<user>/daml/sdk`
2. **Output base path**: `/opt/daml-build-out/<hash>/` vs `<runtime-output-root>/<hash>/`

Both of these end up in manifests, args files, and other generated inputs that then change action hashes.

The output base hash is computed from the workspace path:
```
md5("/opt/daml/sdk") = 48699ba50040a691b9e1ada81fb8ade4
md5("/home/roger.bosman/daml/sdk") = d2bc5e364f3f82cbea0c650e3e424d0d
```

## Tested workaround (works but impractical)

Bind-mounting and matching paths gives cache hits:

```bash
sudo mkdir -p /opt/daml /opt/daml-build-out
sudo mount --bind /home/<user>/daml /opt/daml
sudo chown agent /opt/daml-build-out

# Update .bazelrc.local to use:
startup --output_user_root=/opt/daml-build-out

# Work from bind-mounted path
cd /opt/daml/sdk
bazel build //compiler/damlc:damlc
```

Result: **6554 disk cache hits** (64%), only 203 rebuilds (2%)

**Why impractical**: Claude runs from the mounted workspace folder (`/home/<user>/daml`), so we can't just work from `/opt/daml`.

## The fix (implemented)

**Use `--output_base` instead of `--output_user_root`.**

`--output_user_root` sets the parent directory where Bazel computes a hash subdirectory based on
workspace path. `--output_base` sets the EXACT path, bypassing the hash computation.

`daml-bazel-prepare` now:
1. Detects daml-prebuilt mode (`/etc/daml-prebuilt.ref` exists)
2. Finds the baked output_base directory dynamically (`find /opt/daml-build-out -name '[0-9a-f]*'`)
3. Emits `startup --output_base=<exact-path>` instead of `startup --output_user_root=<parent>`

This ensures cache hits regardless of where the workspace is mounted.

## Other approaches considered (not needed)

1. **Build the image from the same path structure that runtime will use** - but that varies per user (`/home/alice/...`, `/home/bob/...`)

2. **Find a way to make Bazel NOT embed absolute paths** - Bazel has `--experimental_output_paths=strip` but it only strips config paths (like `k8-opt`), NOT workspace paths. GitHub issue #2998 (open since 2017) tracks this.

3. **Accept that disk cache won't work and rely on remote cache** - the remote cache IS path-independent. If `bazel-cache.da-ext.net` is reachable at runtime, actions will hit the remote cache instead.

4. **Request sbx feature**: Custom workspace mount path (`sbx run --workspace-path=/opt/daml`) would let us match the image build path.

## Key evidence from testing

Build log shows paths embedded in cache lookups:
```
warning: [path] bad path element "/opt/daml-build-out/48699ba.../execroot/.../scala-reflect.jar": no such file or directory
```

This confirms actions are looking for files at the image-build-time paths, not runtime paths.

## Files involved

- Dockerfile: `sbx-templates/docker/daml-prebuilt.Dockerfile`
- Verification doc: `sbx-templates/docs/daml-prebuilt-verify.md`
- This file: `sbx-templates/docker/daml-prebuilt-CACHE-ISSUE.md`
