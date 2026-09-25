# 001 - libm/libmvec sysroot divergence (local vs container)

## Summary

Container builds fail during C/C++ link steps with:

- `ld: cannot find /lib64/libm.so.6`
- `ld: cannot find /lib64/libmvec.so.1`

while equivalent local builds can pass.

The issue reproduces in tool-link targets (for example, protobuf and pigz link actions) and is not tied to one user target.

## What we found

1. The toolchain is Bootlin-based and includes glibc linker scripts in sysroot.
2. Some linker scripts reference absolute paths such as `/lib64/libm.so.6` and `/lib64/libmvec.so.1`.
3. In sandboxed/container execution, these absolute paths may resolve against host/container root instead of the intended sysroot, depending on linker/script resolution context.
4. Local environment can mask the problem if host paths happen to exist; container environment exposes it.
5. This creates local/container divergence and violates hermeticity expectations.

## Why this is a regression risk

- Any fix that depends on host filesystem presence (`/lib64/...`) is environment-sensitive.
- Environment-sensitive behavior can pass on one machine and fail on another.
- The failure appears in external/tool actions, so target-by-target fixes are fragile.

## Recommended fix approach (when resuming)

### 1) Establish a minimal reproducible failing link action

- Use one stable container-scoped target that triggers the link failure.
- Capture exact linker argv with `--verbose_failures` and sandbox diagnostics.

### 2) Make toolchain linker script handling deterministic

- Normalize relevant glibc linker scripts in the fetched toolchain sysroot so absolute references are sysroot-aware (for example `=/...` semantics), implemented idempotently.
- Ensure rewrite is deterministic and does not double-prefix on repeated fetches.

### 3) Force consistent sysroot injection on all linker paths

- Ensure both `gcc`-driven and direct `ld`-driven paths get the same absolute sysroot.
- Keep this in the toolchain layer (wrappers/tool paths), not in individual targets.

### 4) Remove host-conditional behavior

- Avoid logic that changes behavior based on host path existence.
- A hermetic toolchain should behave the same in local and container contexts.

### 5) Add a regression guard

- Add a small validation check that fails if unresolved absolute `libm/libmvec` linker-script paths reappear.

### 6) Validate with 4-step gate

1. Local scoped build (green)
2. Local full build (green)
3. Container scoped build (no `libm/libmvec` missing signature)
4. Container full build (no `libm/libmvec` missing signature)
