# Remove `os_info` and Use `ctx` as Source of Truth

## Goal

We want to remove `@os_info//:os_info.bzl` from the build graph and use repository-rule context (`ctx`) as the truth provider for host OS/CPU detection.

## Why

- `os_info` is a legacy compatibility layer from WORKSPACE-era setup.
- Bazel 8 migration work already relies on `ctx.os.name` / `ctx.os.arch` in active Bzlmod workflow.
- Keeping two host-detection paths increases maintenance cost and confusion.

## Target State

- No production code depends on `@os_info//:os_info.bzl`.
- Module extensions and repository rules derive host info directly from `ctx`.
- Target-level platform decisions are expressed via standard Bazel platform constraints where possible.

## Incremental Plan

1. Inventory all `load("@os_info//:os_info.bzl", ...)` usages and classify by type:
   - repository rule / module extension logic
   - BUILD/select logic
   - test-only logic
2. Replace repository-rule usage with local helpers based on `ctx.os.name` and `ctx.os.arch`.
3. For BUILD/Starlark target logic, migrate toward platform constraints (`@platforms//os`, `@platforms//cpu`) and remove boolean indirection.
4. Keep a temporary compatibility shim only where migration is blocked; delete once last caller is removed.
5. Remove `os_info` repository creation from workflow extensions after last consumer is gone.

## Definition of Done

- No `@os_info//:os_info.bzl` loads remain in the active SDK Bzlmod path.
- `bazelisk build --nobuild //...` passes with no `os_info` dependency in module graph.
- Legacy WORKSPACE compatibility paths are either migrated or explicitly documented as deprecated with a removal date.
