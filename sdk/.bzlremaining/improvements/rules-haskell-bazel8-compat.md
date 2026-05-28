# rules_haskell Bazel 8 Compatibility

## Context

We upgraded to `rules_haskell@1.0` while keeping project GHC version unchanged (`9.0.2` via `haskell_toolchains_ext.bindists(version = "9.0.2")`).

In this workspace, Bazel 8 compatibility is not fully upstream-clean yet, so we currently rely on:

- `single_version_override(module_name = "rules_haskell", patches = ["//bazel/patches:haskell/rules_bazel-8_compat.patch"])`

## Current Issue

Observed failure classes without downstream compatibility handling include:

1. `get_cpu_value` loads from `@bazel_tools//tools/cpp:lib_cc_configure.bzl` (removed in Bazel 8).
2. Python toolchain symbols expected under `@bazel_tools//tools/python` instead of `@rules_python`.
3. Asterius extension mismatch with `aspect_rules_js@2.x` (`@aspect_rules_js//npm:npm_import.bzl` no longer exists).

Related upstream tracker:

- https://github.com/tweag/rules_haskell/issues/2426

## Why We Keep the Patch For Now

- It unblocks Bazel 8 migration work in this repo without changing GHC version.
- Forcing `aspect_rules_js@1.x` is not viable here because it conflicts with existing JS setup (pnpm lockfile/tooling expectations).

## TODO

- Re-check latest `rules_haskell` release and issue updates.
- Drop `rules_bazel-8_compat.patch` when upstream support is sufficient for this workspace.
- Keep this aligned with `sdk/MODULE.bazel` overrides and migration status notes.
