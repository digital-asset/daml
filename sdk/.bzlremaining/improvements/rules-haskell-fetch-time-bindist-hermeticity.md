# rules_haskell Fetch-Time Bindist Hermeticity

## Context

This workspace uses `rules_haskell@1.0` with bindist toolchains and fixed GHC version `9.0.2` via:

- `haskell_toolchains_ext.bindists(version = "9.0.2")`

In bindist mode, `rules_haskell` provisions GHC with a repository rule (`ghc_bindist`) that runs `./configure` and `make install` during repository fetch/evaluation.

## Current Issue

The current bindist provisioning path is not hermetic at fetch-time:

1. It executes host tools from ambient `PATH` (`make`, compiler toolchain, shell utilities).
2. During `make install`, `ghc-pkg` is executed and can require host runtime libraries such as `libtinfo.so.5`.
3. Failures happen before normal Bazel action-phase toolchain resolution, so action-time hermetic toolchain wiring cannot help.

As a result, this path conflicts with hermetic toolchain requirements ("no ambient host toolchain fallback" and deterministic fetch behavior).

## Target End-State (Perfect World)

`rules_haskell` bindist provisioning should be redesigned so repository evaluation remains declarative and side-effect free:

1. Repository rules only download/verify/extract immutable artifacts.
2. Any configure/install/registration work is performed by normal Bazel actions with declared inputs, tools, and environment.
3. Toolchain registration consumes produced action outputs (or a fully relocatable prebuilt layout), not host-mutated fetch-time trees.
4. Runtime dependencies required by bindist executables (such as terminfo/tinfo) are provided by declared Bazel artifacts, never host system packages.
5. Platform-specific runtime/linker behavior is encoded in rule/toolchain metadata, not inferred from ambient host state.

In this end-state, bindist setup is reproducible across local/CI/container environments and does not depend on host `PATH`, host C toolchain discovery, or host shared library availability.

## Why This Improvement Exists

- It isolates a blocker class distinct from Bazel 8 API compatibility patches.
- It documents that failures are architectural (execution phase boundary), not only missing local packages.
- It defines the desired upstream-quality destination for bindist provisioning.

## TODO

- Create/track upstream issue(s) for bindist execution-phase redesign and runtime dependency declaration.
- Specify the target provider/contracts needed so toolchains can consume action-produced bindist layouts.
- Replace fetch-time install logic with action-phase installation/normalization flow.
- Remove all ambient host assumptions from bindist provisioning (`PATH`, host compiler, host shared libs).
