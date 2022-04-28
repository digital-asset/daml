# GHC-LIB

This setup builds `ghc-lib` and `ghc-lib-parser` entirely within Bazel,
including the generation of the Cabal sdists performed by `ghc-lib-gen`.

## Update

- To update the GHC revision used to build ghc-lib change the `GHC_REV`
  variable within `bazel_tools/ghc-lib/version.bzl`.
  If needed update the patches listed within `GHC_PATCHES`, see below.
- To update the GHC version used to build ghc-lib also update the `GHC_FLAVOR`
  and `GHC_LIB_VERSION` variables within `bazel_tools/ghc-lib/version.bzl`.
- To update the ghc-lib revision, which provides the `ghc-lib-gen` tool, change
  the `GHC_LIB_REV` and `GHC_LIB_SHA256` variables within
  `bazel_tools/ghc-lib/version.bzl` and update the patches in `GHC_LIB_PATCHES`
  if needed.

## Patches

- `ghc-daml-prim.patch` rename `ghc-prim` to `daml-prim`
    Patch generated with
    ```
    BASE=da-master-8.8.1
    git clone https://github.com/digital-asset/ghc.git
    cd ghc
    git checkout $BASE
    git merge --no-edit 833ca63be2ab14871874ccb6974921e8952802e
    git diff $BASE
    ```
- `ghc-hadrian.patch` patch Hadrian to build with the more recent GHC version
    used by the Daml repository. The patch is generated manually be removing
    all version constraints from Hadrian's Cabal file and changing the
    implementation to fix any GHC compiler errors.
- `ghc-lib-no-stack.patch` patch `ghc-lib-gen` to use a prebuilt Hadrian binary.
    With this change `ghc-lib-gen` no longer requires `stack`.
