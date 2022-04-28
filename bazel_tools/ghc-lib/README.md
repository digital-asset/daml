# GHC-LIB

This setup builds `ghc-lib` and `ghc-lib-parser` entirely within Bazel,
including the generation of the Cabal sdists performed by `ghc-lib-gen`.

The generated sdists are built by Bazel using `haskell_cabal_library` rules and
then included in the `stack_snapshot` rule for `@stackage` as
`vendored_packages`.

Note, `stack_snapshot`'s vendoring mechanism requries the Cabal files to be
sources files, i.e. it cannot reference generated Cabal files. Therefore, the
Cabal files are checked in and need to be updated when ghc-lib(-parser)
changes.

## Build

- To build the sdists of ghc-lib(-parser) use the following commands:
    ```
    $ bazel build @da-ghc//:ghc-lib-parser
    $ bazel build @da-ghc//:ghc-lib
    ```
- To build the Cabal libraries ghc-lib(-parser) use the following commands:
    ```
    $ bazel build //bazel_tools/ghc-lib/ghc-lib-parser
    $ bazel build //bazel_tools/ghc-lib/ghc-lib
    ```
    or alternatively you can use the aliases exposed by `stack_snapshot`'s
    `vendored_packages`.
    ```
    $ bazel build @stackage//:ghc-lib-parser
    $ bazel build @stackage//:ghc-lib
    ```
- To depend on ghc-lib(-parser) use the targets exposed by `stack_snapshot`:
    ```
    @stackage//:ghc-lib-parser
    @stackage//:ghc-lib
    ```

## Update

Note, an update of any of these may affect ghc-lib(-parser)'s Cabal files. If
so, see below for updating the checked in Cabal files.

- To update the GHC revision used to build ghc-lib change the `GHC_REV`
  variable within `bazel_tools/ghc-lib/version.bzl`.
  If needed update the patches listed within `GHC_PATCHES`, see below.
- To update the GHC version used to build ghc-lib also update the `GHC_FLAVOR`
  and `GHC_LIB_VERSION` variables within `bazel_tools/ghc-lib/version.bzl`.
- To update the ghc-lib revision, which provides the `ghc-lib-gen` tool, change
  the `GHC_LIB_REV` and `GHC_LIB_SHA256` variables within
  `bazel_tools/ghc-lib/version.bzl` and update the patches in `GHC_LIB_PATCHES`
  if needed.
- To update the checked in Cabal files execute the following Bazel commands.
    ```
    $ bazel run //bazel_tools/ghc-lib/ghc-lib-parser:cabal-update
    $ bazel run //bazel_tools/ghc-lib/ghc-lib:cabal-update
    ```

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
