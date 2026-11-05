# HLint

Available command: `bazelisk test //bazel/checks/hlint:hlint_test`\
Description: Fails if hlint reports a hint in a git-tracked Haskell file; lists them as `path:line:col`. Manual and never cached. Add `--test_output=errors` to print the list in the terminal.

Available command: `bazelisk run //bazel/checks/hlint -- check [PATH...]`\
Description: Same check, printed directly. `PATH` is a file or directory relative to `sdk/`; without it, every git-tracked `.hs`/`.lhs` file is checked. There is no `update`: fix the code, or ignore the hint in `.hlint.yaml`.

Available command: `bazelisk run @hlint_stackage-unpinned//:pin`\
Description: Re-resolves `bazel/haskell/hlint_snapshot.json` after a change to `bazel/versions/hlint.version.bzl`. Downloads the Hackage index.

Builds hlint 3.5 from source with the repo's GHC 9.0.2 (Stackage `lts-19.33`), pinned in `bazel/versions/hlint.version.bzl`; hlint 3.6 and later need GHC 9.2. The first build takes several minutes. Linux and macOS, x86_64 and arm64; not Windows. The `<target>@hlint` build targets use the same binary.
