# Platform independence

Available command: `bazelisk test //bazel/checks/platform-independence:platform-independence_test`\
Description: Fails if the file hashes of `//compiler/damlc/tests:platform-independence.dar` differ from `compiler/damlc/tests/platform-independence.dar-hash`; shows the diff. Part of `//...`, so CI runs it on every platform. Cached. Skipped unless the SDK version is `0.0.0`.

Available command: `bazelisk run //bazel/checks/platform-independence -- check`\
Description: Same check, printed directly.

Available command: `bazelisk run //bazel/checks/platform-independence -- update`\
Description: Rewrites `platform-independence.dar-hash` from the current build. Only for intended changes to the DAR (compiler, daml-prim, daml-stdlib); a mismatch on a single platform means the output is platform-dependent.

The pre-commit hook runs the check only with `RUN_PLATFORM_INDEPENDENCE_CHECK=yes`, because it builds damlc.
