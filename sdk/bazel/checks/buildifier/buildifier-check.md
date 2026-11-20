# Buildifier

Available command: `bazelisk test //bazel/checks/buildifier:buildifier_test`\
Description: Fails if a Starlark file under `sdk/` is not buildifier-formatted (formatting only, no lint). Part of `//...`, so CI runs it. Never cached. Add `--test_output=errors` to print the diff in the terminal.

Available command: `bazelisk run //bazel/checks/buildifier:check`\
Description: Same check, printed directly as a diff.

Available command: `bazelisk run //bazel/checks/buildifier:update`\
Description: Reformats the files in place. The pre-commit hook runs this.

Covers every Starlark file on disk, untracked ones included. Skipped: `.bazelbsp/`.
