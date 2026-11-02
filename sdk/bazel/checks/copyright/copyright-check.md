# Copyright headers

Available command: `bazel test //bazel/checks/copyright:copyright_test`\
Description: Fails if a git-tracked file under `sdk/` has a missing or out-of-date header; lists them as `path:line`. Manual and never cached. Add `--test_output=errors` to print the list in the terminal.

Available command: `bazel run //bazel/checks/copyright -- check [DIRECTORY]`\
Description: Same check, printed directly. `DIRECTORY` is relative to `sdk/`.

Available command: `bazel run //bazel/checks/copyright -- update [DIRECTORY]`\
Description: Adds missing headers and replaces out-of-date ones in place, then lists the changed files.

Skipped: directories containing `NO_AUTO_COPYRIGHT`, `pnpm-lock.yaml`, symlinks, unsupported file types.
