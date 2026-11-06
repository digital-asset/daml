# All checks

Available command: `bazelisk test //bazel/checks`\
Description: Runs every check test under `bazel/checks/` in parallel, manual ones included. Add `--test_output=errors` to print the failing checks' output in the terminal. Manual itself, so `//...` does not run it.

Fixes are per check: see the `update` commands in each `<name>-check.md`.
