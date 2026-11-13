# pnpm lockfiles

Available command: `bazelisk test //bazel/checks/pnpm:pnpm_test`\
Description: Fails if a git-tracked `pnpm-lock.yaml` under `sdk/` is not up to date with its `package.json` files; lists them with pnpm's reason. Manual and never cached. Add `--test_output=errors` to print the list in the terminal.

Available command: `bazelisk run //bazel/checks/pnpm -- check`\
Description: Same check, printed directly.

Available command: `bazelisk run //bazel/checks/pnpm -- update`\
Description: Updates the lockfiles in place (`pnpm install --lockfile-only`, no `node_modules`), then lists the changed ones. May contact the npm registry.

Uses the Bazel-provided pnpm (`@pnpm`). Skipped: `language-support/js/codegen/tests/ts/pnpm-lock.yaml`, which is resolved at test time.
