# Prettier

Available command: `bazelisk test //bazel/checks/prettier:prettier_test`\
Description: Fails if a git-tracked TypeScript file (`.ts`, `.tsx`) is not prettier-formatted; lists them. Manual and never cached. Add `--test_output=errors` to print the list in the terminal.

Available command: `bazelisk run //bazel/checks/prettier -- check [PATH...]`\
Description: Same check, printed directly. `PATH` is a file relative to `sdk/`; without it, every git-tracked TypeScript file is checked.

Available command: `bazelisk run //bazel/checks/prettier -- update [PATH...]`\
Description: Reformats the files in place, then lists the changed ones.

Runs prettier from `dev-tooling/pnpm-lock.yaml` on Bazel's Node.js with `.prettierrc.yaml`. Files matched by `.prettierignore` are skipped.
