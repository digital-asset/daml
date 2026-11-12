# Git hooks

Available command: `bazelisk run //:setup_git_hooks -- [deploy] [fast|full]`\
Description: Deploys the `pre-commit` hook into the checkout's `.git/hooks`, shared by every worktree. `fast`, the default, runs `//bazel/checks:fast`: every check except hlint and vale, which take the longest. `full` runs all of `//bazel/checks`. Without a variant, a deployed hook keeps its own. Leaves a hook from elsewhere alone, and only reports a differing version of this one.

Available command: `bazelisk run //:setup_git_hooks -- update [fast|full]`\
Description: Replaces a differing version of the hook, including the other variant, keeping it as `pre-commit.bak`. Deploys the hook if there is none.

Available command: `bazelisk run //:setup_git_hooks -- remove`\
Description: Removes the hook, keeping a changed version as `pre-commit.bak` first. Leaves a hook from elsewhere alone.

The hook runs `bazelisk test <checks> --test_output=errors` from `sdk/` on every commit: about 5 seconds for `fast` and 40 for `full` once built. It checks the whole working tree, not only the staged changes, and fixes nothing: follow the failing check's `<name>-check.md`, or commit with `--no-verify`. Without `bazelisk` on PATH it blocks every commit. The setup refuses to run while `core.hooksPath` is set, because git then ignores `.git/hooks`. It recognises its own hook by the `//:setup_git_hooks` text in it, and the variant by its `checks=` line. `//:setup_git_hooks` is an alias of `//bazel/checks/git-hooks`.
