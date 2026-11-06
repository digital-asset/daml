# Vale

Available command: `bazelisk test //bazel/checks/vale:vale_test`\
Description: Fails if vale reports an error in a git-tracked `.rst` file under `docs/sharable/`; lists the alerts as `path:line:col`. Manual and never cached. Add `--test_output=errors` to print the list in the terminal.

Available command: `bazelisk run //bazel/checks/vale -- check [PATH...]`\
Description: Same check, printed directly. `PATH` is a file or directory relative to `sdk/`; without it, every git-tracked `.rst` file under `docs/sharable/` is checked. There is no `update`: fix the text, or adjust the rules in `.vale-ci.ini`.

Runs vale 3.4.2 (pinned in `bazel/versions/vale.version.bzl`) with `.vale-ci.ini` and `--minAlertLevel=warning`. Vale reads `.rst` through docutils' `rst2html`, pinned in `bazel/versions/docutils.version.bzl`. Takes about 30 seconds for all files.
