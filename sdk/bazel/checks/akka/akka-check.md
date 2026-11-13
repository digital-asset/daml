# Akka ban

Available command: `bazelisk test //bazel/checks/akka:akka_test`\
Description: Fails if a checked Maven lockfile mentions `akka`; lists the lines as `path:line`. Part of `//...`, so CI runs it. Cached. Add `--test_output=errors` to print the list in the terminal.

Available command: `bazelisk run //bazel/checks/akka -- check`\
Description: Same check, printed directly. There is no `update`: replace the dependency with Apache Pekko (`org.apache.pekko`).

Checked lockfiles: `_LOCKFILES` in `BUILD.bazel` (now `maven_install_2.13.json`).
