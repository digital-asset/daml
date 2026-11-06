# scalafmt

Available command: `bazelisk test //bazel/checks/scalafmt:scalafmt_test`\
Description: Fails if a git-tracked Scala file (`.scala`, `.sc`) is not scalafmt-formatted; lists them. Manual and never cached. Add `--test_output=errors` to print the list in the terminal.

Available command: `bazelisk run //bazel/checks/scalafmt -- check [PATH...]`\
Description: Same check, printed directly. `PATH` is a Scala file relative to `sdk/`; without it, every git-tracked Scala file is checked.

Available command: `bazelisk run //bazel/checks/scalafmt -- update [PATH...]`\
Description: Reformats the files in place, then lists the changed ones. The pre-commit hook runs this.

Available command: `bazelisk run @scalafmt//:pin`\
Description: Re-resolves `bazel/checks/scalafmt/scalafmt_install.json` after a change to `SCALAFMT_VERSION` in `MODULE.bazel`. Contacts Maven Central.

Runs scalafmt-cli 3.5.1 with `.scalafmt.conf`, whose `project.excludePaths` apply. Keep `version` in `.scalafmt.conf` equal to `SCALAFMT_VERSION`; otherwise scalafmt downloads the configured version at run time.
