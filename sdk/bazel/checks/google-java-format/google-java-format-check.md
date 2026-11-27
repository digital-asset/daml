# google-java-format

Available command: `bazelisk test //bazel/checks/google-java-format:google-java-format_test`\
Description: Fails if a git-tracked Java file is not google-java-format-formatted; lists them. Manual and never cached. Add `--test_output=errors` to print the list in the terminal.

Available command: `bazelisk run //bazel/checks/google-java-format -- check [PATH...]`\
Description: Same check, printed directly. `PATH` is a Java file relative to `sdk/`; without it, every git-tracked `.java` file is checked.

Available command: `bazelisk run //bazel/checks/google-java-format -- update [PATH...]`\
Description: Reformats the files in place, then lists the changed ones. The pre-commit hook runs this.

Runs google-java-format 1.9 (pinned in `bazel/versions/google_java_format.version.bzl`) on Bazel's remote JDK 11; 1.9 fails on JDK 16 and later.
