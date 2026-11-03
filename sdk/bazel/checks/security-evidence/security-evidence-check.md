# Security evidence

Available command: `bazelisk test //bazel/checks/security-evidence:security-evidence_test`\
Description: Fails if `security-evidence.md` is out of date with the `TEST_EVIDENCE` markers in git-tracked files under `sdk/`; shows the diff. Manual and never cached. Add `--test_output=errors` to print the diff in the terminal.

Available command: `bazelisk run //bazel/checks/security-evidence -- check`\
Description: Same check, printed directly.

Available command: `bazelisk run //bazel/checks/security-evidence -- update`\
Description: Regenerates `security-evidence.md` and prints the diff.

Marker format: `TEST_EVIDENCE: <Category>: <description>` on one line; every category needs at least one. Skipped: `bazel/checks/security-evidence/`.
