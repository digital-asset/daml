# Making a Stable Release (1.x)

1. Go through the [checklist] before making the release. Note that
   the checklist is not available publicly.

1. Stable releases are promoted from snapshot releases. Open a PR
   that changes the `LATEST` file to remove the `-snapshot` suffix on the
   corresponding snapshot, and add the `Standard-Change` label.

1. Once the PR has built, check that it was considered a release build by our
   CI. If you are working from an automated PR, check that it sent a message to
   `#team-daml` to say it has finished building. If the PR was manually created,
   you can look at the output of the `check_for_release` build step.

1. The PR **must** be approved by a team lead before merging. As
   of this writing (2022-02-08), @bame-da, @gerolf-da, @cocreature,
   @ray-roestenburg-da or @adriaanm-da.

1. Merge the PR and wait for the corresponding `main` build to finish. You
   will be notified on `#team-daml`.

1. Go back to the general [release instructions](RELEASE.md).

[checklist]: https://docs.google.com/document/d/1RY2Qe9GwAUiiSJmq1lTzy6wu1N2ZSEILQ68M9n8CHgg
