# Making a Stable Release (2.x)

1. Go through the [checklist] before making the release. Note that
   the checklist is not available publicly.

1. Stable releases should be created from snapshot releases. On the
   [digital-asset/daml] repo, look up the commit sha for the snapshot you're
   building from, and add a line to the LATEST file that ties that sha to the
   corresponding stable version number. Add a `SPLIT_RELEASE` tag. For example:
   ```
   (echo "781a63f4353f1b39fe6d401c1567ff2766a3e78d 2.0.0 SPLIT_RELEASE"; cat LATEST) > LATEST.tmp
   mv LATEST.tmp LATEST
   ```
   Merge that PR, then wait for the `main` build to finish.

1. Reach out to the Canton team (`#team-canton on Slack`)
   and ask them to make a stable Canton release based on the split release you
   just made. Wait for them to tell you the release is ready on their side.

1. Open a PR on the [DACH-NY/assembly] repo targeting the Canton release that
   just got created.  See the instructions in the README of the
   [DACH-NY/assembly] repo for details.

1. The PR **must** be approved by a team lead before merging. As
   of this writing (2022-02-08), @bame-da, @gerolf-da, @cocreature,
   @ray-roestenburg-da or @adriaanm-da.

1. Merge the PR and wait for the corresponding `main` build to finish.

1. Go back to the general [release instructions](RELEASE.md).

[checklist]: https://docs.google.com/document/d/1RY2Qe9GwAUiiSJmq1lTzy6wu1N2ZSEILQ68M9n8CHgg
[DACH-NY/assembly]: https://github.com/DACH-NY/assembly
[digital-asset/daml]: https://github.com/digital-asset/daml
