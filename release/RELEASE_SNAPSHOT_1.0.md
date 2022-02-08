# Making a Snapshot Release (1.x)

1. Start _from latest `main`_ and run
   ```
   ./release.sh new snapshot
   ```
   If you want to have a stricter control over the specific commit and version
   prefix the command can also be used as follows:
   ```
   ./release.sh snapshot <sha> <prefix>
   ```
   for example:
   ```
   $ ./release.sh snapshot cc880e2 0.1.2
   cc880e290b2311d0bf05d58c7d75c50784c0131c 0.1.2-snapshot.20200513.4174.0.cc880e29
   ```
   The former version of the command defaults to `HEAD` as the commit and tries
   to figure out the version prefix to use based on the first line of the `LATEST`
   file. This also mean that you will have to use the latter form for maintenance releases.
   Once the script has run, open a PR _to be merged to `main`_ (even if it's for a maintenance release)
   with the changed `LATEST` file, add the line produced by the `release.sh`
   invocation in a meaningful position (if you’re not sure, [semver](https://semver.org/) ordering is
   probably the right thing to do) and add the `Standard-Change` label. It
   is better to add such a label _before confirming the PR's creation_, else
   the associated CI check will fail and merging the PR will require you to
   re-run it after all the other ones have completed successfully.

1. Once the PR has built, check that it was considered a release build by our
   CI. If you are working from an automated PR, check that it sent a message to
   `#team-daml` to say it has finished building. If the PR was manually created,
   you can look at the output of the `check_for_release` build step.

1. Merge the PR and wait for the corresponding `main` build to finish. You
   will be notified on `#team-daml`.
