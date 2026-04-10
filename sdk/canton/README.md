This directly defines the version and hash of the Canton repository that is to
be built and used in the Daml repo when building the SDK and tests.

## Updating the Canton version

The Canton version to be used is specified in `canton/canton_version.bzl`. This
version is used by build definitions inside `canton/canton_jar.bzl` and
`canton/BUILD.bazel` to determine which version of the Canton app's JAR to pull
in from DA's docker images.

### The normal way

Normally, you shouldn't modify this file manually. Update the Canton to the
latest available version by running `./ci/refresh-canton.sh` with no
arguments:

```
./ci/refresh-canton.sh
```

Update Canton to specific version with `./ci/refresh-canton.sh <version>`, e.g.

```
./ci/refresh-canton.sh 3.5.0-ad-hoc.20260320.18390.0.vd311a797
```

These will update `canton/canton_version.bzl` with the correct version and hash.

### Consuming a remote Canton branch in Daml

If you want to build Daml against a specific commit in Canton, you'll need to
start by publishing all of the libraries and executables for Canton from that
branch.

* Open a PR for the commit, go to the CircleCI for that commit.
* In the top left, click "Trigger Pipeline" and select "snapshot-release" in the
  dropdown to the right of "manual_job".
* The manual_snapshot_release workflow will take about 30 minutes to complete -
  when it's done, the logs for its publish_release job will report the new
  adhoc version produced as part of the "Publish to OCI Registry" task,
  something of the form `3.X.Y-ad-hoc.YYYYMMDD.XXXXX.X.v<commit>`, e.g.
  `3.5.0-ad-hoc.20260320.18390.0.vd311a797`.
* Supply the new version to `./ci/refresh-canton.sh`, e.g.

### Consuming a local Canton repo in your local Daml repo

To avoid needing to publish a full Canton, the Daml repo can be made to use
local artifacts from a local Canton repo.

* In `canton/canton_version.bzl`, set `LOCAL_CANTON_OVERRIDE` to an absolute
path to the Canton repo in question.
* Run `./sdk/canton/pull-local-canton-to-daml.sh -v --all` - the script will
navigate to the local Canton repo, build all of the artifacts, publish them to
the local Maven cache, and update the Daml repo to use the local artifacts.
* Run any bazel rule that uses Canton, it will pick up the new canton libraries
and the new Canton executable.

For quicker iteration, if you are working on a specific Canton subproject that
you need to consume in Daml, you may go to the local Canton directory and run
`sbt <name_of_subproject> / publishM2`, and then rerun
`./sdk/canton/pull-local-canton-to-daml.sh -v --repin` in the Daml repo.

### Using your local Daml changes in a local Canton repo

**WARNING**: Publishing artifacts in this direction is dangerous - it overwrites
the Daml instance globally for the version used by your remote Canton repo. If
you run this script, all uses of that compiler version on your system will
instead use your 0.0.0 artifacts. Once you're done using this to develop your
changes, clean out your DPM with --nuke.

In the opposite direction, the Canton repo sometimes uses the Daml compiler or
the Daml script service to build test DARs and run tests. The Canton repo
expects to use the Daml repo via a hardcoded version in
`project/project/DamlVersions.scala`, which it then supplies to DPM. The
quickest option to get our local Daml into a Canton repo, then, is to overwrite
the Daml artifacts that DPM uses at that version.

Run the `./canton/push-daml-to-local-canton.sh`, which will:
* Scrape the Daml version that the local Canton repo is expecting.
* Overwrite that version's DPM artifacts with our local Damlc, Codegen, and
  daml-script artifacts.
