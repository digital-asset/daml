# Making a Release

1. Make sure you have the latest master branch of the `daml` repository and
   create a new local branch from that.
   Bump the version number in the `VERSION` file.
   In `docs/source/support/release-notes.rst`, add a new header and label for
   the new version. (See previous releases as examples.)

   Retrieve the new release notes using the following command:

      `./unreleased.sh <revision range>`

   where `<revision range>` refers to all commits since the last release.
   For example, if the previous release was `v0.13.36` then use the range `v0.13.36..`
   to refer to all commits since that release tag.
   (See `man gitrevisions` for the full syntax of revision ranges.)

   This command outputs each change individually with its appropriate section.
   You need to group them into sections for the `release-notes.rst` file.
   (Again, see previous releases for an example.)

   The changelog may also specify edits to existing changelog additions,
   in which case they will be reported with the `WARNING` tag as in the following example:

       CHANGELOG_BEGIN

       WARNING: fix typo in entry "Adds new amdin API to upload DAR files" with the following.

       - [Sandbox] Adds new admin API to upload DAR files

       CHANGELOG_END

   You will need to manually incorporate such edits to the changelog.

   Once this is done, create a Github pull request (PR) with the above changes
   to the `VERSION` and `release-notes.rst` files.
   It is important that your PR changes exactly these two files.
   Both files must be modified, even if there are no additional release notes.

1. Get a review and approval on your PR and then merge it into master.

1. Once CI has passed for the corresponding master build, the release should be
   available on Bintray, Maven Central and GitHub and have a Git tag. The release
   should be visible on GitHub with _prerelease_ status, meaning it's not yet ready
   for production. The release notes should not be defined yet and will be adjusted
   later on. Maven central has a slight delay of around 20 minutes until the new
   version is visible.

1. Run through the following test plan on Linux or MacOS:

   1. Install the SDK using `curl -sSL https://get.daml.com/ | sh -s X.XX.XX`,
      where `X.XX.XX` is the new version number.
   1. Run `daml version --assistant=yes` and verify that the new version is
      selected as the assistant version and the default version for new projects.
   1. Create a new project with `daml new quickstart quickstart-java`
      and switch to it using `cd quickstart`.
   1. Run `daml start`. Your browser should be opened automatically at
      `http://localhost:7500`. Login as `Alice` and verify that there is
      1 contract and 3 templates. Close the tab and kill `daml
      start` using `Ctrl-C`.
   1. Run `daml build`.
   1. In 3 separate terminals (since each command will block) run
      1. `daml sandbox --port 6865 --scenario Main:setup .daml/dist/quickstart-0.0.1.dar`.
      1. `daml navigator server localhost 6865 --port 7500`
      1. `mvn compile exec:java@run-quickstart`
      > Note: It takes some time for our artifacts to be available on Maven Central. If you try running the above before the artifacts are available, you will get a "not found" error. Trying to build again _in the next 24h_ will result in:
      > ```
      > Failure to find ... was cached in the local repository, resolution will not be reattempted until the update interval of digitalasset-releases has elapsed or updates are forced
      > ```
      > This is Maven telling you it has locally cached that "not found" result and will consider it valid for 24h. To bypass that and force Maven to try the network call again, add a `-U` option, as in `mvn compile exec:java@run-quickstart -U`. Note that this is required to bypass your local cache of the failure; it will not be required for any user trying to run the quickstart after the artifacts have been published.
   1. Point your browser to `http://localhost:7500`,
      login as `Alice` and verify that there is 1 contract, 3 templates and 1 owned IOU.
   1. Check that `curl http://localhost:8080/iou` returns
      ```
      {"0":{"issuer":"EUR_Bank","owner":"Alice","currency":"EUR","amount":100.0000000000,"observers":[]}}
      ```
   1. Kill all processes.
   1. Run `daml studio --replace=always`.
      This should open the VSCode application and trigger the DAML extension
      bundled with the new SDK version.
      (The new VSCode extension will not be in the marketplace at this point.)
   1. Open `daml/Main.daml`
   1. Click on `Scenario results` above `setup` and wait for the scenario results
      to appear.
   1. Add `+` at the end of line 11, after `"Alice"` and confirm you get an
      error in line 12.
   1. Add `1` after the `+` and confirm you get an error in line 11.
   1. Delete the `+1`, and the `e` in `Alice` and verify that the scenario results
      are updated.
   1. Right click on `eurBank` in line 17 and verify that goto
      definition takes you to the definition in line 14.
   1. Close all files.

1. Run through the following test plan on Windows.
   This is slightly shortened to not make testing too annoying and
   since most issues are not platform specific.

   1. Download the Windows installer from `https://github.com/digital-asset/daml/releases`.
   1. Close any running SDK instance in PowerShell (Navigator or Sandbox)
   1. Run the installer. If asked if you want to remove an existing installation, click `yes`.
   1. Open a new Powershell.
   1. Run `daml new quickstart` to create a new project
      and switch to it using `cd quickstart`.
   1. Run `daml start`.
   1. Open your browser at `http://localhost:7500`, verify that you
      can login as Alice and there is one template and one contract.
   1. Kill `daml start` with Ctrl-C
   1. Run `daml studio --replace=always` and open `daml/Main.daml`.
   1. Verify that the scenario result appears within 30 seconds.
   1. Add `+` at the end of line 26 after `"Alice"` and verify that you get a red squiggly line.

1. If no issues are found, the release should be made public. To do so, go to
   [the releases page](https://github.com/digital-asset/daml/releases)
   and click on the `Edit` button for the relevant release. Take the
   combined release notes from `docs/source/support/release-notes.rst`
   for all releases since the last public release, convert them to
   markdown and insert them in the textbox, then uncheck the `This is
   a pre-release` checkbox at the bottom.
1. On the release PR, add the `Standard-Change` label.
1. Leave a comment like "All manual tests have passed" on the release PR on
   GitHub.
1. Finally, announce the release in the relevant Slack channels.
1. Documentation is published automatically once you make the release
   public on Github but you might have to wait up to an hour for the
   job to run.
