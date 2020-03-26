# Making a Release

First, you need to decide whether you are making a technical snapshot
("prerelease" at the github level, hereafter "snapshot") or an officially
supported release (hereafter "stable release").  For the latter, there are
extra steps marked as **[STABLE]** in the following instructions. You have to
skip those if you are making a snapshot release, that is, one intended mostly
for internal use and early testing, but which makes no promises about future
compatibility.

In either case, before going through the following list, you need to know which
commit you want to create the release from, `$SHA`. For a stable release, it is
highly recommended that this be the same commit as the latest existing
snapshot, so we "bless" an existing, tested version of the SDK rather than try
our luck with a random new one. For a snapshot, this should generally be the
latest commit on master.

1. **[STABLE]** Coordinate with the product and marketing teams to define
   release highlights, tweets, blog posts, as well as timeline for publishing
   the release. Define a version number, `$VERSION`. The following command may
   be useful as a starting point; it will list all changes between the previous
   stable release and the latest snapshot release:

   ```
   ./release.sh changes stable latest
   ```

1. Pull the latest master branch of the `daml` repository and create a new,
   clean branch off it.

   - For a snapshot, run `./release.sh snapshot HEAD`.
     - If applicable (e.g. latest release was a stable one), edit `LATEST` to
       update the release "version". For example, change:
       ```
       6ea118d6142d2a937286b0a7bf9846dbcdb1751b 0.13.56-snapshot.20200318.3529.0.6ea118d6
       ```
       to:
       ```
       6ea118d6142d2a937286b0a7bf9846dbcdb1751b 0.13.57-snapshot.20200318.3529.0.6ea118d6
       ```
   - For a stable release, run `echo "$SHA $VERSION" > LATEST`.
     - Ideally, for a stable release, the resulting change is only to cut off
       the prerelease part of the version number (the `-snapshot...`).

1. **[STABLE]** In `docs/source/support/release-notes.rst`, add a new header
   and label for the new version. (See previous releases as examples.)

   Retrieve the new release notes using the command:

    ```
    ./unreleased.sh $LAST_VERSION..$SHA
    ```

   where `$LAST_VERSION` is the previous stable version. (See `man gitrevisions`
   for the full syntax of revision ranges.)

   This command outputs each change individually with its appropriate section.
   You need to group them into sections in the `release-notes.rst` file.
   (Again, see previous releases for examples.)

   The changelog may also specify edits to existing changelog additions.
   These are reported with the `WARNING` tag, for example:

       CHANGELOG_BEGIN

       WARNING: fix typo in entry "Adds new amdin API to upload DAR files" with the following.

       - [Sandbox] Adds new admin API to upload DAR files

       CHANGELOG_END

   You will need to manually incorporate such edits to the changelog.

1. Once this is done, create a GitHub pull request (PR) with the above changes
   to the `LATEST` and (for a stable release) `release-notes.rst` files.
   It is important that your PR changes no other file.

1. Get a review and approval on your PR and then merge it into master.
   **[STABLE]** For a stable release, the approval **MUST** be from the team
   lead of the Language, Runtime or Product team.

1. Once the CI checks have passed for the corresponding master build, the release
   should be available on Bintray, Maven Central and GitHub, and have a Git tag.
   The release should be visible on GitHub with _prerelease_ status, meaning it's
   not yet ready for users to consume. The release notes should not be defined yet
   and will be adjusted later on. Maven central has a delay of around 20 minutes
   until the new version is visible. (**Note:** The 20-minute delay is for
   artifacts to be available through e.g. `mvn build`; the delay for artifacts to
   show up in web searches on the Maven Central website is up to two hours. Do not
   worry if the artifacts do not show on the website yet.)

1. Run through the following test plan on one of Linux or macOS:

   1. Install the new SDK using:
      ```
      curl -sSL https://get.daml.com/ | sh -s $(cat LATEST | gawk '{print $2}')
      ```
      Note: this assumes you have the up-to-date `LATEST` file, either because
      you just checked out master or because you're still on the release PR
      commit.

   1. Run `daml version --assistant=yes` and verify that the new version is
      selected as the assistant version and the default version for new projects.

   1. Create a new project with `daml new quickstart quickstart-java`
      and switch to it using `cd quickstart`.

   1. Verify the new version is specified in `daml.yaml` as the `sdk-version`.

   1. Run `daml start`. Your browser should be opened automatically at
      `http://localhost:7500`. Login as `Alice` and verify that there is
      1 contract and 3 templates. Close the tab and kill `daml start` using
      `Ctrl-C`.

   1. Run `daml build`.

   1. In 3 separate terminals (since each command except for `daml script …`
      will block), run:

      1. `daml sandbox --wall-clock-time --port 6865 .daml/dist/quickstart-0.0.1.dar`
      1. `daml script --dar .daml/dist/quickstart-0.0.1.dar --script-name Setup:initialize --ledger-host localhost --ledger-port 6865 --wall-clock-time`
      1. `daml navigator server localhost 6865 --port 7500`
      1. `mvn compile exec:java@run-quickstart`

      > Note: It takes some time for our artifacts to be available on Maven
      > Central. If you try running the last command before the artifacts are
      > available, you will get a "not found" error. Trying to build again _in
      > the next 24 hours_ will result in:
      >
      > ```
      > Failure to find ... was cached in the local repository, resolution will not be reattempted until the update interval of digitalasset-releases has elapsed or updates are forced
      > ```
      >
      > This is Maven telling you it has locally cached that "not found" result
      > and will consider it valid for 24h. To bypass that and force Maven to
      > try the network call again, add a `-U` option, as in
      > `mvn compile exec:java@run-quickstart -U`. Note that this is required to
      > bypass your local cache of the failure; it will not be required for a
      > user trying to run the quickstart after the artifacts have been
      > published.

   1. Point your browser to `http://localhost:7500`, login as `Alice` and verify
      that there is 1 contract, 3 templates and 1 owned IOU.

   1. Check that `curl http://localhost:8080/iou` returns:
      ```
      {"0":{"issuer":"EUR_Bank","owner":"Alice","currency":"EUR","amount":100.0000000000,"observers":[]}}
      ```

   1. Kill all processes.

   1. Run `daml studio --replace=always`. This should open VSCode and trigger
      the DAML extension that's bundled with the new SDK version. (The new
      VSCode extension will not be in the marketplace at this point.)

   1. Open `daml/Main.daml`.

   1. Click on `Scenario results` above `setup` and wait for the scenario
      results to appear.

   1. Add `+` at the end of line 12, after `"Alice"` and confirm you get an
      error in line 13.

   1. Add `1` after the `+` and confirm you get an error in line 12.

   1. Delete the `+1` and the `e` in `Alice` and verify that the scenario
      results are updated to the misspelled name.

   1. Right click on `eurBank` in line 18 and verify that "Go to Definition"
      takes you to the definition in line 15.

   1. Close all files.

1. On your PR, add the comment:

   > Manual tests passed on [Linux/macOS].

   specifying which platform you tested on.

1. Run through the following test plan on Windows. This is slightly shortened to
   make testing faster and since most issues are not platform specific.

   1. Close any running SDK instance in PowerShell (Navigator or Sandbox).
   1. Download and run the Windows installer (the `.exe` file) from
      `https://github.com/digital-asset/daml/releases`.
   1. If asked if you want to remove an existing installation, click `Yes`.
   1. Open a new PowerShell.
   1. Run `daml new quickstart` to create a new project and switch to it using
      `cd quickstart`.
   1. Run `daml start`.
   1. Open your browser at `http://localhost:7500`, verify that you can login as
      Alice and there is one template and one contract.
   1. Kill `daml start` with `Ctrl-C`.
   1. Run `daml studio --replace=always` and open `daml/Main.daml`. Verify that
      the scenario result appears within 30 seconds.
   1. Add `+` at the end of line 26 after `"Alice"` and verify that you get an
      error.

1. On your PR, add the comment:

   > Manual tests passed on Windows.

1. If the release is bad, delete the release from [the releases
   page](https://github.com/digital-asset/daml/releases). Mention why it is bad
   as a comment on your PR, and **stop the process here**.

1. Add the label `Standard-Change` to your PR.

1. Go to [the releases page](https://github.com/digital-asset/daml/releases)
   and edit the release to look better. For both types of release, the release
   title should be the version number (i.e. same as the git tag). For a
   snapshot, the message should be set to

   > This is a snapshot release. Use at your own risk.

   For a stable release, the message should contain the team-lead-approved
   release notes, and the "prerelease" checkbox should be unticked.

1. Announce the release on the relevant internal Slack channels (#product-daml,
   \#team-daml). Add release notes in a thread under your announcement. For a
   stable release, these are the notes decided with the product team; for a
   snapshot release, include both the changes in this release (i.e. since the
   last snapshot) and the complete list of changes since the last stable
   release. Use the raw output of `unreleased.sh`.

   You can produce the changes since the previous (snapshot or stable) release
   by running:
   ```
   ./release.sh changes previous latest
   ```
   and the changes between the latest stable and the previous release with:
   ```
   ./release.sh changes stable previous
   ```

1. **[STABLE]** Coordinate with product (& marketing) for the relevant public
   announcements (public Slack, Twitter, etc.).

1. **[STABLE]** Documentation is published automatically once the release is
   public on Github, however it runs on an hourly job and takes about 20
   minutes to complete, so it could take up to an hour and a half depending on
   when the prerelease tag was removed.

Thanks for making a release!
