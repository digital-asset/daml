# Making a Release

Daml SDK releases are created by the [dpm-assembly] repo, as bundles of components created by other repos. The daml repo is repsonsible for publishing the following components (as of 2026-05-05):
- damlc - The daml compiler and IDE
- daml-script - The daml script runner and daml library
- upgrade-check - A tool for checking upgrade compatibility between DARs
- daml-new - A project templating tool, for new daml projects

Releases of the above components are unified and triggered by changes to the [`LATEST`] file.

## Release Branches

In this repo, the release branch for minor version `A.B` must be named
`release/A.B.x`, e.g. `release/3.5.x`.

> Note: This also means you should not create branches with those (or similar)
> names if they are not meant to be release branches.

> Note: In most cases work should not be done on a release branch directly.
> Changes should go into the `main` branch first, and then be backported to the
> release branch.

> **When you create the release branch in the [daml] repo, make a PR to `main`
> bumping [`NIGHTLY_PREFIX`] accordingly.** You may also want to bump it in the
> release branch (e.g. from 3.5.0 to 3.5.1).

## Creating an SDK Release

The steps to create a release are:

1. In the [daml] repo, edit the [`LATEST`](../LATEST) file. The format of that file is a
   commit sha of the code you want to release (at this point typically the tip
   of the release branch), the version number you want to attribute to the
   build, and the words `SPLIT_RELEASE` (for historical reason dating to
   pre-2.0 days). For a snapshot release, one can generate a snapshot version
   string using the `release.sh` script. For example, to create a PR for a
   3.5.0 snapshot release:

   ```
   $ git fetch
   $ ./release.sh snapshot origin/release/3.5.x 3.5.0
   ```

   You should put that line in the file in order to preserve semver ordering,
   and overwrite any existing snapshot with the same prefix.

   For non-adhoc snapshot please add the link to the commits of the release branch
   and the commit sha in the description of you PR. For instance here are examples 
    - "Create a snapshot for [main](https://github.com/digital-asset/daml/commits/main/) at commit ae6a13742aa79da2fcac79e0a27c6f7fe81fcf10"
    - "Create a snapshot for [release/3.5.x](https://github.com/digital-asset/daml/commits/release/3.5.x/) at commit 7f71343bd265865977a09c4936d48278322e34ad"

   For a stable release, follow the same steps but remove the `-snapshot.*`
   part of the generated string.

   **Only change a single line** in [`LATEST`](../LATEST), otherwise
   no release is going to be created.

2. Make a PR **targeting the `release-trigger` branch** with just that one line added,
   touching no other file. Add the `Standard-Change` label to that PR.

   **The `release-trigger` branch is the only one that triggers releases, even for "release
   line" releases.**

3. When the PR is merged, the build of the corresponding commit on `release-trigger` will
   create a "split release" bundle and push it to Artifactory. It then notifies
   on `#team-internal-releases` on Slack.

## Daily Snapshots

CI will automatically create a daily snapshot using the tip of the `main` and
`main-2.x` branch. This does not appear in the [`LATEST`] file but is
otherwise identical to a snapshot one would have created on the same commit
using the steps in the previous section.

## Testing

This testing procedure starts once a dpm assembly release has been created. Stable releases are listed [here](https://console.cloud.google.com/artifacts/docker/da-images/europe/public/sdk-manifests%2Fopen-source?project=da-images), unstable releases (snapshots + ad-hoc) are listed [here](https://console.cloud.google.com/artifacts/docker/da-images/europe/public-unstable/sdk-manifests%2Fopen-source?project=da-images)

### Windows

1. If you're testing a snapshot, the PR that created the dpm snapshot (on the dpm-assembly repo) should have a comment with a link to the windows installer you need. If it does not, contact @samuelwilliams-da. Download this installer (by copying the link into your browser on windows, and running the downloaded file)
   If you're testing a full release, install it as per [dpm documentation](https://docs.digitalasset.com/build/3.4/dpm/dpm.html)

1. Run `dpm versions` and verify the (green) version is what you expect (for snapshots, this should be the version in the most recent generated comment on the snapshot PR). If you did not know what version to expect, ensure it is less than a week old for weekly testing.
   (If it is too old, leave a message about this in #project-dpm and pause testing until you get a response. #project-dpm is US based so consider this for timezones.)

1. Run `dpm new myproject --template multi-package-example` to create a new project and switch to it
   `cd myproject`.

1. Build all packages by running `dpm build --all`.
   (The first line should say `Running multi-package build of all packages in <location>`)

1. Run `dpm sandbox --dar ./main/.daml/dist/myproject-main-1.0.0.dar`. Wait for "Canton sandbox is ready." (it may take up to around 2 minutes)

1. Kill `dpm sandbox` with `Ctrl-C` (followed by "y" because Windows).

1. Run `dpm studio --replace=always` and open `test/daml/Test.daml`. Verify that there is a `Script results` link above `main` (in the code), click on it, and
   verify that the script result appears within 30 seconds.

1. Add `+` at the end of line 11 after `allocateParty "Alice"`, verify that
   you get an error on line 12, then undo the `+` changes.

1. Right click `IOU` in `... createCmd Main.IOU with ...` on line 12 and click Go to definition.
  1. Verify you are taken to main/daml/Main.daml (and not somewhere in a `.daml` directory)

1. Close the `Main.daml` tab, open `multi-package.yaml`, remove `- ./main`, save the file, then go to definition on IOU again as per previous instruction
  1. Verify that now (after a small delay), you are taken to a read-only copy of `Main.daml`. This can be verified by attempting to make a change in this file.

1. Close VSCode

1. On the [dpm-assembly] PR, add the comment:

   > Manual tests passed on Windows.

1. Destroy your Windows VM.


### Linux/macOS

1. If you're testing a snapshot, the PR that created the daml snapshot (on the dpm-assembly repo) should have a comment with the curl command you need. If it does not, contact @samuelwilliams-da.
   If you're testing a full release, install it as per [dpm documentation](https://docs.digitalasset.com/build/3.4/dpm/dpm.html)

1. Make sure you have the prerequisites for running the tests:

   - [Visual Studio Code, Java-SDK](https://docs.daml.com/getting-started/installation.html)
   - [Maven](https://maven.apache.org)

1. Run `dpm versions` and verify the (green) version is what you expect (for snapshots, this should be the version in the most recent generated comment on the snapshot PR). If you did not know what version to expect, ensure it is less than a week old for weekly testing.
   (If it is too old, leave a message about this in #project-dpm and pause testing until you get a response. #project-dpm is US based so consider this for timezones.)
   (If the version you need is listed but not green, run `rm -rf ~/.dpm` then run the installation again)

1. Create a new project with `dpm new quickstart --template quickstart-java`
   and switch to it using `cd quickstart`.

1. Verify the new version is specified in `daml.yaml` as the `sdk-version`.

1. Build the package by running `dpm build`.

1. In 3 separate terminals (each being in the `quickstart-java` project directory), run:

   1. In Terminal 1, run:

      ```sh
      dpm sandbox --dar ./.daml/dist/quickstart-0.0.1.dar
      ```
      Wait for "Canton sandbox is ready."

   1. In Terminal 2, run each of the following:

      ```sh
      dpm script --ledger-host localhost --ledger-port 6865 --dar .daml/dist/quickstart-0.0.1.dar --script-name Main:initialize --output-file output.json
      ```

      ```sh
      dpm codegen-java && mvn compile exec:java@run-quickstart -Dparty=$(cat output.json | sed 's/\[\"//' | sed 's/".*//')
      ```

   1. In Terminal 3, run:

      ```sh
      cat output.json
      ```

      and verify that the output looks like this:

      ```json
      ["Alice::NAMESPACE", "EUR_Bank::NAMESPACE"]
      ```

      where `NAMESPACE` is some randomly generated series of hex digits.

      Note that this step scrapes the `Alice::NAMESPACE` party name from the `output.json` produced in the previous steps.

   1. Still in Terminal 3, run:

      ```sh
      curl http://localhost:8080/iou
      ```

      and verify that the output looks like:

      ```json
      {
        "0": {
          "issuer": "EUR_Bank::NAMESPACE",
          "owner": "Alice::NAMESPACE",
          "currency": "EUR",
          "amount": 100.0,
          "observers": []
        }
      }
      ```

      where NAMESPACE is the same series of hex digits as in the previous step.

1. Kill the processes running in Terminals 1 and 2 using CTRL-C.

1. Delete the quickstart project, create a new project using
  ```
  dpm new myproject --template multi-package-example
  ```
  then cd into it.

1. Build it using `dpm build --all`
   (The first line should say `Running multi-package build of all packages in <location>`)

1. Run `dpm studio --replace=always`. This should open VSCode and trigger the
   Daml extension that's bundled with the new SDK version (the new VSCode
   extension will not be in the marketplace at this point). Verify by checking
   the version listed under "Installation". It is difficult to find the exact
   version string this should be, simply verify it is within a few days
   of the SDK version you installed. (using the YYYYMMDD timestamp in the string)

   > Note: when running `dpm studio --replace=always`, you force the installation
   > of the VSCode extension bundled with the Daml SDK, and _disable the
   > auto-upgrade mechanism in VSCode_. To instruct VSCode to go back to the
   > published version of the extension, including auto-upgrades, you can run
   >
   > ```
   > dpm studio --replace=published
   > ```
   > Also note that Studio will use `Daml` or `Dpm` based on the last studio command you ran
   > If you want to switch your default assistant back to `Daml`, run `daml studio --replace=published`.

1. Open `test/daml/Test.daml`.

1. Click on `Script results` above the script and wait for the
   script results to appear.

1. Add `+` at the end of line 11 after `allocateParty "Alice"` and verify that
   you get an error on line 12. Then undo the `+` changes.

1. Right click `IOU` in `... createCmd Main.IOU with ...` on line 12 and click Go to definition.
  1. Verify you are taken to main/daml/Main.daml (and not somewhere in a `.daml` directory)

1. Close the `Main.daml` tab, open `multi-package.yaml`, remove `- ./main`, save the file, then go to definition on IOU again as per previous instruction.
  1. Verify that now (after a small delay), you are taken to a read-only copy of `Main.daml`. This can be verified by attempting to make a change in this file.

1. Close VSCode.

1. On the [dpm-assembly] PR, add the comment:

   > Manual tests passed on [Linux/macOS].

   specifying which platform you tested on.

### Wrap up

1. If the release is bad, close the PR on the [dpm-assembly] repo. Mention why the release is bad in the closing comment. After this, please report the failure on #project-dpm (or #team-daml-language, if you know better).
   Note that **the Standard-Change label must remain on the PR**, even if the
   release has failed.

1. If the release is good, merge the PR! This will label the release as tested in DPM.

1. For stable releases, please announce that tests were successful on #product-releases.

1. For 2.10 releases that pass testing, please ensure that _key_ stakeholders are pinged in the slack message that announces successful testing (that way they may inform selected end users of the snapshots availability).

Thanks for making a release!

[Remmina]: https://remmina.org
[`LATEST`]: https://github.com/digital-asset/daml/blob/main/LATEST
[`NIGHTLY_PREFIX`]: https://github.com/digital-asset/daml/blob/main/NIGHTLY_PREFIX
[`release.sh`]: https://github.com/digital-asset/daml/blob/main/release.sh
[ad-hoc]: https://github.com/DACH-NY/daml-language-ad-hoc
[dpm-assembly]: https://github.com/DACH-NY/dpm-assembly
[daml]: https://github.com/digital-asset/daml
[release notes]: https://daml.com/release-notes/
[releases page]: https://github.com/digital-asset/daml/releases
