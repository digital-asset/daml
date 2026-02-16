# Making a Release

Daml Enterprise releases are made in the [assembly] repo by combining the
releases of individual components (as of 2024-05-28, those are SDK (this repo),
canton, drivers, scribe, and finance). SDK releases are triggered in this repo
by modifying the [`LATEST`] file.

If you're looking for broader context on Daml Enterprise releases, please see
the [assembly] repo.

## Release Branches

In this repo, the release branch for minor version `A.B` must be named
`release/A.B.x`, e.g. `release/2.7.x`.

> Note: This also means you should not create branches with those (or similar)
> names if they are not meant to be release branches.

> Note: In most cases work should not be done on a release branch directly.
> Changes should go into the `main` branch first, and then be backported to the
> release branch.

> **When you create the release branch in the [daml] repo, make a PR to `main`
> bumping [`NIGHTLY_PREFIX`] accordingly.** You may also want to bump it in the
> release branch (e.g. from 2.8.0 to 2.8.1).

## Creating an SDK Release

The steps to create a release are:

1. In the [daml] repo, edit the [`LATEST`](../LATEST) file. The format of that file is a
   commit sha of the code you want to release (at this point typically the tip
   of the release branch), the version number you want to attribute to the
   build, and the words `SPLIT_RELEASE` (for historical reason dating to
   pre-2.0 days). For a snapshot release, one can generate a snapshot version
   string using the `release.sh` script. For example, to create a PR for a
   2.0.0 snapshot release:

   ```
   $ git fetch
   $ ./release.sh snapshot origin/release/3.4.x 3.4.0
   ```

   You should put that line in the file in order to preserve semver ordering,
   and overwrite any existing snapshot with the same prefix.

   For non-adhoc snapshot please add the link to the commits of the release branch
   and the commit sha in the description of you PR. For instance here are examples 
    - "Create a snapshot for [main](https://github.com/digital-asset/daml/commits/main/) at commit ae6a13742aa79da2fcac79e0a27c6f7fe81fcf10"
    - "Create a snapshot for [release/3.3.x](https://github.com/digital-asset/daml/commits/release/3.3.x/) at commit 7f71343bd265865977a09c4936d48278322e34ad"

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

This testing procedure starts once the release is listed on the [releases
page]. That is, _after_ the [assembly] step has finished.

### Windows (Daml Assistant)

1. Find the PR on the assembly repo (as linked/pinged in the `#team-daml` slack channel). Follow the instructions in its description.

1. Set $VERSION as the release version listed in the PR you merged above.

> ## Tips for Windows testing in an ad-hoc machine
>
> If you are part of the release rotation, you should b able to create
> Windows VMs through the [ad-hoc] project. If you don't have permission,
> follow these steps:
>
> 1. First you should clone the [ad-hoc] repository and then enter the
>    cloned repo.
> 2. Edit `tf/main.tf` and add your username to the `members` field of the
>    `google_project_iam_binding.machine_managers` resource. Generate and
>    submit a PR with these changes. Once the PR has been accepted, you
>    should now have permission to create GCP compute instances.
>
> Assuming `direnv` is installed, entering the `daml-language-ad-hoc`
> project directory will be sufficient to configure and install the extra
> software (e.g. the GCP SDK) required for your environment. Note that this
> could take a few minutes.
>
> A new GCP windows instance can be created by running `./ad-hoc.sh temp
windows` inside the `daml-language-ad-hoc` project. This command prints IP
> address, username and password for the created Windows VM. Save this
> output. You will need this information later when you create an RDP
> connection.
>
> > ‼️ After starting, it's going to take some time for the machine to be
> > configured (see notes below).
>
> Before you may connect to this windows instance, you need to ensure that
> the VPN is connected. On macOS you can do this by selecting the
> preconfigured _Connect GCP Frankfurt full tunnel_ VPN profile.
>
> If you're on a Mac, you can use _Microsoft Remote Desktop_ aka **Windows App**
> to connect. This can be installed via the Mac App Store or directly
> [here](https://go.microsoft.com/fwlink/?linkid=868963).
>
> If you're on Linux, you can use [Remmina].
>
> > Remmina notes: when creating an RDP connection, you may want to specify
> > custom resolution. The default setting is to `use client resolution`.
> > You may notice a failure due to color depth settings. You can adjust
> > those in the settings panel right below the resolution settings.
>
> The ad-hoc machines take a bit of time to be available after being
> reported as created, so be patient for a bit if your first connection
> attempt(s) fail. Also note that the machine will accept connections before
> it is fully initialized; initialization is finished when the Firefox icon
> appears on the Desktop.
>
> Ad-hoc machines come with VSCode and OpenJDK preinstalled.
>
> NOTE 2: After logging in, **it takes some time for the machine to be
> configured.** The script that installs Firefox, VSCode and OpenJDK runs
> once the machine is available for login. The software you need should
> appear within about 10 minutes (an easy way to check is to try to open
> `D:\` , as this volume is created after all the software is installed).
>
> All the commands mentioned in this testing section can be run from a
> simple DOS prompt (start menu -> type `cmd.exe` -> click "Command prompt").
>
> At the end of your Windows testing session, please be sure to terminate
> the GCP instance by running `./ad-hoc.sh destroy $ID`. Here `$ID` is the
> identity for your GCP instance - this is printed when you create your
> Windows instance.

1. Install the new SDK using the installer on the [releases page]. This will
   typically be the asset named `daml-sdk-$VERSION-windows.exe` (located on the
   [DAML releases](https://github.com/digital-asset/daml/releases) page). The
   automated message on `#team-daml` should contain a direct link to the
   installer.

1. Run `daml version --assistant=yes` and verify that the new version is
   selected as the assistant version and the default version for new projects.

1. Run `daml new myproject --template multi-package-example` to create a new project and switch to its main package using
   `cd myproject/main`.

1. Run `daml start`. Wait for the yellow message confirming the sandbox is ready for use.

1. Kill `daml start` with `Ctrl-C` (followed by "y" because Windows).

1. Build all packages by moving out of the main package (`cd ..`) and running `daml build --all`.
   (The first line should say `Running multi-package build of all packages in <location>`)

1. Run `daml studio --replace=always` and open `test/daml/Test.daml`. Verify that there is a `Script results` link above `main` (in the code), click on it, and
   verify that the script result appears within 30 seconds.

1. Add `+` at the end of line 11 after `allocateParty "Alice"`, verify that
   you get an error on line 12, then undo the `+` changes.

1. Right click `IOU` in `... createCmd Main.IOU with ...` on line 12 and click Go to definition.
  1. Verify you are taken to main/daml/Main.daml (and not somewhere in a `.daml` directory)

1. Close the `Main.daml` tab, open `multi-package.yaml`, remove `- ./main`, save the file, then go to definition on IOU again as per previous instruction.
  1. Verify that now (after a small delay), you are taken to a read-only copy of `Main.daml`. This can be verified by attempting to make a change in this file.

1. Close VSCode and delete the `myproject` directory ready for DPM testing using:
  `cd .. && rmdir /S /Q myproject`

1. On the [assembly] PR, add the comment:

   > Manual tests passed on Windows using Daml Assistant.

# Windows (DPM)

You will now also need to test the release using DPM, please follow the instructions below (they look similar, but have small tweaks, don't reuse commands from the Daml Assistant section above).
**ONLY RUN THESE STEPS FOR 3.4+ VERSIONS RELEASED AFTER 2025/09/22**

1. If you're testing a snapshot, the PR that created the daml snapshot (on the dpm-assembly repo) should have a comment with a link to the windows install you need. If it does not, contact @samuelwilliams-da. Download this installer (by copying the link into your browser on windows, and running the downloaded file)
   If you're testing a full release, install it as per [dpm documentation](https://docs.digitalasset.com/build/3.4/dpm/dpm.html)

1. Run `dpm versions` and verify the (green) version is what you expect. If you did not know what version to expect, ensure it is less than a week old for weekly testing.
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

1. On the [assembly] PR, add the comment:

   > Manual tests passed on Windows using DPM.

1. Destroy your Windows VM.

### Linux/macOS (Daml Assistant)

1. Find the PR on the assembly repo (as linked/pinged in the `#team-daml` slack channel). Follow the instructions in its description.

1. Set $VERSION as the release version listed in the PR you merged above.

1. Run:

   ```
   curl -sSL https://get.daml.com/ | sh -s "$VERSION"
   ```

1. Make sure you have the prerequisites for running the tests:

   - [Visual Studio Code, Java-SDK](https://docs.daml.com/getting-started/installation.html)
   - [Maven](https://maven.apache.org)

1. Run `daml version --assistant=yes` and verify that the new version is
   selected as the assistant version and the default version for new projects.

1. Create a new project with `daml new quickstart --template quickstart-java`
   and switch to it using `cd quickstart`.

1. Verify the new version is specified in `daml.yaml` as the `sdk-version`.

1. Run `daml start`, and wait for the yellow message confirming the sandbox is ready for use.

1. Kill `daml start` using `Ctrl-C`.

1. Run `daml build`.

1. In 3 separate terminals (each being in the `quickstart-java` project directory), run:

   1. In Terminal 1, run:

      ```
      daml sandbox --port 6865
      ```

   1. In Terminal 2, run each of the following:

      ```sh
      daml ledger upload-dar --host localhost --port 6865 .daml/dist/quickstart-0.0.1.dar
      ```

      ```sh
      daml script --ledger-host localhost --ledger-port 6865 --dar .daml/dist/quickstart-0.0.1.dar --script-name Main:initialize --output-file output.json
      ```

      ```sh
      daml codegen java && mvn compile exec:java@run-quickstart -Dparty=$(cat output.json | sed 's/\[\"//' | sed 's/".*//')
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
  daml new myproject --template multi-package-example
  ```
  then cd into it.

1. Build it using `daml build --all`
   (The first line should say `Running multi-package build of all packages in <location>`)

1. Run `daml studio --replace=always`. This should open VSCode and trigger the
   Daml extension that's bundled with the new SDK version (the new VSCode
   extension will not be in the marketplace at this point). Verify by checking
   the version listed under "Installation" matches the version of the sdk bundled
   in the snapshot (described in the description of the overall release 
   here https://github.com/digital-asset/daml/releases).

   > Note: when running `daml studio --replace=always`, you force the installation
   > of the VSCode extension bundled with the Daml SDK, and _disable the
   > auto-upgrade mechanism in VSCode_. To instruct VSCode to go back to the
   > published version of the extension, including auto-upgrades, you can run
   >
   > ```
   > daml studio --replace=published
   > ```

1. Open `test/daml/Test.daml`.

1. Click on `Script results` above the script and wait for the
   script results to appear.

1. Add `+` at the end of line 11 after `allocateParty "Alice"` and verify that
   you get an error on line 12. Then undo the `+` changes.

1. Right click `IOU` in `... createCmd Main.IOU with ...` on line 12 and click Go to definition.
  1. Verify you are taken to main/daml/Main.daml (and not somewhere in a `.daml` directory)

1. Close the `Main.daml` tab, open `multi-package.yaml`, remove `- ./main`, save the file, then go to definition on IOU again as per previous instruction.
  1. Verify that now (after a small delay), you are taken to a read-only copy of `Main.daml`. This can be verified by attempting to make a change in this file.

1. Close VSCode and delete the `myproject` directory ready for DPM testing using:
  `cd .. && rm -rf myproject`

1. On the [assembly] PR, add the comment:

   > Manual tests passed on [Linux/macOS] using Daml Assistant.

   specifying which platform you tested on.

### Linux/macOS (DPM)

You will now also need to test the release using DPM, please follow the instructions below (they look similar, but have small tweaks, don't reuse commands from the Daml Assistant section above).
**ONLY RUN THESE STEPS FOR 3.4+ VERSIONS RELEASED AFTER 2025/09/22**

1. If you're testing a snapshot, the PR that created the daml snapshot (on the dpm-assembly repo) should have a comment with the curl command you need. If it does not, contact @samuelwilliams-da.
   If you're testing a full release, install it as per [dpm documentation](https://docs.digitalasset.com/build/3.4/dpm/dpm.html)

1. Make sure you have the prerequisites for running the tests:

   - [Visual Studio Code, Java-SDK](https://docs.daml.com/getting-started/installation.html)
   - [Maven](https://maven.apache.org)

1. Run `dpm versions` and verify the (green) version is what you expect. If you did not know what version to expect, ensure it is less than a week old for weekly testing.
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

1. On the [assembly] PR, add the comment:

   > Manual tests passed on [Linux/macOS] using DPM.

   specifying which platform you tested on.

### Wrap up

1. If the release is bad, ask #team-daml to delete the release from the [releases
   page]. Mention why it is bad as a comment on your PR, and **stop the process
   here**.

   Note that **the Standard-Change label must remain on the PR**, even if the
   release has failed.

1. if testing fails, do not report this on #product-releases, instead please report this in #team-daml-language.

1. for 2.10 and 3.x releases, please annouce that tests were successful on #product-releases.

1. for 3.x releases, that pass testing, please ensure that artifacts are successfully published on [artifactory](https://digitalasset.jfrog.io/ui/repos/tree/General/external-files/daml-enterprise) and [github](https://github.com/digital-asset/daml/releases).

1. for 2.10 rleases, that pass testing, please ensure that _key_ stakeholders are pinged in the slack message that announces successful testing (that way they may inform selected end users of the snapshots availability).

1. Announce the release on `#product-releases` on Slack. For a stable release,
   direct people to the release blog post. If there were any errors during testing,
   but we decided to keep the release anyway, report those on the PR and include a
   link to the PR in the announcement.

For a stable release, you need to additionally:

1. Go to the [releases page] and remove the prerelease marker on
   the release. Also change the text to
   `See [the release notes blog]() for details.`
   adding in the direct link to this version's [release notes].

Thanks for making a release!

[Remmina]: https://remmina.org
[`LATEST`]: https://github.com/digital-asset/daml/blob/main/LATEST
[`NIGHTLY_PREFIX`]: https://github.com/digital-asset/daml/blob/main/NIGHTLY_PREFIX
[`release.sh`]: https://github.com/digital-asset/daml/blob/main/release.sh
[ad-hoc]: https://github.com/DACH-NY/daml-language-ad-hoc
[assembly]: https://github.com/DACH-NY/assembly
[daml]: https://github.com/digital-asset/daml
[release notes]: https://daml.com/release-notes/
[releases page]: https://github.com/digital-asset/daml/releases
