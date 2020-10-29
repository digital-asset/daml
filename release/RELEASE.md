# Making a Release

For snapshot releases, skip the steps marked **[STABLE]**. For stable releases,
skip the steps marked **[SNAPSHOT]**.

Valid commits for a release should come from either the `master` branch or one
of the support `release/a.b.x` branches (e.g. `release/1.0.x` branch is for
patches we backport to the 1.0 release branch).

> **IMPORTANT**: If the release fails, please delete it from the releases page
> and write how it failed on the PR.

1. **[STABLE]** Go through the [checklist] before making the release. Note that
   the checklist is not available publicly.

1. **[STABLE]** Stable releases are promoted from snapshot releases. Open a PR
   that changes the `LATEST` file to remove the `-snapshot` suffix on the
   corresponding snapshot, and add the `Standard-Change` label.

1. **[SNAPSHOT]** For most snapshot releases, the PR is created automatically.
   Double-check the snapshot version: it may need incrementing. Ask on Slack
   (`#team-daml`) if you're not sure.

   If you are manually creating the PR for an out-of-schedule snapshot, start
   from latest master and run
   ```
   ./release.sh snapshot <sha> <prefix>
   ```
   for example:
   ```
   $ ./release.sh snapshot cc880e2 0.1.2
   cc880e290b2311d0bf05d58c7d75c50784c0131c 0.1.2-snapshot.20200513.4174.0.cc880e29
   ```
   Then open a PR with the changed `LATEST` file, and add the `Standard-Change`
   label.

1. Once the PR has built, check that it was considered a release build by our
   CI. If you are working from an automated PR, check that it sent a message to
   `#team-daml` to say it has finished building. If the PR was manually created,
   you can look at the output of the `check_for_release` build step.

1. **[STABLE]** The PR **must** be approved by a team lead before merging. As
   of this writing (2020-08-13), @bame-da, @gerolf-da, @cocreature or
   @hurryabit.

1. Merge the PR and wait for the corresponding `master` build to finish. You
   will be notified on `#team-daml`.

1. On Windows, install the new SDK using the installer on
   https://github.com/digital-asset/daml/releases.

   On macOS/Linux:
   ```
   curl -sSL https://get.daml.com/ | sh -s "$VERSION"
   ```
   where `$VERSION` is the full version tag of the new release you are making,
   i.e. the second column of the `LATEST` file.

   > ## Tips for Windows testing in an ad-hoc machine
   >
   > If you are part of the release rotation, you can create Windows VMs
   > through the [ad-hoc] project. The created machine is a bit raw, though, so
   > here are a few tips to help you along.
   >
   > [ad-hoc]: https://github.com/DACH-NY/daml-language-ad-hoc
   >
   > `ad-hoc.sh` prints IP address, username and password for the created Windows VM.
   > Save this output. You will need this information later when you create an RDP connection.
   >
   > If you're on a Mac, you can use Microsoft Remote Desktop to connect; on
   > Linux, you can use Remmina.
   >
   > Remmina notes: when creating an RDP connection, you may want to specify custom
   > resolution. The default setting is to `use client resolution`. You may notice a
   > failure due to color depth settings. You can adjust those in the settings panel
   > right below the resolution settings.
   >
   > The ad-hoc machines take a bit of time to be available after being reported as
   > created, so be patient for a bit if your first connection attempt(s) fail.
   >
   > The first thing you should do is install Firefox, because Internet
   > Explorer is a pain. Open IE, go to Internet Options (gear icon in the top
   > right. If you don't see the icon, make sure the entire window fits into your screen),
   > switch to the Security tab, click Trusted Sites, click Sites, and
   > add `*.mozilla.org`. Then navigate to [getfirefox.com] and download the
   > "64-bit MSI" version.
   >
   > [getfirefox.com]: https://getfirefox.com
   >
   > Install Java from [adoptopenjdk]. The installer has an option, unticked by
   > default, to set the `JAVA_HOME` variable. Tick it.
   >
   > [adoptopenjdk]: https://adoptopenjdk.net
   >
   > [node] and [VS Code] installers add to PATH by default so those are just
   > download > next > next installations.
   >
   > [node]: https://nodejs.org/en/
   > [VS Code]: https://code.visualstudio.com
   >
   > All of the commands mentioned in this document can be run from a simple
   > DOS prompt (start menu -> type "cmd" -> click "Command prompt").

1. Prerequisites for running the tests:
    - [Visual Studio Code, Java-SDK](https://docs.daml.com/getting-started/installation.html)
    - [Node.js](https://nodejs.org/en/download/)
      - Just the bare install; no need to build C dependencies.

1. Run `daml version --assistant=yes` and verify that the new version is
   selected as the assistant version and the default version for new projects.

1. Tests for the getting started guide (macOS/Linux **and** Windows). Note: if
   using a remote Windows VM and an RDP client that supports copy/paste, you
   can run through this on both Windows and your local unix in parallel fairly
   easily.

    1. For these steps you will need the documentation for the
       release you are about to make. Documentation is published at
       every hour so if you wait for a bit you can go to
       https://docs.daml.com/$VERSION/getting-started/index.html.
       Otherwise, check out the commit that you are referencing in the `LATEST` file
       and build documentation locally via `./docs/scripts/preview.sh`.

    1. `daml new create-daml-app --template create-daml-app`

    1. `cd create-daml-app`

       1. `daml start`

    1. In a new terminal, from the `ui` folder:

       1. `npm install`

       1. `npm start`

    1. Open two browser windows (you want to see them simultaneously ideally) at `localhost:3000`.

    1. Log in as `Alice` in the first window, log in as `Bob` in the second window.

    1. In the first window, where you are logged in as `Alice`,
       follow `Bob` by typing their name in the text input and pressing enter.
       Verify that `Bob` appears in the
       list of users `Alice` is following. Verify in the other
       browser window that `Alice` shows up in `Bob`’s network.

    1. In the second window, where you are logged in as `Bob`,
       follow `Alice` by clicking on the "add user" logo to right of
       `Alice` name below the "The Network" heading.
       Verify that `Alice` appears in
       the list of users `Bob` is following. Verify in the other
       browser window that `Bob` shows up in `Alice`’s network.

    1. Open the your first feature section of the GSG, e.g., from
       https://docs.daml.com/$VERSION/getting-started/first-feature.html
       if you did not build docs locally.

    1. Run `daml studio --replace=always` from the project root
       directory and open `User.daml`.

    1. Copy the `Message` template from the documentation to the end of `User.daml`.

    1. Copy the `SendMessage` choice from the documentation to the
       `User` template below the `Follow` choice.

    1. Close VSCode.

    1. In the terminal where `daml start` is running, press 'r' 
       respectively 'r' + 'Enter' on Windows.

    1. Run `code .` from the project root directory (the extension is
       already installed, no need to use `daml studio`).

    1. Create `MessageList.tsx`, `MessageEdit.tsx` and modify
       `MainView.tsx` as described in the documentation.

    1. Verify that you do not see errors in the typescript code in VSCode.

    1. Close VSCode.

    1. As before, open two browser windows at `localhost:3000` and log
       in as `Alice` and `Bob`.

    1. Make `Alice` follow `Bob`.

    1. From `Bob`, select `Alice` in the `Select a follower` drop down,
       insert `hi alice` in the message field and click on `Send`.

    1. Verify that `Alice` has received the message in the other window.

    1. Make `Bob` follow `Alice`.

    1. From `Alice`, select `Bob` in the drop down insert `hi bob` in
       the message field and click on `Send`.

    1. Verify that `Bob` has received the message in the other window.

    1. You can now close both browser windows and both running processes (`daml
       start` and `npm start`).

    1. Don't forget to run this on the other platform! E.g. if you just ran
       through on Linux or macOS, you still need to run on Windows, and vice
       versa.

1. Tests for `quickstart-java` (Linux/macOS)

   While this is no longer the default getting started guide we still test it
   for now since it covers things not covered by the new GSG
   (Navigator, scenarios, Maven artifacts, …)

    1. Create a new project with `daml new quickstart --template quickstart-java`
       and switch to it using `cd quickstart`.

    1. Verify the new version is specified in `daml.yaml` as the `sdk-version`.

    1. Run `daml start`. Your browser should be opened automatically at
       `http://localhost:7500`. Login as `Alice` and verify that there is
       1 contract and 3 templates. Close the tab and kill `daml start` using
       `Ctrl-C`.

    1. Run `daml build`.

    1. In 3 separate terminals (since each command blocks), run:

       1. `daml sandbox --wall-clock-time --port 6865 .daml/dist/quickstart-0.0.1.dar`
       1. `daml script --dar .daml/dist/quickstart-0.0.1.dar --script-name Main:initialize --ledger-host localhost --ledger-port 6865 --wall-clock-time && daml navigator server localhost 6865 --port 7500`
       1. `daml codegen java && mvn compile exec:java@run-quickstart`

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

    1. Click on `Script results` above `initialize` and wait for the script
       results to appear.

    1. Add `+` at the end of line 14, after `"Alice")` and confirm you get an
       error in line 15.

    1. Add `1` after the `+` and confirm you get a type error in line 14,
       which says that `Script Party` does not match `Int`.

    1. Delete the `+1` and the `e` in the second `"Alice"` and verify
       that the script results are updated to the misspelled name.

    1. Right click on `eurBank` in line 20 and verify that "Go to Definition"
       takes you to the definition in line 17.

    1. Close all files.

1. On your PR, add the comment:

   > Manual tests passed on [Linux/macOS].

   specifying which platform you tested on.

1. Run through the following test plan on Windows. This is slightly shortened to
   make testing faster and since most issues are not platform specific.

   1. Run `daml new quickstart` to create a new project and switch to it using
      `cd quickstart`.
   1. Run `daml start`.
   1. Open your browser at `http://localhost:7500`, verify that you can login as
      Alice and there is one template and one contract.
   1. Kill `daml start` with `Ctrl-C`.
   1. Run `daml studio --replace=always` and open `daml/Main.daml`. Verify that
      the scenario result appears within 30 seconds.
   1. Add `+` at the end of line 14 after `(PartyIdHint "Alice")` and verify that
      you get an error on line 15.

1. On your PR, add the comment:

   > Manual tests passed on Windows.

1. If the release is bad, delete the release from [the releases page]. Mention
   why it is bad as a comment on your PR, and **stop the process here**.

1. Announce the release on the relevant internal Slack channels (`#product-daml-connect`,
   `#team-daml`). For a stable release, direct people to the release blog post;
   for a prerelease, you can include the raw output of the `unreleased.sh`
   script.

1. **[STABLE]** Go to the [releases page] and remove the prerelease marker on
   the release. Also change the text to
   ```See [the release notes blog]() for details.```
   adding in the direct link to this version's [release notes]. Documentation
   for this release will be added to docs.daml.com on the next hour.

1. **[STABLE]** Coordinate with product (& marketing) for the relevant public
   announcements (DAML Forum, Twitter, etc.).

1. **[STABLE]** Documentation is published automatically once the release is
   public on GitHub, though this runs on an hourly cron.

Thanks for making a release!

[checklist]: https://docs.google.com/document/d/1RY2Qe9GwAUiiSJmq1lTzy6wu1N2ZSEILQ68M9n8CHgg
[releases page]: https://github.com/digital-asset/daml/releases
[release notes]: https://daml.com/release-notes/
