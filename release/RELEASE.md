# Making a Release

Valid commits for a release should come from either the `main` branch or one
of the support `release/a.b.x` branches (e.g. `release/1.0.x` branch is for
patches we backport to the 1.0 release branch).

> **IMPORTANT**: If the release fails, please delete it from the [releases page]
> and write how it failed on the PR.

First, you need to know which type of release you are making. At this point in
time, these are the options, in rough order of likelihood/frequency:

1. The weekly snapshot.
2. A release candidate for a 2.x release.
3. A stable 2.x release.
4. A release candidate for a 1.x release.
5. A stable 1.x release.

If you don't know which of these your current case fits in, or you think it
doesn't fit in any, please reach out on `#team-daml` on Slack, preferably
mentioning `@gary`.

## The weekly snapshot

> Note: You should not have to make any change to this repo.

1. On Wednesday morning, a cron should create a release PR on the [assembly]
   repo, mentioning you. Please reach out to `@gary` on Slack if that's
   missing.

2. Merge the PR and wait for the corresponding `main` build to finish.

3. Go to the [Testing](#testing) section of this file.

## 2.x release candidate

In a perfect world, there is no need for a separate RC: the latest weekly
snapshot can work. In the world we live in, though, we frequently want to add a
few things, or patch previous minor rleases.

In those cases, we create `release/` branches (e.g. `release/2.0.x`). Those are
special branches, protected by GitHub rules and treated specially by CI.

When making a release candidate, you generally want to pick the tip of one of
those release branches. The release itself is always triggered from `main`.

The process is similar to the weekly snapshot, except that both the [daml] and
[canton] bundles have to be manually triggered. Specifically:


1. Make sure all the changes you want are in the release branch.
2. Start _from latest `main`_ and run
   ```
   $ ./release.sh snapshot origin/release/2.0.x 2.0.1
   ```
   The output will be a line that starts with a commit sha, followed by a
   snapshot release number. You need to take that line and add it to the [ `LATEST`]
   file, adding ` SPLIT_RELEASE` at the end of that line. You should put that line
   in the file so as to preserve semver ordering, and overwrite any existing
   snapshot with the same prefix.
3. Make a PR against the `main` branch with just that one line added, touching
   no other file. Add the `Standard-Change` label to that PR.
4. When the PR is merged, the build of the corresponding commit on `main` will
   create a "split release" bundle and push it to Artifactory. It should notify
   on `#ci-failures-daml` on Slack.
5. The next step is to request a build from the [Canton] team (on
   `#team-canton`) that relies on the RC snapshot. Once you have a [canton]
   release, you can proceed.
6. Go to the [assembly] repo, and follow the instructions there to make a release
   using the [Canton] version that was just created. The `LATEST` file on the
   [assembly] repo only contains one version; it is safe to overwrite it, and to
   "go backwards" if needed.
7. Once the `main` build of the [assembly] repo has finished, you should
   proceed with testing. You should open up this document _in the branch of
   the release you're making_, as testing instructions change over time.
8. If this was a main-branch release, bump the `NIGHTLY_PREFIX` file in the
   daml repo.
9. After testing, if everything went well, you should go on to turning the RC
   into a stable release. If something went wrong, make appropriate changes to
   the release branch and start over.

## Stable 2.x

The overall process of coordinating a stable release is documented in the
[release planning] document, including the relevant stakeholders and roles.
Cutting and testing a release as documented here is part of that process.

Making a stable release follows the same steps as a snapshot RC, except that:

- You should not be choosing an arbitrary commit, you should pick the latest RC
  for the branch.
- Instead of adding a line to [`LATEST`], remove the `-snapshot` part of the
  version number for the existing RC.
- Similarly, modifying the `LATEST` file on the assembly repo _should_ amount
  to just removing the `-` parts of the version numbers for both Canton and
  daml, but follow the instructions there.
- You need a team lead to approve the PR on both the [daml] and [assembly] repos.

Once you have finished testing the release, communicate this to the relevant
stakeholder (send a message on `#team-daml` if unsure) so that the process can
move on by removing the `prerelease` marker on the [releases page]. (This is what
makes it available to `daml install`.)

## 1.x release candidate

1. Make sure all the changes you want are in the release branch.
2. Start _from latest `main`_ and run
   ```
   $ ./release.sh snapshot origin/release/1.18.x 1.18.3
   ```
   The output will be a line that starts with a commit sha, followed by a
   snapshot release number. You need to take that line and add it to the [`LATEST`]
   file. You should put that line in the file so as to preserve semver
   ordering, and overwrite any existing snapshot with the same prefix.
3. Make a PR against the `main` branch with just that one line added, touching
   no other file. Add the `Standard-Change` label to that PR.
4. Once the PR has built, check that it was considered a release build by our
   CI. You can look at the output of the `check_for_release` job.
5. When the PR is merged, the build of the `main` branch will create the
   release, push it to GitHub releases, and announce it is ready for testing on
   `#team-daml`.
6. Follow the testing instructions in this document, but from the tip of the
   release branch.
7. After testing, if everything went well, you should go on to turning the RC
   into a stable release. If something went wrong, make appropriate changes to
   the release branch and start over.

## Stable 1.x

Making a stable release follows the same steps as a snapshot RC, except that:

- You should not be choosing an arbitrary commit, you should pick the latest RC
  for the branch.
- Go through the [checklist] before making the release. Note that
  the checklist is not available publicly. Since 1.x are old patch releases at
  this point, you may have to adapt the checklist a bit. Usee your best
  judgement; if we're making a patch release on 1.x at this point there should be
  a specific reason for it, which should suggest specific additional tests (e.g.
  a speecific bug we want to fix).
- Instead of adding a line to [`LATEST`], remove the `-snapshot` part of the
  version number for the existing RC.
- You need a team lead to approve the PR.

Once you have finished testing the release, coordinate with Product to decide
how to communicate around it and when to remove the `prerelease` marker on the
[releases page]. (This is what makes it available to `daml install`.)

## Testing

This testing procedure starts once the release is listed on the [releases page]. 

In the following notes, we assume that `$VERSION` contains
the full version tag for the release you are testing - in other words, the full version as recorded on the Slack 
`#team-daml` message that is generated when closing the `main` build PR.

For example, for the Slack message:
> team_daml_notifs
>
> Just published `2.4.0-snapshot.20220830.10494.0.4622de48`.
>
> For testing:
>
> - Follow the [instructions](https://github.com/digital-asset/daml/blob/v2.4.0-snapshot.20220830.10494.0.4622de48/release/RELEASE.md).
> - Install on macOS/Linux with `curl -sSL https://get.daml.com/ | sh -s 2.4.0-snapshot.20220830.10494.0.4622de48`.
> - Install on Windows using this [link](https://github.com/digital-asset/daml/releases/download/v2.4.0-snapshot.20220830.10494.0.4622de48/daml-sdk-2.4.0-snapshot.20220830.10494.0.4622de48-windows.exe).

we set `$VERSION` to be `2.4.0-snapshot.20220830.10494.0.4622de48`.

1.
   - On Windows, install the new SDK using the installer on the [releases page]. This will typically be the asset 
     named `daml-sdk-$VERSION-windows.exe` (located on the [DAML releases](https://github.com/digital-asset/daml/releases) page). 
     Please ensure that `$VERSION` is expanded correctly!.

   - On MacOS/Linux (please ensure that `$VERSION` is expanded correctly!):
   ```
   curl -sSL https://get.daml.com/ | sh -s "$VERSION"
   ```
   
   > ## Tips for Windows testing in an ad-hoc machine
   >
   > If you are part of the release rotation, you can create Windows VMs
   > through the [ad-hoc] project. The created machine is a bit raw, though, so
   > here are a few tips to help you along.
   > 
   > First we should clone the git repository https://github.com/DACH-NY/daml-language-ad-hoc and then enter the cloned
   > repo.
   > 
   > If this is your first time doing this, edit `tf/main.tf` and add your username to the `members` field of the 
   > `google_project_iam_binding.machine_managers` resource. Generate and submit a PR with these changes. Once the PR 
   > has been accepted, you should now have permission to create GCP compute instances.
   > 
   > Assuming `direnv` is installed, entering the `daml-language-ad-hoc` project directory will be sufficient to 
   > configure and install the extra software (e.g. the GCP SDK) required for your environment.
   >
   > A new GCP windows instance can be created by running `./ad-hoc.sh temp windows` - this command prints IP address,
   > username and password for the created Windows VM. Save this output. You will need this information later when you 
   > create an RDP connection.
   >
   > ‼️ After starting, it's going to take some time for the machine to be configured (see notes below).
   >
   > Before you may connect to this windows instance, you need to ensure that the VPN is connected. On Mac OSX you can 
   > do this by selecting the preconfigured _Connect GCP Frankfurt full tunnel_ VPN profile.
   > 
   > If you're on a Mac, you can use Microsoft Remote Desktop to connect (this can be installed via the Mac App Store);
   > on Linux, you can use Remmina.
   >
   > Remmina notes: when creating an RDP connection, you may want to specify custom
   > resolution. The default setting is to `use client resolution`. You may notice a
   > failure due to color depth settings. You can adjust those in the settings panel
   > right below the resolution settings.
   >
   > The ad-hoc machines take a bit of time to be available after being reported as
   > created, so be patient for a bit if your first connection attempt(s) fail.
   >
   > At this point, use Firefox to download and install `daml-sdk-$VERSION-windows.exe` from https://github.com/digital-asset/daml/releases
   > (please ensure `$VERSION` is expanded correctly!.
   > 
   > NOTE 1: **Use Firefox for testing.** Windows machines come with both Internet Explorer and Firefox installed. Do 
   > not make the mistake of trying to use Internet Explorer.
   >
   > Ad-hoc machines also come with Node, VSCode and OpenJDK preinstalled, so
   > you don't need to worry about those.
   >
   > NOTE 2: After logging in, **it takes some time for the machine to be configured.** The script that installs Firefox, 
   > Node, VSCode and OpenJDK runs once the machine is available for login. The software you need should appear within 
   > about 10 minutes (an easy way to check is to try to open `D:\` , as this volume is created after all the software 
   > is installed).
   >
   > All of the commands mentioned in this testing section can be run from a simple
   > DOS prompt (start menu -> type "cmd" -> click "Command prompt").
   > 
   > At the end of your Windows testing session, please be sure to terminate the GCP instance by running 
   > `./ad-hoc.sh destroy $ID`. Here `$ID` is the identity for your GCP instance - this is printed when you create your 
   > Windows instance.

1. Prerequisites for running the tests:
    - [Visual Studio Code, Java-SDK](https://docs.daml.com/getting-started/installation.html)
    - [Node.js](https://nodejs.org/en/download/)
      - Just the bare install; no need to build C dependencies.
      - `create-daml-app` doesn't work with the latest version 17.x of node.js.
        If you have `nix` installed, you can use a suitable version of nodejs by
        running `nix-shell -p nodejs-14_x` before running the `npm` commands below.
    - [Maven](https://maven.apache.org)

1. Run `daml version --assistant=yes` and verify that the new version is
   selected as the assistant version and the default version for new projects.

1. Tests for the getting started guide (macOS/Linux **and** Windows). Note: if
   using a remote Windows VM and an RDP client that supports copy/paste, you
   can run through this on both Windows and your local unix in parallel fairly
   easily.

    1. For these steps you will need the getting started documentation for the
       release that you are about to make. This documentation (for the release that you are testing) is published 
       at `https://docs.daml.com/$VERSION/getting-started/index.html`. Please ensure that `$VERSION` is expanded 
       correctly before trying this link!

    1. `daml new create-daml-app --template create-daml-app`

    1. `cd create-daml-app`

       1. `daml start`

    1. In a new terminal (with nodejs configured as above), from the `ui` folder:

       1. `npm install`
           - if this command returns with an exit code of 0, errors may be safely ignored. 

       1. `npm start`

    1. Open two browser windows (you want to see them simultaneously ideally) at `localhost:3000`.

    1. Log in as `alice` in the first window, log in as `bob` in the second window.

    1. In the first window, where you are logged in as `Alice`, follow
       `Bob` by typing their name in the drop down (note that it will
       be `Bob` not `bob`, the former is the global alias, the latter
       is the participant-local username).  Verify that `Bob` appears
       in the list of users `Alice` is following. Verify in the other
       browser window that `Alice` shows up in `Bob`’s network.

    1. In the second window, where you are logged in as `Bob`,
       follow `Alice` by selecting it in the drop down.
       Verify that `Alice` appears in
       the list of users `Bob` is following. Verify in the other
       browser window that `Bob` shows up in `Alice`’s network.

    1. Open the your first feature section of the Getting Started Guide, e.g., from
       `https://docs.daml.com/$VERSION/getting-started/first-feature.html`
       if you did not build docs locally.

    1. In a third terminal window, run `daml studio --replace=always` from the project root
        directory and open `daml/User.daml`.

    1. Copy the `Message` template from the documentation to the end of `User.daml`.

    1. Copy the `SendMessage` choice from the documentation to the
        `User` template below the `Follow` choice.

    1. Save your changes and close VSCode.

    1. In the first terminal window (where `daml start` is running), press 'r'
        (respectively 'r' + 'Enter' on Windows).

    1. In the third terminal window, run `daml studio`.

    1. Create `MessageList.tsx`, `MessageEdit.tsx` and modify
        `MainView.tsx` as described in the documentation.

    1. Verify that you do not see errors in the typescript code in VSCode.

    1. Save your changes and close VSCode.

    1. As before, open two browser windows at `localhost:3000` and log
        in as `alice` and `bob`.

    1. Make `Alice` follow `Bob`.

    1. From `Bob`, select Alice in the `Select a follower` drop down,
        insert `hi alice` in the message field and click on `Send`.

    1. Verify that `Alice` has received the message in the other window.

    1. Make `Bob` follow `Alice`.

    1. From `Alice`, select Bob in the `Select a follower` drop down,
        insert `hi bob` in the message field and click on `Send`.

    1. Verify that `Bob` has received the message in the other window.

    1. You can now close both browser windows and both running processes (`daml
        start` and `npm start`).

    1. Don't forget to run this on the other platform! E.g. if you just ran
        through on Linux or macOS, you still need to run on Windows, and vice
        versa. For testing on Windows instances, please refer to the _Tips for Windows testing in an ad-hoc machine_ 
        notes above.

1. Run through the following test plan on Windows. This is slightly shortened to
   make testing faster and since most issues are not platform specific.

   1. Run `daml new myproject` to create a new project and switch to it using
      `cd myproject`.
   1. Run `daml start`.
   1. Open your browser at `http://localhost:7500`, verify that you can login as
      alice and there is one contract, and that the template list contains
      `Main:Asset` among other templates.
   1. Kill `daml start` with `Ctrl-C`.
   1. Run `daml studio --replace=always` and open `daml/Main.daml`. Verify that
      the script result appears within 30 seconds.
         - you will need to click on the _Script results_ link in the open VS code window in order to verify this
   1. Add `+` at the end of line 26 after `(PartyIdHint "Alice")` and verify that
      you get an error on line 27.

1. On your PR (the one that triggered the release process: on
   [daml] for 1.x releases, and on [assembly] for 2.x
   releases), add the comment:

   > Manual tests passed on Windows.

1. Tests for `quickstart-java` (Linux/macOS)

   While this is no longer the default in the Getting Started Guide, we still test it
   since the process covers things not covered by the new Getting Started Guide
   (e.g. Navigator, Scripts, Maven artifacts, etc.)

    1. Create a new project with `daml new quickstart --template quickstart-java`
       and switch to it using `cd quickstart`.

    1. Verify the new version is specified in `daml.yaml` as the `sdk-version`.

    1. Run `daml start`. Your browser should be opened automatically at
       `http://localhost:7500`. Login as `alice` and verify that there is
       1 contract, and that the templates list contains `Iou:Iou`, `Iou:IouTransfer`,
       and `IouTrade:IouTrade` among other templates.

    1. Close the tab and kill `daml start` using `Ctrl-C`.

    1. Run `daml build`.

    1. In 3 separate terminals (each being in the `quickstart-java` project directory), run:

       1. `daml sandbox --port 6865`

       1. Each of the following:

          1. `daml ledger upload-dar --host localhost --port 6865 .daml/dist/quickstart-0.0.1.dar`

          1. `daml script --ledger-host localhost --ledger-port 6865 --dar .daml/dist/quickstart-0.0.1.dar --script-name Main:initialize --output-file output.json`

          1. `cat output.json` and verify that the output looks like this:
             ```
             ["Alice::NAMESPACE", "EUR_Bank::NAMESPACE"]
             ```
             where `NAMESPACE` is some randomly generated series of hex digits.

          1. `daml navigator server localhost 6865 --port 7500`

       1. `daml codegen java && mvn compile exec:java@run-quickstart -Dparty=$(cat output.json | sed 's/\[\"//' | sed 's/".*//')`

           Note that this step scrapes the `Alice::NAMESPACE` party name from the `output.json` produced in the previous steps.

           > Note: It takes some time (typically around half-an-hour) for our artifacts
           > to be available on Maven Central. If you try running the last command before
           > the artifacts are available, you will get a "not found" error. Trying to
           > build again _in the next 24 hours_ will result in:
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
           >
           > Another common problem is that artifacts fail to resolve because of custom
           > Maven settings. Check your `~/.m2/settings.xml` configuration and try
           > disabling them temporarily.

    1. Point your browser to `http://localhost:7500`, login as `alice` and verify
       that there is 1 contract, 1 owned IOU, and the templates list contains `Iou:Iou`, `Iou:IouTransfer`,
       and `IouTrade:IouTrade` among other templates.

    1. Check that `curl http://localhost:8080/iou` returns:
       ```
       {"0":{"issuer":"EUR_Bank::NAMESPACE","owner":"Alice::NAMESPACE","currency":"EUR","amount":100.0000000000,"observers":[]}}
       ```
       where NAMESPACE is again the series of hex digits that you saw before.

    1. Kill all processes.

    1. Run `daml studio --replace=always`. This should open VSCode and trigger
       the Daml extension that's bundled with the new SDK version. (The new
       VSCode extension will not be in the marketplace at this point.)

    1. Open `daml/Main.daml`.

    1. Click on `Script results` above `initialize` and wait for the script
       results to appear.

    1. Add `+` at the end of line 14, after `(PartyIdHint "Alice")` and
       confirm you get an  error in line 15.

    1. Add `1` after the `+` and confirm you get a type error in line 14,
       which says that `Script Party` does not match `Int`.

    1. Delete the `+1` and the `e` in the second `"Alice"` and verify
       that the script results are updated to the misspelled name.

    1. Right click on `eurBank` in line 28 and verify that "Go to Definition"
       takes you to the definition in line 17.

    1. Close all files.

    > Note: when running `daml studio --replace=always`, you force the
    > installation of the VSCode extension bundled with the Daml SDK, and
    > _disable the autoupgrade mechanism in VSCode_. To instruct VSCode to go
    > back to the published version of the extension, including auto-upgrades,
    > you can run `daml studio --replace=published`.

1. On your PR (the one that triggered the release process: on
   [daml] for 1.x releases, and on [assembly] for 2.x
   releases), add the comment:

   > Manual tests passed on [Linux/macOS].

   specifying which platform you tested on.

1. If the release is bad, delete the release from the [releases page]. Mention
   why it is bad as a comment on your PR, and **stop the process here**.

   Note that **the Standard-Change label must remain on the PR**, even if the
   release has failed.

1. Announce the release on `#product-daml` on Slack. For a stable release,
   direct people to the release blog post; for a prerelease, you can include
   the raw output of the `unreleased.sh` script in a thread after the
   announcement. If there were any errors during testing, but we decided to keep
   the release anyway, report those on the PR and include a link to the PR in the
   announcement.

For a stable release, you need to additionally:

1. Go to the [releases page] and remove the prerelease marker on
   the release. Also change the text to
   ```See [the release notes blog]() for details.```
   adding in the direct link to this version's [release notes]. Documentation
   for this release will be added to docs.daml.com on the next hour.

1. Coordinate with product (& marketing) for the relevant public
   announcements (Daml Forum, Twitter, etc.).

1. Documentation is published automatically once the release is
   public on GitHub, though this runs on an hourly cron.

Thanks for making a release!

[assembly]: https://github.com/DACH-NY/assembly
[canton]: https://github.com/DACH-NY/canton
[`LATEST`]: https://github.com/digital-asset/daml/blob/main/LATEST
[checklist]: https://docs.google.com/document/d/1RY2Qe9GwAUiiSJmq1lTzy6wu1N2ZSEILQ68M9n8CHgg
[release planning]: https://docs.google.com/document/d/1FaBFuYweYt0hx6fVg9rhufCtDNPiATXu5zwN2NWp2s4/edit#
[daml]: https://github.com/digital-asset/daml
[release notes]: https://daml.com/release-notes/
[releases page]: https://github.com/digital-asset/daml/releases
[testing]: #testing
