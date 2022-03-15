# Making a Release

Creating a release follows a slightly different process depending on whether
the release is stable or not, and whether it's a legacy 1.x release or a 2.x
release. Testing the release is broadly similar for stable & snapshots, but if
the release is a patch on an old branch you may want to look at the version of
this document that matches the release version for precise testing steps.

Valid commits for a release should come from either the `main` branch or one
of the support `release/a.b.x` branches (e.g. `release/1.0.x` branch is for
patches we backport to the 1.0 release branch).

> **IMPORTANT**: If the release fails, please delete it from the [releases page]
> and write how it failed on the PR.

The most common case is a 2.x snapshot, but if you have any doubt about the
type of release you're doing, please ask on `#team-daml`.

## Making a Snapshot Release (2.x)

1. Open a PR on the [DACH-NY/assembly] repo using the most recent Canton
   snapshot. See the instructions in the README of the [DACH-NY/assembly] repo
   for details.

1. Merge the PR and wait for the corresponding `main` build to finish.

1. Go to the [Testing](#testing) section of this file.

## Making a Stable Release (2.x)

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

1. Go to the [Testing](#testing) section of this file.

## Making a Snapshot Release (1.x)

At this point, snapshot releases on 1.x should be rare, and only meant to be
precursors to stable, patch releases. First, identify which release branch
you're patching, and what the next version number should be. In this example
we'll assume you're patching the 1.18 branch to produce a snapshot (likely a
release candidate) for 1.18.3.

1. Start _from latest `main`_ and run
   ```
   $ ./release.sh snapshot origin/release/1.18.x 1.18.3
   5221a5d13e6d2e4b657abdd951103cd3030fae36 1.18.3-snapshot.20220310.8437.0.5221a5d1
   ```
   The output line needs to be added to [`LATEST`] _on `main`_, either as a new
   entry if there is no existing 1.18.3 line, or as a replacement for the existing
   one if there is. Try to keep the [`LATEST`] file sorted by semver (highest
   veersion on top) and containing only one entry for each version number. Make a
   PR with just the changed [ `LATEST`] file _to the `main` branch_. Add the
   `Standard-Change` label to that PR.

1. Once the PR has built, check that it was considered a release build by our
   CI. You can look at the output of the `check_for_release` job.

1. Merge the PR and wait for the corresponding `main` build to finish. You
   will be notified on `#team-daml`.

1. Go to the [Testing](#testing) section of this file.

## Making a Stable Release (1.x)

A stable release is based on an existing stable release and should use the same
coommit, with a different version number.

1. Go through the [checklist] before making the release. Note that
   the checklist is not available publicly. Since 1.x are old patch releases at
   this point, you may have to adapt the checklist a bit. Usee your best
   judgement; if we're making a patch release on 1.x at this point there should be
   a specific reason for it, which should suggest specific additional tests (e.g.
   a speecific bug we want to fix).

1. Stable releases are promoted from snapshot releases. Open a PR
   that changes the `LATEST` file _on `main`_ to remove the `-snapshot` suffix on the
   corresponding snapshot, and add the `Standard-Change` label.

1. Once the PR has built, check that it was considered a release build by our
   CI. You can look at the output of the `check_for_release` job.

1. The PR **must** be approved by a team lead before merging. As
   of this writing (2022-02-08), @bame-da, @gerolf-da, @cocreature,
   @ray-roestenburg-da or @adriaanm-da.

1. Merge the PR and wait for the corresponding `main` build to finish. You
   will be notified on `#team-daml`.

1. Go to the [Testing](#testing) section of this file.

## Testing

This testing procedure starts once the release is listed on the [releases page].

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
   > `ad-hoc.sh create windows` prints IP address, username and password for the created Windows VM.
   > Save this output. You will need this information later when you create an RDP connection.
   >
   > ‼️ After starting, it's going to take some time for the machine to be configured (see notes below).
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
   > NOTE 1: **Use Firefox for testing.** Windows machines come with both Internet Explorer and Firefox installed. Do not make the mistake of trying to use Internet Explorer.
   >
   > Ad-hoc machines also come with Node, VSCode and OpenJDK preinstalled, so
   > you don't need to worry about those.
   >
   > NOTE 2: After logging in, **it takes some time for the machine to be configured.** The script that installs Firefox, Node, VSCode and OpenJDK runs once the
   > machine is available for login. The software you need should appear within about 10 minutes. (An easy way to check is to try to open `D:\` , as it is created after all the software is installed.)
   >
   > All of the commands mentioned in this document can be run from a simple
   > DOS prompt (start menu -> type "cmd" -> click "Command prompt").

1. Prerequisites for running the tests:
    - [Visual Studio Code, Java-SDK](https://docs.daml.com/getting-started/installation.html)
    - [Node.js](https://nodejs.org/en/download/)
      - Just the bare install; no need to build C dependencies.
      - `create-daml-app` doesn't work with the latest version 17.x of node.js.
        If you have `nix` installed, you can use a suitable version of nodejs by
        running `nix-shell -p nodejs-14_x` before running the `npm` commands below.

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
       versa.

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
   1. Add `+` at the end of line 26 after `(PartyIdHint "Alice")` and verify that
      you get an error on line 27.

1. On your PR (the one that triggered the release process: on
   [digital-asset/daml] for 1.x releases, and on [DACH-NY/assembly] for 2.x
   releases), add the comment:

   > Manual tests passed on Windows.

1. Tests for `quickstart-java` (Linux/macOS)

   While this is no longer the default getting started guide we still test it
   for now since it covers things not covered by the new GSG
   (Navigator, Scripts, Maven artifacts, …)

    1. Create a new project with `daml new quickstart --template quickstart-java`
       and switch to it using `cd quickstart`.

    1. Verify the new version is specified in `daml.yaml` as the `sdk-version`.

    1. Run `daml start`. Your browser should be opened automatically at
       `http://localhost:7500`. Login as `alice` and verify that there is
       1 contract, and that the templates list contains `Iou:Iou`, `Iou:IouTransfer`,
       and `IouTrade:IouTrade` among other templates.

    1. Close the tab and kill `daml start` using `Ctrl-C`.

    1. Run `daml build`.

    1. In 3 separate terminals, run:

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

    1. Add `+` at the end of line 14, after `"Alice")` and confirm you get an
       error in line 15.

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
   [digital-asset/daml] for 1.x releases, and on [DACH-NY/assembly] for 2.x
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

[DACH-NY/assembly]: https://github.com/DACH-NY/assembly
[`LATEST`]: https://github.com/digital-asset/daml/blob/main/LATEST
[checklist]: https://docs.google.com/document/d/1RY2Qe9GwAUiiSJmq1lTzy6wu1N2ZSEILQ68M9n8CHgg
[digital-asset/daml]: https://github.com/digital-asset/daml
[release notes]: https://daml.com/release-notes/
[releases page]: https://github.com/digital-asset/daml/releases
