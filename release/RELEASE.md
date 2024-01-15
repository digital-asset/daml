# Making a Release

Please read this document carefully.

At a high level, a release goes through the following steps. All releases must
use the same version number for stable releases; there is more leeway for
snapshots and RCs.

1. A "daml repo" release, orchestrated from this repository.
2. A "canton repo" release, where the code in the release commit must have a
   dependency on the artifacts produced by step 1.
3. An "assembly repo" release, which combines the daml and canton release
   artifacts and creates the GitHub Release on the daml repo.
4. A "daml-finance repo" release. This is independant of the other steps on a
   technical level.
5. A "docs repo" release to publish documentation on
   [docs.daml.com](https://docs.daml.com).
6. Manual tests and approval of the final release artifacts.

Note that these are the technical steps to create the release artifacts. The
release process as a whole is quite a bit larger and involves synchronization
between various teams about exactly what we want to include in a release, when
we want to release it, communication to various stakeholders, etc. For details
on the broader process, see the [release planning] document.

For the more detailed technical steps, we need to distinguish between three
cases:

a. A new minor release.
b. A patch release on an existing minor version.
c. The weekly snapshot.

The way in which we get to the final artifact differs based on the three cases
above, but the manual testing steps are always the same. As such, they have
their own section at the end of this document.

## Minor Release

For a minor release, we will usually set a target date and a target scope. When
we get "close enough" to the target scope or date, we create the release
branches for the future release, branching off from `main`. There is no hard rule about
when this step happens, and there may be a few days between repos.

In the [daml] repo, the release branch for minor version `A.B` must be named
`release/A.B.x`, e.g.  `release/2.7.x`. In the [canton] repo, the release
branch is named `release-line-A.B`, e.g. `release-line-2.7`. In both repos, we
have special "branch protection rules" set up in GitHub for branches with those
names.

> Note: This also means you should not create branches with those (or similar)
> names if they are not meant to be release branches.

When the release branches contain all the required changes and there are no
blockers left, we create a release candidate (RC). This step can be repeated
multiple times: if issues are found with the current RC, we'll add patches to
the release branches, and create a new RC.

> Note: In most cases work should not be done on a release branch directly.
> Changes should go into the `main` branch first, and then be backported to the
> release branch.

After some time and testing (see the [release planning] document for details),
the RC is accepted and we create a new release from the same code base.

RC version strings in the [daml] repo are indistinguishable from snapshot
names. In the [canton] repo, snapshots are usually names by their date of
production (e.g. `20230401`) whereas RCs contain an explicit `rc` market (e.g.
`2.6.0-rc2`). This can create some confusion, but is hard to change at this
time.

> **When you create the release branch in the [daml] repo, make a PR to `main`
> bumping [`NIGHTLY_PREFIX`] accordingly.**

The steps to create a release are:

1. In the [daml] repo, edit the [`LATEST`] file. The format of that file is a
   commit sha of the code you want to release (at this point typically the tip
   of the release branch), the version number you want to attribute to the
   build, and the words `SPLIT_RELEASE` (for historical reason dating to
   pre-2.0 days).  For a release candidate, one can generate a snapshot version
   string using the `release.sh` script. For example, to create a PR for a
   2.0.0 release candidate:

   ```
   $ git fetch
   $ ./release.sh snapshot origin/release/2.0.x 2.0.0
   ```

   You should put that line in the file in order to preserve semver ordering,
   and overwrite any existing snapshot with the same prefix.

   Stable releases should use the same code as the last RC, so instead of
   adding a new line you should simply remove the `-snapshot.*` part of the
   version string on the appropriate line of the [`LATEST`] file.

2. Make a PR **targeting the `main` branch** with just that one line added,
   touching no other file. Add the `Standard-Change` label to that PR.

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
   (Though rarely these days.)

## Patch Release

On the technical side, a patch release is very similar to a minor release,
except that:

1. The non-technical steps of the broader process are much simpler. See the
   [release planning] document for details.
2. Depending on the nature of the patch, we sometimes skip the RC process and
   go straight for a stable version number. This is a judgement call based on
   the specifics of the changes.

Patch releases are done from the corresponding release branch.

## Weekly Snapshot

> Note: You should not have to make any change to this repo.

The weekly snapshot relies on daily builds from both the [canton] and [daml]
repos, so there is no need to create those manually.

1. On Tuesday, you should see a reminder on Slack (`#team-daml`) with the name
   of the release tester for the week.
1. On Wednesday morning, a cron should create a release PR on the [assembly]
   repo, mentioning you. Please reach out to `@gary` on Slack if that's
   missing.

2. Follow the instructions in the PR description. Merge the PR and wait for the
   corresponding `main` build to finish.

2. Once the [assembly] build is finished, it should post a message to Slack
   with instructions on how to get the release artifacts.

3. Go to the [Testing](#testing) section of this file.

> Note: Documentation for weekly snapshots should be published automatically by
> the [docs] repo cron. This is an hourly cron, however, and it can only run
> after the release artifacts have been created and pushed to GitHub releases
> on the [daml] repo, so it may take some time to appear.

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
   > windows` inside the `daml-language-ad-hoc` project. This command prints IP
   > address, username and password for the created Windows VM. Save this
   > output.  You will need this information later when you create an RDP
   > connection.
   >
   > > ‼️ After starting, it's going to take some time for the machine to be
   > > configured (see notes below).
   >
   > Before you may connect to this windows instance, you need to ensure that
   > the VPN is connected. On macOS you can do this by selecting the
   > preconfigured _Connect GCP Frankfurt full tunnel_ VPN profile.
   >
   > If you're on a Mac, you can use Microsoft Remote Desktop to connect. This
   > can be installed via the Mac App Store or directly
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
   > Once the Windows machine is up and running, use Firefox (in Windows) to
   > download and install `daml-sdk-$VERSION-windows.exe` from the [releases
   > page] (please ensure `$VERSION` is expanded correctly!.
   >
   > Ad-hoc machines come with VSCode and OpenJDK preinstalled, so you don't
   > need to worry about those.
   >
   > NOTE 2: After logging in, **it takes some time for the machine to be
   > configured.** The script that installs Firefox, VSCode and OpenJDK runs
   > once the machine is available for login. The software you need should
   > appear within about 10 minutes (an easy way to check is to try to open
   > `D:\` , as this volume is created after all the software is installed).
   >
   > All the commands mentioned in this testing section can be run from a
   > simple DOS prompt (start menu -> type "cmd" -> click "Command prompt").
   >
   > At the end of your Windows testing session, please be sure to terminate
   > the GCP instance by running `./ad-hoc.sh destroy $ID`. Here `$ID` is the
   > identity for your GCP instance - this is printed when you create your
   > Windows instance.

1. Prerequisites for running the tests:
    - [Visual Studio Code, Java-SDK](https://docs.daml.com/getting-started/installation.html)
    - [Maven](https://maven.apache.org)

1. Run `daml version --assistant=yes` and verify that the new version is
   selected as the assistant version and the default version for new projects.

1. Run through the following test plan on Windows. This is slightly shortened to
   make testing faster and since most issues are not platform specific.

   1. Run `daml new myproject` to create a new project and switch to it using
      `cd myproject`.
   1. Run `daml start`.
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

    1. Create a new project with `daml new quickstart --template quickstart-java`
       and switch to it using `cd quickstart`.

    1. Verify the new version is specified in `daml.yaml` as the `sdk-version`.

    1. Kill `daml start` using `Ctrl-C`.

    1. Run `daml build`.

    1. In 3 separate terminals (each being in the `quickstart-java` project directory), run:

       1. In Terminal 1 run `daml sandbox --port 6865`

       1. In Terminal 2, run each of the following:

          1. `daml ledger upload-dar --host localhost --port 6865 .daml/dist/quickstart-0.0.1.dar`

          1.
          ```sh
          daml script --ledger-host localhost --ledger-port 6865 --dar .daml/dist/quickstart-0.0.1.dar --script-name Main:initialize --output-file output.json
          ```

          1. `cat output.json` and verify that the output looks like this:
             ```json
             ["Alice::NAMESPACE", "EUR_Bank::NAMESPACE"]
             ```
             where `NAMESPACE` is some randomly generated series of hex digits.

       1. In Terminal 3, run:

            ```sh
            daml codegen java && mvn compile exec:java@run-quickstart -Dparty=$(cat output.json | sed 's/\[\"//' | sed 's/".*//')
            ```

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

    1. Click on `Script results` above `initialize` (in the code) and wait for the script
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
    > _disable the auto-upgrade mechanism in VSCode_. To instruct VSCode to go
    > back to the published version of the extension, including auto-upgrades,
    > you can run `daml studio --replace=published`.

1. On your PR (the one that triggered the release process: on
   [daml] for 1.x releases, and on [assembly] for 2.x and 3.x
   releases), add the comment:

   > Manual tests passed on [Linux/macOS].

   specifying which platform you tested on.

1. If the release is bad, ask Gary to delete the release from the [releases
   page]. Mention why it is bad as a comment on your PR, and **stop the process
   here**.

   Note that **the Standard-Change label must remain on the PR**, even if the
   release has failed.

1. Announce the release on `#product-daml` on Slack. For a stable release,
   direct people to the release blog post. If there were any errors during testing,
   but we decided to keep the release anyway, report those on the PR and include a
   link to the PR in the announcement.

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

[Remmina]: https://remmina.org
[`LATEST`]: https://github.com/digital-asset/daml/blob/main/LATEST
[`NIGHTLY_PREFIX`]: https://github.com/digital-asset/daml/blob/main/NIGHTLY_PREFIX
[`release.sh`]: https://github.com/digital-asset/daml/blob/main/release.sh
[ad-hoc]: https://github.com/DACH-NY/daml-language-ad-hoc
[assembly]: https://github.com/DACH-NY/assembly
[canton]: https://github.com/DACH-NY/canton
[checklist]: https://docs.google.com/document/d/1RY2Qe9GwAUiiSJmq1lTzy6wu1N2ZSEILQ68M9n8CHgg
[daml]: https://github.com/digital-asset/daml
[docs]: https://github.com/digital-asset/docs.daml.com
[release notes]: https://daml.com/release-notes/
[release planning]: https://docs.google.com/document/d/1FaBFuYweYt0hx6fVg9rhufCtDNPiATXu5zwN2NWp2s4/edit#
[releases page]: https://github.com/digital-asset/daml/releases
[testing]: #testing
