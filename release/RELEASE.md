# Making a Release

First, you need to decide whether you are making a technical snapshot
("prerelease" at the github level, hereafter "snapshot") or an officially
supported release (hereafter "stable release"). For the latter, there are
extra steps marked as **[STABLE]** in the following instructions. You have to
skip those if you are making a snapshot release, that is, one intended mostly
for internal use and early testing, but which makes no promises about future
compatibility.

In either case, before going through the following list, you need to know which
commit you want to create the release from, `$SHA`. For a stable release, it is
highly recommended that this be the same commit as an existing snapshot, and
these instructions will be written from that perspective.

Valid commits for a release should come from either the `master` branch or one
of the support `release/a.b.x` branches (e.g. `release/1.0.x` branch is for
patches we backport to the 1.0 release branch).

> **IMPORTANT**: If the release fails, please do not just abandon it. There are
> some cleanup steps that need to be taken.

1. **[STABLE]** Coordinate with the product and marketing teams to define
   release highlights, tweets, blog posts, as well as timeline for publishing
   the release. Define a version number, `$VERSION`. As a starting point, find
   out the sha of the reference version (previous stable release in the same
   release branch), say `$PREV_SHA`, and run:
   ```
   ./unreleased.sh $PREV_SHA $SHA
   ```

1. Pull the latest master branch of the `daml` repository and create a new,
   clean branch off it. Now, we have three possible cases:

   - For a stable release, just remove the snapshot suffix from the latest
     entry for that branch in the LATEST file, replacing the existing line.

   - If you are making the first snapshot for a new target version (i.e. there
     is no snapshot for it yet), add a new line for that version. It does not
     matter to the process where that line is, but try to keep versions sorted
     by version number.

   - If you are making a new snapshot for an existing target version, i.e. the
     stable part of the version number is the same, replace the existing line.

   In both of the latter cases, you can get the "snapshot" part of the version
   tag by running `./release.sh snapshot $SHA`.

1. **[STABLE]** In `docs/source/support/release-notes.rst`, add a new header
   and label for the new version. (See previous releases as examples.)

   Note that the release notes page is not version dependent, i.e. you are
   editing the one and only release notes page. If the release your are making
   is a patch on a support branch, it should be included in-between the next
   "minor" release and the latest patch to the branch your are targeting.

   Once we are ready to make a release stable, preliminary release
   notes will already have been published to the blog, e.g., the
   preliminary release notes for 1.0 were at
   https://blog.daml.com/release-notes/1.0.

   These release notes now have to be converted to Rst so we can
   include them in the documentation. You can do that manually or you
   can use `pandoc` to create a first version and then fine tune
   that. For the latter, download everything inside `<div
   class="section post-body">` from the web page displaying the
   release notes and save it as `release_notes.html`. Then you can run
   `pandoc release_notes.html -o release_notes.rst`. Now copy those
   release notes under the header you created above in
   `docs/source/support/release-notes.rst` and start editing
   them. Here are a couple of things that you should pay attention to:

   1. Try to match the formatting of previous release notes.
   1. Make sure that links from the release notes to our documentation
      point to the documentation for the version you are about to
      release. In particular, this means that in Rst terminology all
      of these are external links.
   1. Pandoc does not seem to preserve markup of inline code blocks so
      you will have to manually wrap them in double backslashes.

1. Once this is done, create a GitHub pull request (PR) with the above changes
   to the `LATEST` and (for a stable release) `release-notes.rst` files. It is
   important that your PR changes no other file. Your PR also needs to have the
   `Standard-Change` label. Note that if the build failed because of this, you
   should be able to add the label and "rerun failed checks" in a few seconds;
   there is no need to restart an entire build cycle.

1. Once the PR has built, check that it was considered a release build by our
   CI. You can do that by looking at the output of the `check_for_release`
   build step.

1. Get a review and approval on your PR and then merge it into master.
   **[STABLE]** For a stable release, the approval **MUST** be from the team
   lead of the Language, Runtime or Product team.

1. Once the CI checks have passed for the corresponding master build, the release
   should be available on Maven Central and GitHub, and have a Git tag.
   The release should be visible on GitHub with _prerelease_ status, meaning it's
   not yet ready for users to consume. The release notes should not be defined yet
   and will be adjusted later on. Maven central has a delay of around 20 minutes
   until the new version is visible. (**Note:** The 20-minute delay is for
   artifacts to be available through e.g. `mvn build`; the delay for artifacts to
   show up in web searches on the Maven Central website is up to two hours. Do not
   worry if the artifacts do not show on the website yet.)

1. On Windows, install the new SDK using the installer on
   https://github.com/digital-asset/daml/releases.

   On macOS/Linux:
   ```
   curl -sSL https://get.daml.com/ | sh -s $(cat LATEST | gawk '{print $2}')
   ```

   Note: this assumes you have the up-to-date `LATEST` file, either because
   you just checked out master or because you're still on the release PR
   commit.

1. Windows prerequisites for running the tests:
    - [Visual Studio Code, Java-SDK](https://docs.daml.com/getting-started/installation.html)
      - The above link takes you docs.daml.com's "getting started" installation guide;
    - [Maven](https://maven.apache.org/install.html)
      - You may have to manually set the  environment variable `JAVA_HOME`;
      - For example, assuming the Zulu Java-SDK, something like
        `C:\Program Files\Zulu\zulu-14`;
    - [Node.js](https://nodejs.org/en/download/)
      - Just the bare install; you don't need Visual Studio build
        tools for compiling C dependencies (and trying to install them
        takes forever and in the end hangs it seems);
    - [Yarn](https://classic.yarnpkg.com/en/docs/install/)
      - Install Node.js first.

1. Run `daml version --assistant=yes` and verify that the new version is
   selected as the assistant version and the default version for new projects.

1. Tests for the getting started guide (macOS/Linux **and** Windows)

    1. For these steps you will need the documentation for the
       release you are about to make. Documentation is published at
       every hour so if you wait for a bit you can go to
       https://docs.daml.com/$VERSION/getting-started/index.html.
       Otherwise, check out the commit that you are referencing in the `LATEST` file
       and build documentation locally via `./docs/scripts/preview.sh`.

    1. Create a new project using `daml new create-daml-app create-daml-app`
       and switch to the project directory using `cd create-daml-app`.

    1. Build the project using `daml build`.

    1. Run the JavaScript codegen using `daml codegen js .daml/dist/create-daml-app-0.1.0.dar -o daml.js`.

    1. Install yarn dependencies using `cd ui && yarn install`.

    1. Run `daml start` from the project root directory.

    1. In a separate terminal run `yarn start` from the `ui` directory.

    1. Open two browser windows (you want to see them simultaneously ideally) at `localhost:3000`.

    1. Log in as `Alice` in the first window, log in as `Bob` in the second window.

    1. Make `Alice` follow `Bob`. Verify that `Bob` appears in the
       list of users `Alice` is following. Verify in the other
       browser window that `Alice` shows up in `Bob`’s network.

    1. Make `Bob` follow `Alice`. Verify that `Alice` appears in
       the list of users `Bob` is following. Verify in the other
       browser window that `Bob` shows up in `Alice`’s network.

    1. Kill the `daml start` process and the `yarn start` process.

    1. Open the your first feature section of the GSG, e.g., from
       https://docs.daml.com/$VERSION/getting-started/first-feature.html
       if you did not build docs locally.

    1. Run `daml studio --replace=always` from the project root
       directory and open `User.daml`.

    1. Copy the `Message` template from the documentation to the end of `User.daml`.

    1. Copy the `SendMessage` choice from the documentation to the
       `User` template below the `Follow` choice.

    1. Close VSCode.

    1. Run `daml build && daml codegen js .daml/dist/create-daml-app-0.1.0.dar -o daml.js`.

    1. From the `ui` directory run `yarn install --force --frozen-lockfile`.

    1. Run `code .` from the project root directory (the extension is
       already installed, no need to use `daml studio`).

    1. Create `MessageList.tsx`, `MessageEdit.tsx` and modify
       `MainView.tsx` as described in the documentation.

    1. Verify that you do not see errors in the typescript code in VSCode.

    1. Close VSCode.

    1. Run `daml start` from the project root directory.

    1. In a separate terminal, run `yarn start` from the `ui` directory.

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
       start` and `yarn start`).

    1. Don't forget to run this on the other platform! E.g. if you just ran
       through on Linux or macOS, you still need to run on Windows, and vice
       versa.

1. Tests for `quickstart-java` (Linux/macOS)

   While this is no longer the default getting started guide we still test it
   for now since it covers things not covered by the new GSG
   (Navigator, scenarios, Maven artifacts, …)

    1. Create a new project with `daml new quickstart quickstart-java`
       and switch to it using `cd quickstart`.

    1. Verify the new version is specified in `daml.yaml` as the `sdk-version`.

    1. Run `daml start`. Your browser should be opened automatically at
       `http://localhost:7500`. Login as `Alice` and verify that there is
       1 contract and 3 templates. Close the tab and kill `daml start` using
       `Ctrl-C`.

    1. Run `daml build`.

    1. In 3 separate terminals (since each command blocks), run:

       1. `daml sandbox --wall-clock-time --port 6865 .daml/dist/quickstart-0.0.1.dar`
       1. `daml script --dar .daml/dist/quickstart-0.0.1.dar --script-name Setup:initialize --ledger-host localhost --ledger-port 6865 --wall-clock-time && daml navigator server localhost 6865 --port 7500`
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
   1. Run through the tests for the getting started guide described above.

1. On your PR, add the comment:

   > Manual tests passed on Windows.

1. If the release is bad, delete the release from [the releases
   page](https://github.com/digital-asset/daml/releases). Mention why it is bad
   as a comment on your PR, and **stop the process here**.

1. **[STABLE]** Go to [the releases page](https://github.com/digital-asset/daml/releases)
   and add the release notes. This unfortunately involves translating the
   release notes from rst to markdown. Once the release has been adequately
   tested, untick the `pre-release` box.

1. Announce the release on the relevant internal Slack channels (#product-daml,
   \#team-daml). For a stable release, direct people to the release blog post;
   for a prerelease, you can include the raw output of the `unreleased.sh`
   script as explained above.

1. **[STABLE]** Coordinate with product (& marketing) for the relevant public
   announcements (public Slack, Twitter, etc.).

1. **[STABLE]** Documentation is published automatically once the release is
   public on Github, however it runs on an hourly job and takes about 20
   minutes to complete, so it could take up to an hour and a half depending on
   when the prerelease tag was removed.

Thanks for making a release!
