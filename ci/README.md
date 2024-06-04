# CI

## Overview

We run our CI on Azure Pipelines. Azure Pipelines uses its own variant of YAML
for its ocnfiguration; it's worth getting familiar with their [YAML
documentation][YAML], as well as with their [expression syntax].

[YAML]: https://learn.microsoft.com/en-us/azure/devops/pipelines/yaml-schema/?view=azure-pipelines
[expression syntax]: https://learn.microsoft.com/en-us/azure/devops/pipelines/process/expressions?view=azure-devops

Azure Pipelines allows one to define any number of "pipelines", which are
definitions of what to do (think CircleCI workflow). Each pipeline has its own
entrypoint and conditions for running, as well as its own configuration within
Azure Pipelines (e.g. each has its own set of nvironment variables).

## Entrypoints

The entrypoints we have in this repo, as of 2024-05-28, are:

- `/azure-cron.yml` for the `digital-asset.daml-cron` pipeline. This is a
  hourly cron. For some reason setting it up as a hourly cron did not work four
  years ago so we have a slightly convoluted set up where a separate job within
  Azure Pipelines (not visible in this repo's files) called `cron-workaround CI`
  "maually" triggers this through the API every hour. This entrypoint is
  responsible for generating the `daml-sdk` Docker image (when a new release is
  detected), cleaning up potentially-broken files from the Bazel cache,
  publishing the VSCode Extension (when a new release is detected), copying the
  GitHub downloads stats to GCS to give us some historical perspective, and
  checking that get.daml.com has not been changed.
- `/azure-pipelines.yml` is the `main` branch build (also `main-2.x`). This
  runs only on commits from `main` or `main-2.x` and has access to release
  secrets. The corresponding pipeline is `digital-asset.daml`.
- `/ci/prs.yml` is the entrypoint for PR builds. It is very similar to the main
  branch build (in either case, most of the jobs are defined in
  `/ci/build.yml`), but has access to fewer secrets. It also has a few specific
  jobs such as `check_standard_change_label`.
- `/ci/daily-snapshot.yml` is, as the name suggests, the entrypoint for the
  daily snapshots. Note that, like all releases, these are made from the latest
  commit of the main branch. The corresponding pipeline is named `snapshot` and
  can be manually triggered with sufficient Azure Pipelines permissions. This
  should **not** be done for non-snapshot releases as we want an audit trail of
  those in `LATEST`. Note that despite the name this does not run on a cron.
- `/ci/cron/daily-compat.yml` is the entrypoint for all of our "daily" cron
  jobs, whether they are related to the compatibility tests or not. These
  include the compatibility tests, but also the speedy performance tests (2.x
  only), a daily BlackDuck scan (which optionally udpates the `NOTICES` file),
  the daily code pull from canton, as well as triggering the `snapshot` pipeline
  if needed.
- `/ci/cron/tuesday.yml` runs on Tuesdays to send a Slack message to
  `#team-daml` announcing who will be responsible for the Wednesday release
  testing.
- `/ci/cron/wednesday.yml` open the release testing rotation update PR.

## Special branches

We currently have a few special branches.

### `main`

The `main` branch is where most of the development happens. It currently
(2024-05-28) targets the 3.1.0 release, but that is changeable: when the code
freeze for 3.1.0 nears, the way we'll actually do the code freeze is by
creating a new branch called `release/3.1.x` from the then tip of `main`, and
then change `main` to target 3.2.0.

The `main` branch is also the only one that triggers releases. This means that
the `sdk/LATEST` file on the `main` branch is the source of truth for releases
in this repo, across all versions. When CI detects that a given build is a
release build, all of the build steps will first check out the target release
commit (first column of the `sdk/LATEST` file).

This has two important consequences:
- On the good side, it's very good for auditability: this one file on this one
  branch is the only trigger for releases, so looking at the history of that
  file tells you all you need to know about releases. (In most cases you'll have
  all you need from the current state of the file.)
- On the bad side, Azure Pipelines loads the YML files _before_ we check out
  the target release commit, which means that the YML files involved in making
  a release need to be changed very carefully as they need to keep working with
  older versions. This has not been an issue in practice so far, but certainly
  something to keep in mind.

The `main` branch is the one that runs for most cron jobs, the one exception
being the `digital-asset.daml-daily-compat` pipeline which runs every day on
both the `main` and `main-2.x` branches.  

Note however that the daily-compat `main-2.x` pipeline does not trigger any 
release build. All snapshots (included `main-2.x` ones) are started by 
[this job](https://github.com/digital-asset/daml/blob/18e4e155fb94ac153e08b9f7b141910d8e005e7a/ci/cron/daily-compat.yml#L314-L335)
in the `main` daily-compat pipeline. The job starts two builds of the `snapshot`
pipeline, with two different commits passed as a parameter. As these builds
are all run from `main` (even when building `main-2.x`), they can't be easily
told appart from the [snapshot pipeline page](https://dev.azure.com/digitalasset/daml/_build?definitionId=40). To tell which branch a given build is building: click on the job,
then consult the `check_for_release`'s `out` step. The `release_tag` variable then
makes it clear whether this is a 2.x or 3.x build.

### `main-2.x`

This temporary fork of `main` targets `2.9.0` and will likely move on to target
`2.10.0` when `2.9.0` gets its code freeze. It is similar to `main` in many
ways, but does not trigger releases.

### `release/*`

The `release/*` branches (e.g. `release/2.3.x`) represent the code base of past
minor releases and exist to allow us to do patch releases.

The process of making a stable release will generall involve creating the
`release/*` branch when we start to close in on the code freeze for that
release, with usually a couple more PRs that need to go in.

Therefore, the vast majority of releases are made using a target commit from a
`release/*` branch, while being triggered by a commit getting merged into the
`main` branch.

Release branches do not run CI on their own commits - instead, CI is run on PRs
targeting them, and we enforce linear merges.

## Working with Azure Pipelines

### Understanding what gets built

Azure Pipelines does not build your branch or your PR; instead, what it builds
is the result of merging your branch into its target (in most cases, `main` or
`main-2.x`). This has some nice properties (you don't need to explicitly
rebase/merge to be confident your PR builds against current head), but it can
also cause some subtle issues because **this is done per job**.

Meaning that, within a single build, two separate jobs may not be building the
same code. This is particularly problematic for the platform-independence test,
in rare cases where the various platform jobs don't start at the same time and
the changes on main in-between change the produced DAR file.

### When a build doesn't start

There are two situations where a build will not start for a PR:

- The PR does not match our security rule "has been opened by an account with
  write access". This covers bot-opened PRs (bots can create branches directly
  on the repo but don't count as having write access, because reasons) as well as
  any PR "from a fork", regardless of who opens it (i.e. if you have write access
  but choose to make a fork instead of pushing your branch directly to the repo,
  CI won't start).
- Sometimes either GitHub or Azure Pipelines has a temporary network issue.
  Builds are triggered by GitHub sending events to Azure Pipelines (PR opened,
  new commit pushed, etc.); there is no polling. So if there's any issue with
  that one notification, the build has been "missed" and won't be started.

Remediation depends on the situation. In most cases, if you have write access
to the repo you can trigger a PR build by adding a comment that reads `/azp
run` on the PR. The comment has to be just thoss 8 characters.

Alternatively, in the second case, operations like pushing a new commit or
closing and reopening the PR can trigger a new notification.

### Restarting a failed build

Azure Pipelines should trigger a build on every pull request (but not every
branch). If a build has failed and you believe the failure to be flaky, you can
re-run the build by navigating to the "Checks" tab of your PR, and clicking the
"Re-run failed checks" button in the top right.

**This will only work once the build is finished, whether successfully or
not.** The button does nothing if some jobs (from that build) are still
running. You can identify builds and jobs on the GitHub Checks page by their
name, which is of the form `[Pipeline] ([Job])`, e.g. `PRs
(compatibility_linux)` where `PRs` is the name of the pipeline and
`compatibility_linux` is the job. A build is an instance of running all the
jobs in a pipeline.

Note that the `Re-run all jobs` button reruns all the jobs, which means you
take a chance with the ones that have already succeeded. This is sometimes
necessary, but the only case I can think of is when the platform-independence
test fails because of a race condition.

A PR-triggered build gets canceled if a new commit is pushed to the
corresponding branch.

### Finding logs for a build

From the same Checks tab (or the equivalent for main branch commits), you can
click on the "View more details on Azure Pipelines" link to get access to the
running logs of a job.

Note that logs at this level are per step, not per job. You can look at logs
scrolling by for a running step, or download the entirety of the logs as a text
file with the "View raw log" button in the top right.

On the build page view (when no specific job or step is selected), you can see
the build artifacts. Most of these artifacts are additional logs, presumably
more detailed.

### Managing jobs in Azure Pipeleines

Only a few people have access to Azure Pipelines directly. Those people can
additionally use the Azure Pipelines UI to:

- Cancel a running build. Note that we cannot cancel individual jobs, and the
  cancellation is a request - some stops react more quickly than others.
- Manually start a build from a pipeline on an arbitrary git commit - this is
  easily abusable and the reason why not many people are given access.

Direct access to Azure Pipelines does not help with most routine tasks, e.g. it
does not allow one to restart a failed job while other jobs in the same build
are still running.

### Managing CI pools

Direct access to Azure Pipelines also allows one to manage the pools of CI
machines:

- See how many jobs are running and how many jobs are queued, which may
  indicate a need for more machines. There is no auto-scaling, so scaling may
  need to be done manually.
- Disable (and then re-enable) individual machines in a pool. A disabled
  machine will finish any ongoing job but will not be assigned new jobs.
- Delete a machine from a pool. This removes it from Azure Pipelines, but does
  not free up the corresponding resources on Azure. Prefer [Bracin] for machine
  deletion.
- Add (or remove) "capabilities" to a machine, which is a set of flags that can
  be used in "demands" in job configuration. By default, all jobs require the
  `assignment` capability to be equal to `default` (this is an explicit demand in
  our YAML files, not a statement about Azure Pipelines defaults), and all
  machines start with the `assignment` capability equal to `default` (this is
  explicitly set in our startup scripts in [daml-ci], not a statement about the
  Azure Agent's defaults). Changing capabilities can allow fine-grained
  selection of which PR runs on which machine, which is generally seen as a bad
  thing we should not do, but is occasionally needed while working on the CI
  infrastructure, for example to test out a new version of the base VM that
  machines run from.

Scaling machines requires access to Azure (which is separate from Azure
Pipelines despite the naming similarity), or the feature to be added to
[Bracin].

[Bracin]: https://daml-ci.da-int.net

## Playbook

### When the split_release job fails

When the `split_release` job fails, that's basically unrecoverable (unless it 
fails on the very first thing it tries to push) because that step is not idempotent.
In other words, the release is busted, you'll have to make a new one with a new version
string. This is the reason why we have that `.0.` in the version string.

### Retrying a snapshot that failed because of a flake

Find the commit on `main` from which the `snapshot` or `digital-asset.daml` pipeline was run
(depending on whether this was a daily snapshot triggered by `daily-compat` or a "manual"
snapshot triggered by a modification to the `LATEST` file). Click on the red cross indicating
that the CI failed on that commit. Re-run the relevant pipeline from the "check" page you land
on.

Sometimes all the pipeline jobs are green because GitHub got the notifications for a
successful 2.x after it got the notifications for a failed 3.x. In that case there is 
unfortunately no way to restart the snpashot from github. 
