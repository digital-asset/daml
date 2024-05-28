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
