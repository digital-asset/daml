---
name: Update critical dependencies between SDK and Canton
about: This template outlines the workflow for updating critical dependencies that must be synchronized between the SDK and Canton repositories. Examples include gRPC, Pekko, OpenTelemetry, etc.
title: "Update ..."

---

<!-- list the dependencies to be updated here -->

Note: Since the process involves synchronous changes to both the SDK and Canton repositories, it may conflict with the daily SDK update task. To avoid issues, please coordinate with the person responsible for the SDK update via the #team-sdk-update Slack channel."

### Steps:

- [ ] Create a _dependencies SDK PR_ to update the relevant dependencies.
   - Leave the PR in draft mode initially (to prevent accidental early merging).
   - In the PR description, list the updated dependencies and their new versions
   - Link to this issue.

- [ ] Ensure the _dependencies SDK PR_ passes all CI checks.

- [ ] Create a _snapshot PR_ to generate an ad-hoc snapshot based on the _dependencies SDK PR_.
  - Follow the instructions in [RELEASE.md](https://github.com/digital-asset/daml/blob/main/sdk/release/RELEASE.md#creating-an-sdk-release).
  - In the PR title, indicate that this is for testing the _dependencies SDK PR_ and include its PR number (e.g., "Ad-hoc snapshot for testing SDK dependencies PR #1234").

- [ ] Obtain approval for the _snapshot PR_ from a [release owner](https://github.com/orgs/digital-asset/teams/daml-release-owners).

- [ ] Merge the _snapshot PR_ and wait until the release process completes.

- [ ] Create a draft _dependencies Canton PR_ with the following updates:
  - Update Canton to use the freshly created ad-hoc snapshot.
  - Copy the [sdk/maven_install_2.13.json](https://github.com/digital-asset/daml/blob/main/sdk/maven_install_2.13.json) file from your _dependencies SDK PR_ to the Canton repository root.
  - In the PR description, link to the _dependencies SDK PR_ together with this isssue.

- [ ] Ensure the _dependencies Canton PR_ passes all CI checks. You may need to iterate by updating additional dependencies in the _dependencies SDK PR_ and repeating the previous steps.

- [ ] Get the _dependencies Canton PR_ reviewed and approved by @rgugliel-da.

- [ ] Mark the _dependencies SDK PR_ as ready for review, obtain approval from a [release owner](https://github.com/orgs/digital-asset/teams/daml-release-owners), and merge it.

- [ ] Create a proper snapshot from the release branch you're working on (typically `main`).

- [ ] Update the _dependencies Canton PR_ to use the new proper snapshot.

- [ ] Merge the _dependencies Canton PR_.

- [ ] close this issue.


