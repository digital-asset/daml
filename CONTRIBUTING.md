# Contributing to Daml

Welcome and thank you for considering contributing to Daml! This page gives a high-level overview of how to contribute to the development of Daml.

## Where to start from

There are many ways you can contribute beyond coding. For example, you can report problems, clarify [issues](https://github.com/digital-asset/daml/issues), and write documentation. If you're completely new to open source development, the [Open Source Guides](https://opensource.guide) is a great place to start.

For anything apart from trivial changes (like fixing a typo), we recommend making the core contributors aware of your ideas, so that you can iterate on them together and make sure you are working on something that can move swiftly though its review phase without any hiccup. If you already have a clear idea of exactly what you want to work on, [open an issue on GitHub](https://github.com/digital-asset/daml/issues/new/choose) and describe it in detail. If you are not 100% sure yet, you can engage with the team on the [Daml forum](https://discuss.daml.com) if you want to have a first, informal chat before opening a ticket. Once the ticket is open and a core contributor endorses the design you proposed, your contribution is on its path to be accepted after the normal review process.

## Working on the codebase

For information on how to build, test, and work on the codebase, see ["Contributing to Daml" in the README](./README.md#contributing-to-daml).

## Code of conduct

This project and everyone participating in it is governed by the [Daml Code of Conduct](./CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [community@digitalasset.com](mailto:community@digitalasset.com).

## Git conventions

For Git commit messages, our principle is that `git log --pretty=oneline` should give readers a clear idea of what has changed and the detailed descriptions should help them understand the rationale. To achieve this:

* Commits must have a concise, imperative title, e.g.:
  * *Fix performance regression in …*
  * *Improve explanation of …*
  * *Remove module X because it is not used.*
* Commits should have a description that concisely explains the rationale and context for the change if that is not obvious.
* Commit descriptions should include a `Fixes #XX` line indicating what GitHub issue number the commit fixes.
* The git logs are not intended for user-facing change logs, but should be a useful reference when writing them.

## Pull request checklist

* Read this document (contribution guidelines).
* Does your PR include appropriate tests?
* Make sure your PR title and description makes it easy for other developers to understand what the contained commits do. The title should say what the changes do. The description should expand on what it does (if not obvious from the title alone), and say why it is being done.
* If your PR corresponds to an issue, add “Fixes #XX” to your pull request description. This will auto-close the corresponding issue when the commit is merged into main and tie the PR to the issue.

## Working with issues

We use issues and [pull requests](https://help.github.com/articles/about-pull-requests/) to collaborate and track our work. Anyone is welcome to open an issue. If you just want to ask a question, please ask away on [the Daml forum](https://discuss.daml.com).

We encourage everyone to vote on issues that they support or not:

* 👍 - upvote
* 👎 - downvote

When you start working on an issue, we encourage you to tell others about it in an issue comment. If other contributors know that this issue is already being worked on, they might decide to tackle another issue instead.

When you add `TODO` (nice to have) and `FIXME` (should fix) comments in the code, we encourage you to create a corresponding issue and reference it as follows:

* `TODO(#XX): <description>` where `#XX` corresponds to the GitHub issue.
* `FIXME(#XX): <description>` where `#XX` corresponds to the GitHub issue.

### Labels

We use labels to indicate what component the issue relates to (`component/...`). We use some special labels:

- `broken` to indicate that something in the repo is seriously broken and needs to be fixed.
- `discussion` to indicate the issue is to discuss and decide on something.
- `good-first-issue` to indicate that the issue is suitable for those who want to contribute but don't know where to start.

By default, issues represent "work to be done" -- that might be features, improvements, non-critical bug fixes, and so on.

The Daml Language team uses labels to indicate priority (the Daml Runtime team does not):

- `language/now`
- `language/soon`
- `language/later`

You can see all labels [here](https://github.com/digital-asset/daml/labels).

### Milestones

In addition to labels, we group issues into *milestones*. The Daml Language team has all issues in a single *Language* milestone; the Daml Runtime team uses them to group work efforts (*Add PostgreSQL backend to the Ledger* for example). *Maintenance* and *Backlog* are special milestones.

Issues without a milestone are treated as in need of triaging.

You can see all the active milestones [here](https://github.com/digital-asset/daml/milestones).

## Discussions

Please hold discussions that are relevant to Daml development and not confidential in GitHub issues. That way, anyone who wants to contribute or follow along can do so. If you have private discussions, please summarize them in an issue or comment to an issue.

You can also participate in the discussions at the following link: [discuss.daml.com](https://discuss.daml.com/).

## Problems

1. When running tests on `MacOS` you might get a system dialog like this:
   ````
   Do you want the application “openssl” to accept incoming network connections?
   
   Clicking Deny may limit the application’s behaviour. 
   This setting can be changed in the Firewall pane of Security & Privacy preferences.
   
   Deny       Allow
   ````
   (As of 2021.10.29) this is caused by the following test `//ledger/participant-integration-api:participant-integration-api-tests_test_suite_src_test_suite_scala_platform_apiserver_tls_TlsCertificateRevocationCheckingSpec.scala`.  
   The test can succeeds independent of whether `Deny` or `Allow`.  
   If the dialog doesn't appear for you, you've probably already excercised one of these two choices.  
   To check your Firewall settings go to: `System Preferences` -> `Security & Privacy` -> `Firewall` -> `Firewall Options...` (checked on macOS Big Sur 11.5.2).  

## Thank you!

Thank you for taking the time to contribute!
