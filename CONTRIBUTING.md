# Contributing to DAML

Welcome! This page gives a high-level overview of how to contribute to the development of DAML.

There are many ways you can contribute beyond coding. For example, you can report problems, clarify [issues](https://github.com/digital-asset/daml/issues), and write documentation. If you're completely new to open source development, the [Open Source Guides](https://opensource.guide) is a great place to start.

## Working on the codebase

For information on how to build, test, and work on the codebase, see ["Contributing to DAML" in the README](./README.md#contributing-to-daml).

## Code of conduct

This project and everyone participating in it is governed by the [DAML Code of Conduct](./CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [community@digitalasset.com](mailto:community@digitalasset.com).

## Git conventions

For Git commit messages, our principle is that `git log --pretty=oneline` should give readers a clear idea of what has changed and the detailed descriptions should help them understand the rationale. To achieve this:

* Commits must have a concise, imperative title, e.g.:
  * *Fix performance regression in …*
  * *Improve explanation of …*
  * *Remove module X because it is not used.*
* Commits should have a description that concisely explains the rationale and context for the change if that is not obvious.
* The first line of the description should include a reference to the fixed issue `(#XXX)`.
* The git logs are not intended for user-facing change logs, but should be a useful reference when writing them.

## Pull request checklist

- Read this document (contribution guidelines).
- Does your PR include appropriate tests?
- Make sure your PR title and description makes it easy for other developers to understand what the contained commits do. The title should say what the changes do. The description should expand on what it does (if not obvious from the title alone), and say why it is being done.
- If your PR corresponds to an issue, add “Fixes #XX” to your pull request description. This will auto-close the corresponding issue when the commit is merged into master and tie the PR to the issue.
- If your PR includes user-facing changes, the squashed commit for the PR must include in its body a section between the ``CHANGELOG_BEGIN`` and ``CHANGELOG_END`` tags that includes relevant changelog entries, where each entry starts with the component to which it belongs in square brackets. Use RST to format links as this text will be added to the changelog upon release.
- If your PR does not include user-facing changes, you still need to include a changelog section, but it can be empty, i.e. it is valid for the `CHANGELOG_END` to be right after the `CHANGELOG_BEGIN` line.

The following is an example of a well-formed commit, including the description (first line) and a body that includes changelog additions:

      Introduced a new API for package management (#1311)

      Also fixes a typo in the Scala bindings documentation

      CHANGELOG_BEGIN

      - [Sandbox] Introduced a new API for package management.
      See `#1311 <https://github.com/digital-asset/daml/issues/1311>`__.

      CHANGELOG_END

If you want to amend an existing changelog entry part of a PR already merged on master, do so by adding a ``WARNING`` to your changelog additions:

      CHANGELOG_BEGIN

      WARNING: replace existing changelog entry "Introduced a new API for package management" with the following.

      - [Sandbox] Introduce a new API for party management.
      See `#1311 <https://github.com/digital-asset/daml/issues/1311>`__.

      CHANGELOG_END

If you want to verify the changelog entries as described by a range of Git revisions, you can use the `unreleased.sh` script. In most cases, to see the entries added as part of commits added since branching off of `master`, you can run:

    ./unreleased.sh master..

## Working with issues

We use issues and [pull requests](https://help.github.com/articles/about-pull-requests/) to collaborate and track our work. Anyone is welcome to open an issue. If you just want to ask a question, please ask away on [Stack Overflow](https://stackoverflow.com/questions/tagged/daml) using the tag `daml`.

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

The DAML Language team uses labels to indicate priority (the DAML Runtime team does not):

- `language/now`
- `language/soon`
- `language/later`

You can see all labels [here](https://github.com/digital-asset/daml/labels).

### Milestones

In addition to labels, we group issues into *milestones*. The DAML Language team has all issues in a single *Language* milestone; the DAML Runtime team uses them to group work efforts (*Add PostgreSQL backend to the Ledger* for example). *Maintenance* and *Backlog* are special milestones.

Issues without a milestone are treated as in need of triaging.

You can see all the active milestones [here](https://github.com/digital-asset/daml/milestones).

## Discussions

Please hold discussions that are relevant to DAML development and not confidential in GitHub issues. That way, anyone who wants to contribute or follow along can do so. If you have private discussions, please summarise them in an issue or comment to an issue.

You can also join a `#daml-contributors` channel on our Slack: [damldriven.slack.com](https://damldriven.slack.com/sso/saml/start).

# Thank you!

Thank you for taking the time to contribute!
