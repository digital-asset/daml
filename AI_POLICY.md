# AI Policy

This policy applies to all contributions — code, issues, reviews, comments, docs — to this repository, regardless of whether you are a maintainer or an external contributor.

Using AI tools is allowed and often useful. The rules below exist to protect the quality of the codebase and the time of reviewers and other contributors.

## You are accountable for what you submit

You are 100% responsible for every contribution you make under your name, regardless of which tools were involved in producing it. Reviewers review *your* work, not AI output.

You must understand every line of code you submit and be able to explain *why* each change is there. "The AI wrote it" is not an answer.

## Guard reviewers' time

Aggressively self-review AI output before asking anyone else to look at it. If you would not ask a colleague to read a specific patch, do not ask the maintainers either.

Do not open a PR you would not have opened without AI assistance. An AI making code cheap to produce is not a reason to produce more of it.

The same applies to review comments you leave on others' PRs: do not post AI-generated feedback you have not read carefully and are not prepared to defend yourself.

**Issues and PRs that appear to be unreviewed AI output — overlong and padded, referencing things that do not exist, plausible-sounding but wrong, filled with bot-style prose, or (in the case of security reports) describing a vulnerability that does not reproduce against the actual code — may be closed without further discussion.** Maintainers are not obligated to explain, respond to, or debate the merits of such submissions. Repeated submissions of this kind may lead to being blocked from the repository.

If you believe a closure was in error, a single comment with a concrete, verifiable correction is welcome. Further back-and-forth is at maintainers' discretion.

## PRs opened autonomously by AI require two human reviews

If a PR is opened by a bot account or created through an automated workflow — i.e. without a named human in the loop — it must receive approvals from **two** human reviewers before merging. PRs opened by a named human follow the normal single-approval rules, including those where AI helped produce the changes. The named human author is responsible for the PR contents as always.

Fully deterministic automated workflows (dependency bumps, submodule bumps, and the like) are not subject to this rule.

## Attribution

There is no requirement to mark AI-assisted contributions as such.

That said, do not hide it if asked. If a reviewer asks how a piece of code came about, answer honestly.

## A note for new contributors

If you are picking up a `good first issue` or similar onboarding-oriented task, the task exists partly so that *you* learn the codebase. Using AI to speed up completion of the task may defeat the purpose — for yourself and for the maintainers trying to onboard you. Engage with the code.
