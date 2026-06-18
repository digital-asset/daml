# Open Source Software Compliance

## Overview

We currently use an asynchronous, daily cron job to check the compliance of our
libraries (and check for vulnerabilities at the same time). If the NOTICES file
needs changing, the cron job will generate a PR to update it.

The cron job leverages Bazel to generate the list of dependencies, and relies
on BlackDuck to flag license violations and security advisories.

## Licenses

Which licenses are or are not acceptable is managed at the Blackduck level.

## What if the check fails?

Checks can fail for a number of reasons. Here are the common ones:

- A library is using a license we don't allow. Check with security & legal to
  see if the license can be added; if not, remove the dependency.
- A library is incorrectly classified on BlackDuck: it should have an allowed
  license, but somehow the information on BlackDuck disagrees with that.
  Contact Security to sort it out.
- A library triggers a security notice. That will depend on the specific issue;
  in general, upgrading the library may help.
