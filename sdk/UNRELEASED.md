# Release of Daml DAML_VERSION
Use one of the following topics to capture your release notes. Duplicate this file if necessary.
Add [links to the documentation too](https://docs.daml.com/DAML_VERSION/about.html).

You text does not need to be perfect. Leave good information here such that we can build release notes from your hints.
Of course, perfect write-ups are very welcome.

You need to update this file whenever you commit something of significance. Reviewers need to check your PR
and ensure that the release notes and documentation has been included as part of the PR.

# Release of Daml 2.10.2

## Bugfixes

### create-daml-app Dependency Security Fix

The package.json dependencies for create-daml-app were underspecified, exposing users and CI to potential supply-chain attacks. This has been resolved by including a package-lock.json file with the create-daml-app template to ensure consistent and secure dependency versions.

## What’s New
