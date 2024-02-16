# Release of Daml DAML_VERSION
Use one of the following topics to capture your release notes. Duplicate this file if necessary.
Add [links to the documentation too](https://docs.daml.com/DAML_VERSION/about.html).

You text does not need to be perfect. Leave good information here such that we can build release notes from your hints.
Of course, perfect write-ups are very welcome.

You need to update this file whenever you commit something of significance. Reviewers need to check your PR
and ensure that the release notes and documentation has been included as part of the PR.

## Bugfixes

## What’s New

# Release of Daml 2.8.1

## Bugfixes

## What’s New

# Release of Daml 2.9.0

## Bugfixes

## What’s New

* interface instances defined in templates ("retroactive interface instances")
  are now deprecated and support for this feature will be removed in an upcoming release.

    * The deprecation warning can be turned off by adding the following snippet
      at the top of a file that uses this feature:

      ```daml
      `{-# OPTIONS -Wno-retroactive-interface-instances #-}`
      ```

* 'agreement' declarations in template definitions are now deprecated and
  support for this feature will be removed in an upcoming release. Users are
  encouraged to remove these declarations from their code.

    * The deprecation warning can be turned off by adding the following snippet
      at the top of a file that uses this feature:

      ```daml
      `{-# OPTIONS -Wno-template-agreement #-}`
      ```

# Release of Daml 3.0.0

## Bugfixes

## What’s New
