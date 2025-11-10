.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _daml-assistant-flags:

Daml Compiler flags
####################

``build``
==============
.. _daml-build-flags:

- | ``-o,--output FILE``
  | Optional output file (defaults to ``.daml/dist/<package-name>-<package-version>.dar``)
- | ``--incremental ARG``
  | Enable incremental builds
  | Can be set to "yes", "no" or "auto" to select the default (no)
- | ``--init-package-db ARG``
  | Initialize package database
  | Can be set to "yes", "no" or "auto" to select the default (yes)
- | ``--enable-multi-package ARG``
  | Enable/disable multi-package.yaml support
  | Can be set to "yes", "no" or "auto" to select the default (yes)
- | ``--all``
  | Build all packages in multi-package.yaml
- | ``--no-cache``
  | Disables cache checking, rebuilding all dependencies
- | ``--multi-package-path FILE``
  | Path to the multi-package.yaml file

Includes :ref:`General Daml Compiler flags <general-damlc-flags>`

``clean``
==============

- | ``--all``
  | Clean all packages in multi-package.yaml
- | ``--enable-multi-package ARG``
  | Enable/disable multi-package.yaml support
  | Can be set to "yes", "no" or "auto" to select the default (yes)
- | ``--multi-package-path FILE``
  | Path to the multi-package.yaml file

``test``
=============

- | ``--files FILE``
  | Only run test declarations in the specified files.                     
- | ``--all``
  | Run tests in current project as well as dependencies
- | ``--load-coverage-only``
  | Don't run any tests - only load coverage results from files and write the aggregate to a single file.
- | ``--show-coverage``
  | Show detailed test coverage
- | ``--color``
  | Colored test results
- | ``--junit FILENAME``
  | Filename of JUnit output file
- | ``-p,--test-pattern PATTERN``
  | Only scripts with names containing the given pattern will be executed.
- | ``--transactions-output ARG``
  | Filename to which the transaction list should be output
- | ``--load-coverage ARG``
  | File to read prior coverage results from. Can be specified more than once.
- | ``--save-coverage ARG``
  | File to write final aggregated coverage results to.
- | ``--coverage-ignore-choice ARG``
  | Remove choices matching a regex from the coverage
  | report. The full name of a local choice takes the
  | format '<module>:<template name>:<choice name>',
  | preceded by '<package id>:' for nonlocal packages.

Includes :ref:`General Daml Compiler flags <general-damlc-flags>`

General Daml Compiler flags
===========================
.. _general-damlc-flags:

The following flags are shared by many Daml commands, which reference this section above.

- | ``--include INCLUDE-PATH``
  | Path to an additional source directory to be included
- | ``--package-db LOC-OF-PACKAGE-DB``
  | Use package database in the given location
- | ``--access-token-file PATH``
  | Path to the token-file for ledger authorization.
- | ``--debug``
  | Set log level to DEBUG
- | ``--log-level ARG``
  | Set log level. Possible values are DEBUG, INFO, WARNING, ERROR
- | ``--detail LEVEL``
  | Detail level of the pretty printed output (default: 0)
- | ``--ghc-option OPTION``
  | Options to pass to the underlying GHC
- | ``--typecheck-upgrades ARG``
  | Typecheck upgrades. Can be set to "yes", "no" or "auto" to select the default (yes)
- | ``--upgrades UPGRADE_DAR``
  | Set DAR to upgrade
- 
  ``-W ARG``

  Turn an error into a warning with ``-W<name>`` or ``-Wwarn=<name>`` or ``-Wno-error=<name>``

  Turn a warning into an error with ``-Werror=<name>``

  Disable warnings and errors with ``-Wno-<name>``
  
  Available names are:
    
    - ``deprecated-exceptions``
    - ``crypto-text-is-alpha``
    - ``upgrade-interfaces``
    - ``upgrade-exceptions``
    - ``upgrade-dependency-metadata``
    - ``upgraded-template-expression-changed``
    - ``upgraded-choice-expression-changed``
    - ``could-not-extract-upgraded-expression``
    - ``unused-dependency``
    - ``upgrades-own-dependency``
    - ``template-interface-depends-on-daml-script``
    - ``template-has-new-interface-instance``

- | ``--ignore-data-deps-visibility ARG``
  | Ignore explicit exports on data-dependencies, and
  | instead allow importing of all definitions from that
  | package (This was the default behaviour before Daml
  | 3.3)
  | Can be set to "yes", "no" or "auto" to select the default (no)
