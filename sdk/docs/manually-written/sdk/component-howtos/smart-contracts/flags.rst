.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


.. _howto-daml-compiler:

The Daml Compiler
#################

At the core of the Daml toolchain lies the Daml Compiler, also known as `damlc`.
Its primary usage is to compile Daml source code, which defines smart contracts
and their behaviours, into a lower-level language called
:ref:`Daml-LF<daml-lf>`, which Canton participants can evaluate in order to run
those smart contracts.

We recommended running the compiler via :subsiteref:`DPM<dpm>` (i.e. through
``dpm build``), or alternatively, by calling ``damlc`` directly. When compiling
code, the Daml Compiler goes through the following stages:


#. The input ``.daml`` :ref:`files<glos-daml-source-files>` are type-checked
   (i.e. it is verified if they are well-formed or not)
#. The type-checked ```.daml`` code is transformed to
   :ref:`Daml-LF<daml-lf-intro>`.
#. The produced Daml-LF is packaged together with its dependencies and their
   metadata into a ``.dar`` :ref:`file<glos-dar-file>`, which essentially is an
   archive of Daml-LF files with metadata.

Build files produced by a call to ``dpm build`` are stored in a ``.daml``
directory, local to the project in which ``dpm build`` was run. They can be
cleaned using ``dpm clean``.

The Daml Compiler also offers testing functionality. It can be invoked either by
first calling ``dpm build`` to obtain a ``.dar`` which is then used in a call to
:ref:`daml-script` (i.e. ``dpm script``), or by calling :ref:`glos-daml-test`
(i.e. ``dpm test``) directly.

.. _daml-assistant-flags:

.. _build-and-test-flags:

Build & test flags
..................

These flags are available for both ``--build`` as well as ``--test``.

- | ``-h``, ``--help``

  | Prints option information when called on the commandline.

- | ``--package-root ARG``

  | :subsiteref:`DPM-specific<dpm>` option.

  | Path to the root of a :ref:`glos-daml-package` containing ``daml.yaml``.
    Using this option, you set the package to build/test. You should prefer the
    ``DAML_PACKAGE`` environment variable over this option. See
    :subsiteref:`dpm-configuration` for more details.

- | ``--project-root ARG``

  | :subsiteref:`DPM-specific<dpm>` option.

  | ``project-root`` is **deprecated**, please use ``--package-root``.

  | Path to the root of a :ref:`glos-daml-package` containing ``daml.yaml``. You
    should prefer the ``DAML_PACKAGE`` environment variable over this option.
    See :subsiteref:`dpm-configuration` for more details.

- | ``--include INCLUDE-PATH``

  | Path to an additional source directory to be included.

- | ``--package-db LOC-OF-PACKAGE-DB``

  | Use :ref:`glos-package-database` in the given location.

- | ``--access-token-file PATH``

  | **Deprecated command****, use :subsiteref:`DPM-specific<dpm>` instead

  | Path to the token-file for ledger authorization.

- | ``--shake-profiling PROFILING-REPORT``

  | Directory for :ref:`glos-shake-profiling-reports`.

- | ``--jobs THREADS``

  | The number of threads to run in parallel. When `-j` is not passed, 1 thread
    is used. If `-j` is passed, the number of threads defaults to the number of
    processors. Use `--jobs=N` to explicitely set the number of threads to `N`.
    Note that the output is not deterministic for > 1 job.

- | ``--debug``

  | Set :ref:`log level<glos-damlc-log-level>` to `DEBUG`.

- | ``--log-level ARG``

  | Set :ref:`log level<glos-damlc-log-level>`. Possible values are `DEBUG`,
    `INFO`, `WARNING`, `ERROR`.

- | ``--detail LEVEL``

  | Detail level of the pretty printed output (default: 0). A higher level of
    detail will pretty print more information from for example ``damlc inspect``
    as well as printing source file locations. See
    :ref:`glos-damlc-detail-level`.

- | ``--ghc-option OPTION``

  | Options to pass to the underlying :ref:`GHC<glos-ghc>`.

- | ``-p,--test-pattern PATTERN``

  | Only scripts with names containing the given pattern will be
    executed.

- | ``--typecheck-upgrades ARG``

  | Typecheck upgrades. Can be set to "yes", "no" or "auto" to select the
    default (True).

- | ``--upgrades UPGRADE_DAR``

  | Set DAR to :ref:`upgrade<glos-dar-upgrades>`>.

- | ``-W ARG``

  | Turn an error into a warning with ``-W<name>`` or ``-Wwarn=<name>`` or
    ``-Wno-error=<name>``. Turn a warning into an error with ``-Werror=<name>``.
    Disable warnings and errors with ``-Wno-<name>``. Available names are:

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

  | Ignore explicit exports on :ref:`glos-data-dependencies`, and instead allow
    importing of all definitions from that :ref:`glos-daml-package` (this was
    the default behaviour before Daml 2.10). Can be set to "yes", "no" or "auto"
    to select the default (False).

.. _daml-build-flags:

Build flags
............

- | All build & test flags :ref:`described above<build-and-test-flags>`.

- | ``-o,--output FILE``

  | Optional output file (defaults to ``<PACKAGE-NAME>.dar``).

- | ``--incremental ARG``

  | Enable :ref:`glos-incremental-builds`. Can be set to "yes", "no" or "auto"
    to select the default (False).

- | ``--init-package-db ARG``

  | Initialize :ref:`glos-package-database`. Can be set to "yes", "no" or "auto"
    to select the default (True).

- | ``--enable-multi-package ARG``

  | Enable/disable :ref:`multi-package.yaml<glos-multi-package>` support
    (enabled by default). Can be set to "yes", "no" or "auto" to select the
    default (True).

- | ``--all``

  | Build all packages in :ref:`multi-package.yaml<glos-multi-package>`.

- | ``--no-cache``

  | Disables cache checking, rebuilding all dependencies.

- | ``--multi-package-path FILE``

  | Path to the :ref:`multi-package.yaml<glos-multi-package>` file.

Test flags
............

- | All build & test flags :ref:`described above<build-and-test-flags>`.

- | ``--files``

  | Only run test declarations in the specified files.

- | ``--all``

  | Run tests in current :ref:`glos-daml-package` as well as dependencies

- | ``--load-coverage-only``

  | Don't run any tests. Only load :ref:`glos-daml-test-coverage` results from
    files and write the aggregate to a single file.

- | ``--show-coverage``

  | Show detailed test :ref:`glos-daml-test-coverage`.

- | ``--color``

  | Colored test results>

- | ``--junit FILENAME``

  | Filename of JUnit output file. This file contains the test output in the
    de-facto standard for test output, JUnit XML file format. See
    https://github.com/testmoapp/junitxml.

- | ``--package-name PACKAGE-NAME``

  | Create package artifacts for the given package name.

- | ``--table-output ARG``

  | Filename to which table should be output. Used to render :subsiteref:`the
   table view<script-results>` of :subsiteref:`Daml Studio<daml-studio>`, but
   usable on the cli as well.

- | ``--transactions-output ARG``

  | Filename to which the transaction list should be output. Used to render
    :subsiteref:`the transaction view<script-results>` of
    :subsiteref:`Daml Studio<daml-studio>`, but usable on the cli as well.

- | ``--load-coverage ARG``

  | File to read prior :ref:`glos-daml-test-coverage` results from. Can be
    specified more than once.

- | ``--save-coverage ARG``

  | File to write final aggregated :ref:`glos-daml-test-coverage` results to.

- | ``--coverage-ignore-choice ARG``

  | Remove choices matching a regex from the :ref:`glos-daml-test-coverage`
    report. The full name of a local choice takes the format
    ``<module>:<template name>:<choice name>``, preceded by ``<package id>:``
    for nonlocal packages.

Test flags
............

- | ``-h``, ``--help``

  | Prints option information when called on the commandline.

- | ``--package-root ARG``

  | :subsiteref:`DPM-specific<dpm>` option.

  | Path to the root of a :ref:`glos-daml-package` containing ``daml.yaml``.
    Using this option, you set the package to clean. You should prefer the
    ``DAML_PACKAGE`` environment variable over this option. See
    :subsiteref:`dpm-configuration` for more details.

- | ``--project-root ARG``

  | :subsiteref:`DPM-specific<dpm>` option.

  | ``project-root`` is **deprecated**, please use ``--package-root``.

  | Path to the root of a :ref:`glos-daml-package` containing ``daml.yaml``. You
    should prefer the ``DAML_PACKAGE`` environment variable over this option.
    See :subsiteref:`dpm-configuration` for more details.

- | ``--multi-package-path FILE``

  | Path to the :ref:`multi-package.yaml<glos-multi-package>` file.

- | ``--all``

  | Clean all packages in :ref:`multi-package.yaml<glos-multi-package>`.
