.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


.. _howto-daml-compiler:

The Daml Compiler
#################

At the core of the Daml toolchain lies the Daml Compiler, also known as `damlc`.
Its primary usage is to compile Daml source code (with ``.daml`` extension) into
a lower-level language called Daml-LF. :ref:`Daml-Lf<daml-lf>` is the language
that canton participants can evaluate.

When building, recommended via :subsiteref:`DPM<dpm>` (i.e. trough ``dpm
build``), or alternatively, by calling ``damlc`` directly, the Daml Compiler
goes through the following stages:


#. The input ``.daml`` :ref:`files<glos-daml-source-files>` are type-checked (i.e. it is verified if they are
   well-formed or not)
#. The type-checked ```.daml`` code is transformed to
   :ref:`Daml-LF<daml-lf-intro>`.
#. The Daml-LF is packaged together with its dependency in a ``.dar``
   :ref:`file<dar-file-dalf-file>`, which essentially is an archive of Daml-LF
   files with metadata.

Build files can be cleaned using ``dpm clean``.

On top of that, the Daml Compiler offers testing functionality, either by
calling ``dpm build`` to obtain a ``.dar`` to be used in combination with
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

- | ``--package-check``

  | Check if running in Daml :ref:`glos-daml-package`.

  | ?????

- | ``--project-check``

  | ``project-check`` is **deprecated**, please use ``--package-check``.

  | Check if running in Daml :ref:`glos-daml-package`. (project-check is
    deprecated, please use `--package-check`).

- | ``--include INCLUDE-PATH``

  | Path to an additional source directory to be included.

- | ``--package-db LOC-OF-PACKAGE-DB``

  | Use :ref:`glos-package-database` in the given location.

- | ``--access-token-file PATH``

  | Path to the token-file for ledger authorization.

  | ?????

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

  | Detail level of the pretty printed output (default: 0)>

  | ?????

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
    - ``template-has-new-interface-instanc``

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

  | Filename of JUnit output file.

  | ?????

- | ``--package-name PACKAGE-NAME``

  | Create package artifacts for the given package name.

- | ``--table-output ARG``

  | Filename to which table should be output.

  | ?????

- | ``--transactions-output ARG``

  | Filename to which the transaction list should be output

  | ?????

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
