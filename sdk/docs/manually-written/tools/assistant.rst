.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml Assistant
##############

``daml`` is a command-line tool that does a lot of useful things related to the SDK. Using ``daml``, you can:

- Create new Daml projects: ``daml new <path to create project in>``
- Create a new project based on the ``create-daml-app`` template: ``daml new --template=create-daml-app <path to create project in>``
- Initialize a Daml project: ``daml init``
- Compile a Daml project: ``daml build``

  This builds the Daml project according to the project config file ``daml.yaml`` (see `Configuration files`_ below).

  In particular, it will download and install the specified version of the Daml SDK (the ``sdk-version`` field in ``daml.yaml``) if missing, and use that SDK version to resolve dependencies and compile the Daml project.

- Launch the tools in the SDK:

  - Launch :doc:`Daml Studio </daml/daml-studio>`: ``daml studio``
  - Launch :doc:`Sandbox </tools/sandbox>`, :doc:`Navigator </tools/navigator/index>` and the :doc:`/json-api/index`: ``daml start``
    You can disable the HTTP JSON API by passing ``--json-api-port none`` to ``daml start``.
    To specify additional options for sandbox/navigator/the HTTP JSON API you can use
    ``--sandbox-option=opt``, ``--navigator-option=opt`` and ``--json-api-option=opt``.
  - Launch Sandbox: ``daml sandbox``
  - Launch Navigator (Deprecated): ``daml navigator``
  - Launch the :doc:`/json-api/index`: ``daml json-api``
  - Run :doc:`Daml codegen </tools/codegen>`: ``daml codegen``

- Install new SDK versions manually: ``daml install <version>``

   Note that you need to update your `project config file <#configuration-files>` to use the new version.

Command Help
**********************

To see information about any command, run it with ``--help``.

.. _daml-yaml-configuration:

Configuration Files
*******************

The Daml assistant and the SDK are configured using two files:

- The global config file, one per installation, which controls some options regarding SDK installation and updates
- The project config file, one per Daml project, which controls how the SDK builds and interacts with the project

.. _global_daml_config:

Global Config File (``daml-config.yaml``)
=========================================

The global config file ``daml-config.yaml`` is in the ``daml`` home directory (``~/.daml`` on Linux and Mac, ``C:/Users/<user>/AppData/Roaming/daml`` on Windows). It controls options related to SDK version installation and upgrades.

By default it's blank, and you usually won't need to edit it. It recognizes the following options:

- ``auto-install``: whether ``daml`` automatically installs a missing SDK version when it is required (defaults to ``true``)
- ``update-check``: how often ``daml`` will check for new versions of the SDK, in seconds (default to ``86400``, i.e. once a day)

   This setting is only used to inform you when an update is available.

   Set ``update-check: <number>`` to check for new versions every N seconds. Set ``update-check: never`` to never check for new versions.
- ``artifactory-api-key``: If you have a license for Daml EE,
  you can use this to specify the Artifactory API key displayed in
  your user profile. The assistant will use this to download the EE
  edition.

Here is an example ``daml-config.yaml``:

.. code-block:: yaml

   auto-install: true
   update-check: 86400

Project Config File (``daml.yaml``)
===================================

The project config file ``daml.yaml`` must be in the root of your Daml project directory. It controls how the Daml project is built and how tools like Sandbox and Navigator interact with it.

The existence of a ``daml.yaml`` file is what tells ``daml`` that this directory contains a Daml project, and lets you use project-aware commands like ``daml build`` and ``daml start``.

``daml init`` creates a ``daml.yaml`` in an existing folder, so ``daml`` knows it's a project folder.

``daml new`` creates a skeleton application in a new project folder, which includes a config file. For example, ``daml new my_project`` creates a new folder ``my_project`` with a project config file ``daml.yaml`` like this:

.. code-block:: yaml

    sdk-version: __VERSION__
    name: __PROJECT_NAME__
    source: daml
    init-script: Main:setup
    parties:
      - Alice
      - Bob
    version: 1.0.0
    exposed-modules:
      - Main
    dependencies:
      - daml-prim
      - daml-stdlib
    script-service:
      grpc-max-message-size: 134217728
      grpc-timeout: 60
      jvm-options: []
    build-options: ["--ghc-option", "-Werror",
                    "--ghc-option", "-v"]


Here is what each field means:

- ``sdk-version``: the SDK version that this project uses.

   The assistant automatically downloads and installs this version if needed (see the ``auto-install`` setting in the global config). We recommend keeping this up to date with the latest stable release of the SDK.
   It is possible to override the version without modifying the ``daml.yaml`` file by setting the ``DAML_SDK_VERSION`` environment variable. This is mainly useful when you are working with an
   external project that you want to build with a specific version.

   The assistant will warn you when it is time to update this setting (see the ``update-check`` setting in the global config  to control how often it checks, or to disable this check entirely).
- ``name``: the name of the project. This determines the filename of the ``.dar`` file compiled by ``daml build``.
- ``source``: the root folder of your Daml source code files relative to the project root.
- ``init-script``: the name of the Daml script to run when using ``daml start``.
- ``parties``: the parties to display in the Navigator when using ``daml start``.
- ``version``: the project version.
- ``exposed-modules``: the Daml modules that are exposed by this project, which can be imported in other projects.
  If this field is not specified all modules in the project are exposed.
- ``dependencies``: library-dependencies of this project. See :doc:`/daml/reference/packages`.
- ``data-dependencies``: Cross-SDK dependencies of this project See :doc:`/daml/reference/packages`.
- ``module-prefixes``: Prefixes for all modules in package See :doc:`/daml/reference/packages`.
- ``script-service``: settings for the script service

  - ``grpc-max-message-size``: This option controls the maximum size of gRPC messages.
    If unspecified this defaults to 128MB (134217728 bytes). Unless you get
    errors, there should be no reason to modify this.
  - ``grpc-timeout``: This option controls the timeout used for communicating
    with the script service. If unspecified this defaults to 60s. Unless you get
    errors, there should be no reason to modify this.
  - ``jvm-options``: A list of options passed to the JVM when starting the script
    service. This can be used to limit maximum heap size via the ``-Xmx`` flag.

- ``build-options``: a list of tokens that will be appended to some invocations of ``damlc`` (currently `build` and `ide`). Note that there is no further shell parsing applied.
- ``sandbox-options``: a list of options that will be passed to Sandbox in ``daml start``.
- ``navigator-options``: a list of options that will be passed to Navigator in ``daml start``.
- ``json-api-options``: a list of options that will be passed to the HTTP JSON API in ``daml start``.
- ``script-options``: a list of options that will be passed to the Daml script
  runner when running the ``init-script`` as part of ``daml start``.
- ``start-navigator``: Controls whether navigator is started as part
  of ``daml start``. Defaults to ``true``. If this is specified as a CLI argument,
  say ``daml start --start-navigator=true``, the CLI argument takes precedence over
  the value in ``daml.yaml``.

The ``daml.yaml`` file supports `Environment Variable Interpolation <#environment-variable-interpolation>`__.

Recommended ``build-options``
=============================

The default set of warnings enabled by the Daml compiler is fairly conservative.
When you are just starting out, seeing a huge set of warnings can easily be
overwhelming and distract from what you are actually working on.  However, as
you get more experienced and more people work on a Daml project, enabling
additional warnings (and enforcing their absence in CI) can be useful.

Here are ``build-options`` you might declare in a project's ``daml.yaml`` for a
stricter set of warnings.

.. code-block:: yaml

    build-options:
      - --ghc-option=-Wunused-top-binds
      - --ghc-option=-Wunused-matches
      - --ghc-option=-Wunused-do-bind
      - --ghc-option=-Wincomplete-uni-patterns
      - --ghc-option=-Wredundant-constraints
      - --ghc-option=-Wmissing-signatures
      - --ghc-option=-Werror

Each option enables a particular warning, except for the last one, ``-Werror``,
which turns every warning into an error; this is especially useful for CI build
arrangements.  Simply remove or comment out any line to disable that category of
warning.  See
`the Daml forum <https://discuss.daml.com/t/making-the-most-out-of-daml-compiler-warnings/739>`__
for a discussion of the meaning of these warnings and pointers to other
available warnings.

Multi-Package Config File (``multi-package.yaml``)
==================================================
See :ref:`Multi-Package Build<multi-package-build>` for more information on this file.
The ``multi-package.yaml`` file is used to inform Daml Build and the IDE of projects containing multiple
connected Daml packages.

An example is given below:

.. code-block:: yaml

  packages:
    - ./path/to/package/a
    - ./path/to/package/b
  projects:
    - ./path/to/project/a
    - ./path/to/project/b

Here is what each field means:

- ``packages``: an optional list of directories containing Daml packages, and by extension, ``daml.yaml`` config files. These allow Daml Multi-Build to
  find the source code for dependency DARs and build them in topological order.
- ``projects``: an optional list of directories containing ``multi-package.yaml`` config files, which will extend the above package set for resolving
  the build order. These exist to allow separation of your project into sub-projects which can still be built when removed from the surrounding environment.

The ``multi-package.yaml`` file supports `Environment Variable Interpolation <#environment-variable-interpolation>`__.

Environment Variable Interpolation
==================================
Both the ``daml.yaml`` and ``multi-package.yaml`` config files support environment variable interpolation on all string fields.
Interpolation takes the form of ``${MY_ENVIRONMENT_VARIABLE}``, which is replaced with the content of ``MY_ENVIRONMENT_VARIABLE`` from the
calling shell. These can be escaped and placed within strings according to the environment variable interpolation semantics.

This allows you to extract common data, such as the sdk-version, package-name, or package-version outside of a package's ``daml.yaml``. For example,
you can use an ``.envrc`` file or have these values provided by a build system. This feature can also be used for specifying dependency DARs, enabling you to either store
your DARs in a common folder and pass its directory as a variable, shortening the paths in your ``daml.yaml``, or pass each dependency as a
separate variable through an external build system, which may store them in a temporary cache.

The following example showcases this:

.. code-block:: yaml

  sdk-version: ${SDK_VERSION}
  name: ${PROJECT_NAME}_test
  source: daml
  version: ${PROJECT_VERSION}
  dependencies:
    // Using a common directory
    ${DEPENDENCY_DIRECTORY}/my-dependency-1.0.0.dar
    ${DEPENDENCY_DIRECTORY}/my-other-dependency-1.0.0.dar
    // Passed directly by a build system
    ${DAML_FINANCE_DAR}
    ${MY_DEPENDENCY_DAR}

Escape syntax uses the ``\`` prefix: ``\${NOT_INTERPOLATED}``, and interpolation can be disallowed for a config file
by setting the ``environment-variable-interpolation`` field to ``false``.

.. code-block:: yaml

  name: ${NOT_INTERPOLATED}
  environment-variable-interpolation: false

Note that environment variables are case sensitive, meaning ``${MY_VAR}`` and ``${My_Var}`` do not reference the same variable.
