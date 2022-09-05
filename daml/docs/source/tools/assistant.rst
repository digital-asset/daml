.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml Assistant (``daml``)
#########################

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
  - Launch Navigator: ``daml navigator``
  - Launch the :doc:`/json-api/index`: ``daml json-api``
  - Run :doc:`Daml codegen </tools/codegen>`: ``daml codegen``

- Install new SDK versions manually: ``daml install <version>``

   Note that you need to update your `project config file <#configuration-files>` to use the new version.

Full Help for Commands
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

.. _assistant-manual-building-dars:

Build Daml Projects
*******************

To compile your Daml source code into a Daml archive (a ``.dar`` file), run::

  daml build

You can control the build by changing your project's ``daml.yaml``:

``sdk-version``
  The SDK version to use for building the project.

``name``
  The name of the project.

``source``
  The path to the source code.

The generated ``.dar`` file is created in ``.daml/dist/${name}.dar`` by default. To override the default location, pass the ``-o`` argument to ``daml build``::

  daml build -o path/to/darfile.dar

.. _assistant-manual-managing-releases:

Manage Releases
***************

You can manage SDK versions manually by using ``daml install``.

To download and install SDK of the latest stable Daml version::

  daml install latest

To download and install the latest snapshot release::

  daml install latest --snapshots=yes

Please note that snapshot releases are not intended for production usage.

To install the SDK version specified in the project config, run::

  daml install project

To install a specific SDK version, for example version ``2.0.0``, run::

  daml install 2.0.0

Rarely, you might need to install an SDK release from a downloaded SDK release tarball. **This is an advanced feature**: you should only ever perform this on an SDK release tarball that is released through the official ``digital-asset/daml`` github repository. Otherwise your ``daml`` installation may become inconsistent with everyone else's. To do this, run::

  daml install path-to-tarball.tar.gz

By default, ``daml install`` will update the assistant if the version being installed is newer. You can force the assistant to be updated with ``--install-assistant=yes`` and prevent the assistant from being updated with ``--install-assistant=no``.

See ``daml install --help`` for a full list of options.

Terminal Command Completion
***************************

The ``daml`` assistant comes with support for ``bash`` and ``zsh`` completions. These will be installed automatically on Linux and Mac when you install or upgrade the Daml assistant.

If you use the ``bash`` shell, and your ``bash`` supports completions, you can use the TAB key to complete many ``daml`` commands, such as ``daml install`` and ``daml version``.

For ``Zsh`` you first need to add ``~/.daml/zsh`` to your ``$fpath``,
e.g., by adding the following to the beginning of your ``~/.zshrc``
before you call ``compinit``: ``fpath=(~/.daml/zsh $fpath)``

You can override whether bash completions are installed for ``daml`` by
passing ``--bash-completions=yes`` or ``--bash-completions=no`` to ``daml install``.

.. _daml_project_dir:

Run Commands Outside of the Project Directory
*********************************************

In some cases, it can be convenient to run a command in a project
without having to change directories. For that usecase, you can set
the ``DAML_PROJECT`` environment variable to the path to the project:

.. code-block:: sh

    DAML_PROJECT=/path/to/my/project daml build

Note that while some commands, most notably, ``daml build``, accept a
``--project-root`` option, it can end up choosing the wrong SDK
version so you should prefer the environment variable instead.
