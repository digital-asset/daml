.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML assistant (``daml``)
#########################

``daml`` is a command-line tool that does a lot of useful things related to the SDK. Using ``daml``, you can:

- Create new DAML projects: ``daml new <path to create project in>``
- Compile a DAML project: ``daml build``

  This builds the DAML project according to the project config file ``daml.yaml`` (see `Configuration files`_ below).

  In particular, it will download and install the specified version of the SDK (the ``sdk-version`` field in ``daml.yaml``) if missing, and use that SDK version to resolve dependencies and compile the DAML project.

- Launch the tools in the SDK:

  - Launch :doc:`DAML Studio </daml/daml-studio>`: ``daml studio``
  - Launch :doc:`Sandbox </tools/sandbox>` and :doc:`Navigator </tools/navigator/index>` together: ``daml start``
  - Launch Sandbox: ``daml sandbox``
  - Launch Navigator: ``daml navigator``
  - Launch :doc:`Extractor </tools/extractor>`: ``daml extractor``

- Install new SDK versions manually: ``daml install <version>``

   Note that you need to update your `project config file <#configuration-files>` to use the new version.

.. _daml-yaml-configuration:

Configuration files
*******************

The DAML assistant and the DAML SDK are configured using two kinds of config files:

- The global config file, one per installation, which controls some options regarding SDK installation and updates
- The project config file, one per DAML project, which controls how the DAML SDK builds and interacts with the project

Global config file: (``daml-config.yaml``)
==========================================

The global config file ``daml-config.yaml`` is in the ``daml`` home directory, which is ``~/.daml`` on Linux and Mac, and ``C:/Users/<user>/AppData/Roaming/daml`` on Windows. It controls options related to SDK version installation and upgrades.

By default it's blank, and you usually won't need to edit it. It recognizes the following options:

- ``auto-install``: whether ``daml`` automatically installs a missing SDK version when it is required (defaults to ``true``)
- ``update-check``: how often ``daml`` will check for new versions of the SDK, in seconds (default to ``86400``, i.e. once a day)

   This setting is only used to inform you when an update is available.
    
   Set ``update-check: <number>`` to check for new versions every N seconds. Set ``update-check: never`` to never check for new versions.

Here is an example of the global config file ``daml-config.yaml``:

.. code-block:: yaml

   auto-install: true
   update-check: 86400

Project config file: ``daml.yaml``
==================================

The project config file ``daml.yaml`` must be in the root of your DAML project directory. It controls how the DAML project is built and how tools like Sandbox and Navigator interact with it.

The existence of a ``daml.yaml`` file is what tells ``daml`` assistant that this directory contains a DAML project, and lets you use project-aware commands like ``daml build`` and ``daml start``.

``daml new`` creates a skeleton application in a new project folder, which includes a config file. For example, ``daml new my_project`` creates a new folder ``my_project`` with a project config file ``daml.yaml`` like this:

.. code-block:: yaml

    sdk-version: __VERSION__
    name: __PROJECT_NAME__
    source: daml/Main.daml
    scenario: Main:setup
    parties:
      - Alice
      - Bob
    version: 1.0.0
    exposed-modules:
      - Main
    dependencies:
      - daml-prim
      - daml-stdlib

Here is what each field means:

- ``sdk-version``: the SDK version that this project uses.

   The assistant automatically downloads and installs this version if needed (see the ``auto-install`` setting in the global config). We recommend keeping this up to date with the latest stable release of the SDK.

   The assistant will warn you when it is time to update this setting (see the ``update-check`` setting in the global config  to control how often it checks, or to disable this check entirely).
- ``name``: the name of the project. This determines the filename of the ``.dar`` file compiled by ``daml build``.
- ``source``: the location of your main DAML source code file, relative to the project root.
- ``scenario``: the name of the scenario to run when using ``daml start``.
- ``parties``: the parties to display in the Navigator when using ``daml start``.
- ``version``: the project version.
- ``exposed-modules``: the DAML modules that are exposed by this project, which can be imported in other projects.
- ``dependencies``: the dependencies of this project.

..  TODO (@robin-da) document the dependency syntax

Full help for commands
**********************

Use ``--help`` with any command.

Comparing to the old SDK assistant
**********************************

.. list-table:: 
   :header-rows: 1

   * - Old ``da`` command
     - Purpose
     - New ``daml`` equivalent
   * - ``da status``
     - Show a list of running services
     - No longer needed, as components no longer run in the background
   * - ``da docs``
     - Display the documentation
     - No longer needed. You can access the docs at docs.daml.com, which includes a PDF download for offline use.
   * - ``da new``
     - Create a new project from template
     - ``daml new  <path to create project in> [<name of template>]``
   * - ``da project``
     - Manage SDK projects
     - No longer needed
   * - ``da template``
     - Manage SDK templates
     - No longer needed
   * - ``da upgrade``
     - Upgrade SDK version
     - ``daml install latest --activate``
   * - ``da list``
     - List installed SDK versions
     - ``daml version`` prints SDK version information.
   * - ``da use``
     - Set the default SDK version
     - No direct equivalent; you now set the new SDK version (``sdk-version: X.Y.Z``) in your project config file (``daml.yaml``) manually.
   * - ``da uninstall``
     - Uninstall the SDK
     - No direct equivalent
   * - ``da start``
     - Start Navigator and Sandbox
     - ``daml build`` then ``daml start``. Now stops by ctrl+c, rather than a ``stop`` command.
   * - ``da restart``
     - Shut down and restart Navigator and Sandbox.
     - ``ctrl+c`` and ``daml start``
   * - ``da stop``
     - Stop running Navigator and Sandbo
     - ``ctrl+c``
   * - ``da feedback``
     - Send us feedback
     - No longer needed. See :doc:`/support/support` for how to give feedback.
   * - ``da studio``
     - Launch DAML Studio
     - ``daml studio``
   * - ``da navigator``
     - Launch Navigator
     - No direct equivalent; ``daml navigator`` is equivalent to ``da run navigator``.
   * - ``da sandbox``
     - Launch Sandbox
     - No direct equivalent; ``daml sandbox`` is equivalent to ``da run sandbox``.
   * - ``da compile``
     - Compile a DAML project into a .dar file
     - ``daml build``
   * - ``da path <component>``
     - Show the path to an SDK component
     - No equivalent
   * - ``da run``
     - Run an SDK component
     - ``daml sandbox``, ``daml navigator``, ``daml damlc``, etc
   * - ``da setup``
     - Initialize the SDK
     - No longer needed: this is handled by the installer
   * - ``da subscribe``
     - Subscribe to a template namespace
     - No longer needed
   * - ``da unsubscribe``
     - Unsubscribe from a template namespace
     - No longer needed
   * - ``da config-help``
     - Show help about config files
     - No longer needed: config files are documented on this page
   * - ``da config``
     - Query and manage config
     - No equivalent: view and edit your config files directly
   * - ``da changelog``
     - Show release notes
     - No longer needed: see the :doc:`/support/release-notes`.
   * - ``da update-info``
     - Show assistant update channel information
     - No longer needed

.. _assistant-manual-building-dars:

Building DAML projects
**********************

To compile your DAML source code into a DAML archive (a ``.dar`` file), run::

  daml build

You can control the build by changing your project's ``daml.yaml``:

``sdk-version``
  The SDK version to use for building the project.

``name``
  The name of the project.

``source``
  The path to the source code.

The generated ``.dar`` file is created in ``dist/${name}.dar`` by default. To override the default location, pass the ``-o`` argument to ``daml build``::

  daml build -o path/to/darfile.dar

.. _assistant-manual-managing-releases:

Managing SDK releases
*********************

In general the ``daml`` assistant will install versions and guide you when you need to update SDK versions or project settings. If you disable ``auto-install`` and ``update-check`` in the global config file, you will have to manage SDK releases manually.

.. To find out what version: TODO (@associahedron) Add output of revamped version command here.

To download and install the latest stable SDK release and update the assistant, run::

  daml install latest --activate

Remove the ``--activate`` flag if you only want to install the latest release without updating the ``daml`` assistant in the process. If it is already installed, you can force reinstallation by passing the ``--force`` flag. See ``daml install --help`` for a full list of options.

To install the SDK release specified in the project config, run::

  daml install project

To install a specific SDK version, for example version ``0.13.0``, run::

  daml install 0.13.0

To install an SDK release from a downloaded SDK release tarball, run::

  daml install path-to-tarball.tar.gz

but beware, this is an advanced feature and you should only ever perform this on an SDK release tarball that is released through the official ``digital-asset/daml`` github repository. Otherwise your ``daml`` installation may become inconsistent with everyone elses.

.. TODO (@associahedron) Add ``daml uninstall`` and ``daml version --list`` commands.

