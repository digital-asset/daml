.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML assistant (``daml``)
#########################

``daml`` is a command-line tool that does a bunch of useful stuff with the SDK. Using ``daml``, you can:

- Create new DAML projects: ``daml new <path to create project in>``
- Compile a DAML project: ``daml build``

  - This will build the DAML project according to the project config file ``daml.yaml`` (see below).
  - In particular, it will download and install the specified version of the SDK (the ``sdk-version`` 
    field in ``daml.yaml``) if missing, and use that SDK version to resolve dependencies and compile
    the DAML project.

- Launch the tools in the SDK:

  - Launch :doc:`DAML Studio </daml/daml-studio>`: ``daml studio``
  - Launch :doc:`Sandbox </tools/sandbox>` and :doc:`Navigator </tools/navigator/index>` together: ``daml start``
  - Launch Sandbox: ``daml sandbox``
  - Launch Navigator: ``daml navigator``
  - Launch :doc:`Extractor </tools/extractor>`: ``daml extractor``

- Install new SDK versions manually: ``daml install <version>``

.. _daml-yaml-configuration:

Configuration files
*******************

The DAML assistant and the DAML SDK are configured using two kinds of config files:

- the global config file, one per installation, controls some options regarding SDK installation and updates
- the project config file, one per DAML project, controls how the DAML SDK builds and interacts with the project

Global config file
==================

The global config file ``daml-config.yaml`` is in the daml home directory, which is ``~/.daml`` on Linux and Mac, and ``C:/Users/<user>/AppData/Roaming/daml`` on Windows. It controls certain options related to SDK version installation and upgrades. By default it is blank, and you will probably not need to change it, but you can edit it to suit your needs. The assistant recognizes the following options:

- ``auto-install`` controls whether ``daml`` will automatically install a missing SDK version when it is required (defaults to ``true``)
- ``update-check`` controls how often ``daml`` will check for new versions of the SDK (default to ``86400``, i.e. once a day)

  - This setting is only to inform the user when an update is available.
  - ``update-check: <number>`` will check for new versions every N seconds.
  - ``update-check: never`` will never check for new versions.

Here is an example of the global config file ``daml-config.yaml``:

.. code-block:: yaml

   auto-install: true
   update-check: 86400

Project config file
===================

The project config file ``daml.yaml``, at the root of your DAML project directory, controls how the DAML project is built and how tools like Sandbox and Navigator interact with it. The ``daml.yaml`` file is necessary for ``daml`` assistant to recognize a DAML project, and to use project-aware commands like ``daml build`` and ``daml start``.

The ``daml new`` command will create a config file for you, along with an example app in a new project folder. For example, ``daml new my_project`` creates a new folder ``my_project`` with a project config file ``daml.yaml`` like this:

.. code-block:: yaml

   sdk-version: 0.13.0
   name: my_project
   source: daml/Main.daml
   scenario: Main:setup
   parties:
     - Alice
     - Bob
     - USD_Bank
     - EUR_Bank
   version: 1.0.0
   exposed-modules:
     - Main
   dependencies:
     - daml-prim
     - daml-stdlib

Here is what each field means:

- ``sdk-version`` specifies the SDK version to be used to build this project. The assistant will automatically download and install
  this version if needed (see the ``auto-install`` setting in the global config). We recommend keeping this up to date with the 
  latest stable release of the SDK. The assistant will warn you when it is time to update this setting (see the ``update-check`` setting
  in the global config  to control how often it checks, or to disable this check entirely).
- ``name`` is the name of the project.
- ``source`` is the location of your main DAML source code file, relative to the project root
- ``scenario`` is the name of the main scenario to run with ``daml start``
- ``parties`` is the list of parties to display in the Navigator, when running with ``daml start``
- ``version`` is the project version.
- ``exposed-modules`` is the list of DAML modules that are exposed by this project, which can be imported in other projects
- ``dependencies`` is the list of dependencies this module has

..  TODO (@robin-da) document the dependency syntax


Full help for commands
*******************************

Use ``--help`` with any command.

Comparing to the old SDK assistant
**********************************

Biggest change is that templates are gone. Moving to more standard mechanisms like git clone.

Plus components don't run in the background, so stop with ctrl+c.

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
     - ``daml start``. Now stops by ctrl+c, rather than a ``stop`` command.
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

Building DAML Project
*********************

Compiling your DAML source code into a DAML archive (a ``.dar`` file)::

  daml build

You can control the build by changing your project's ``daml.yaml``:

``sdk-version``
  The SDK version to use for building the project.

``name``
  The name of the project.

``source``
  The path to the source code.

The generated ``.dar`` file will be stored in ``dist/${name}.dar`` by default. You can override the default location by
passing the ``-o`` argument to ``daml build``::

  daml build -o path/to/darfile.dar

.. _assistant-manual-managing-releases:

Managing SDK releases
*********************

In general the ``daml`` assistant will install versions and guide you when you need to update SDK versions or project settings. If you disable ``auto-install`` and ``update-check`` in the global config file you will have to manage SDK releases manually.

To find out what version

.. TODO (@associahedron) Add output of revamped version command here.

To download and install the latest stable SDK release and update your ``daml`` assistant::

  daml install latest --activate

Remove the ``--activate`` flag if you only want to install the latest release without updating the ``daml`` assistant in the process.
If it is already installed, you can force reinstallation by passing the ``--force`` flag. See ``daml install --help`` for a full list of options.

To install the SDK release specified in the project config::

  daml install project

To install a specific SDK version, for example version ``0.13.0``::

  daml install 0.13.0

To install an SDK release from a downloaded SDK release tarball, run::

  daml install path-to-tarball.tar.gz

but beware, this is an advanced feature and you should only ever perform this on an SDK release tarball that is released through the official ``digital-asset/daml`` github repository, otherwise your ``daml`` installation may become inconsistent with everyone elses.

.. TODO (@associahedron) Add ``daml uninstall`` and ``daml version --list`` commands.

