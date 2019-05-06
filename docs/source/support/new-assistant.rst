.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Moving to the new DAML assistant
################################

New command-line tool for working with the DAML SDK: DAML Assistant, or ``daml``. Many commands are similar to using the old SDK Assistant (``da``), but with some changes:

- Simplified installation process: ``curl -sSL https://get.daml.com/ | sh`` for Linux and Mac.
-

- Overhaul and simplification of templates:
  - ``daml new`` takes arguments in a consistent order (i.e. project folder first, template name last).
  - Mix-in template mechanism is gone (``da add``)
  - No publishing or subscribing of templates, they are distributed with the SDK.
- Use ``daml build`` to compile your project into a DAR.
- ``daml start`` components don't run in the background, you stop them with ``ctrl+c``
  - There is no ``da stop`` and ``da restart`` command.
- No ``da run`` equivalent, but:
  - ``daml sandbox`` is the same as ``da run sandbox``
  - ``daml navigator`` is the same as ``da run navigator``
  - ``daml damlc`` is the same as ``da run damlc``


Migrating a da project to daml
==============================

Before you begin migrating an existing project to ``daml``, make sure you upgrade your project to SDK version 0.12.15 or later.

The biggest difference between a ``da`` project and a ``daml`` project is that the latter requires a ``daml.yaml`` file instead of a ``da.yaml`` file.

.. The ``da migrate`` command, from the old assistant, will create a ``daml.yaml`` file based on the existing ``da.yaml``. This command is not complete at this time.

So to migrate an existing project, you will need to create a ``daml.yaml`` file. The two files are very similar, with ``daml.yaml`` being the ``project`` section of ``da.yaml``, with some additional packaging information. Here is an example of a ``daml.yaml`` file, from the quickstart-java template:

.. code-block:: yaml

   sdk-version: 0.12.14
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

Here is the corresponding ``da.yaml`` file for comparison:

.. code-block:: yaml

   project:
     sdk-version: 0.12.12
     scenario: Main:setup
     name: foobar
     source: daml/Main.daml
     parties:
     - Alice
     - Bob
     - USD_Bank
     - EUR_Bank
   version: 2

The extra fields in ``daml.yaml`` are related to the new packaging functionality in ``damlc``. When you build a DAML project with ``daml build`` (or ``daml start``) it creates a ``.dar`` package from your project inside the ``dist/`` folder. (You can supply a different target location by passing the ``-o`` option.) To create the package properly, the new config file ``daml.yaml`` needs the following additional fields that were not present in ``da.yaml``:

- ``version``: This is the version number for the DAML project, which becomes the version number for the compiled package.
- ``exposed-modules``: This is used when building the ``.dar`` file for the project. It determines what modules are exposed for users of the package.
- ``dependencies``: These are the DAML packages that this project depends on. The two packages ``daml-prim`` and ``daml-stdlib`` give access to the basic definitions of DAML, and you should add them both as dependencies. At this time, additional dependencies can only be added by giving the path to the ``.dar`` file of the other package.


Switching from old commands to new ones
=======================================

Managing versions and config
****************************

.. list-table::
   :header-rows: 1

   * - Old command
     - Purpose
     - New equivalent
   * - ``da setup``
     - Initialize the SDK
     - No longer needed: this is handled by the installer
   * - ``da upgrade``
     - Upgrade SDK version
     - ``daml install <version>``
   * - ``da list``
     - List installed SDK versions
     - ``daml version`` prints the current SDK version in use
   * - ``da use``
     - Set the default SDK version
     - No direct equivalent; you now set the new SDK version (``sdk-version: X.Y.Z``) in your project config file (``daml.yaml``) manually
   * - ``da config``
     - Query and manage config
     - No equivalent: view and edit your config files directly
   * - ``da uninstall``
     - Uninstall the SDK
     - Currently no equivalent for this
   * - ``da update-info``
     - Show assistant update channel information
     - No longer needed

Running components
******************

.. list-table::
   :header-rows: 1

   * - Old command
     - Purpose
     - New equivalent
   * - ``da start``
     - Start Navigator and Sandbox
     - ``daml start``
   * - ``da stop``
     - Stop running Navigator and Sandbox
     - ``ctrl+c``
   * - ``da restart``
     - Shut down and restart Navigator and Sandbox
     - ``ctrl+c`` and ``daml start``
   * - ``da studio``
     - Launch DAML Studio
     - ``daml studio``
   * - ``da navigator``
     - Launch Navigator
     - ``daml navigator``
   * - ``da sandbox``
     - Launch Sandbox
     - ``daml sandbox``
   * - ``da compile``
     - Compile a DAML project into a .dar file
     - ``daml build``
   * - ``da run``
     - Run an SDK component
     - ``daml studio``, ``daml navigator``, etc as above
   * - ``da path <component>``
     - Show the path to an SDK component
     - No equivalent
   * - ``da status``
     - Show a list of running services
     - No longer needed: components no longer run in the background

Managing templates and projects
*******************************

.. list-table::
   :header-rows: 1

   * - Old command
     - Purpose
     - New equivalent
   * - ``da template``
     - Manage SDK templates
     - No longer needed: use ``git clone`` for templates instead
   * - ``da project new``
     - Create an SDK project
     - ``daml new``, or use ``git clone``
   * - ``da project add``
     - Add a template to the current project
     - No longer needed: use ``git clone`` instead
   * - ``da new``
     - Create a new project from template
     - ``daml new  <path to create project in> <name of template>``
   * - ``da subscribe``
     - Subscribe to a template namespace
     - No longer needed: use ``git clone`` instead
   * - ``da unsubscribe``
     - Unsubscribe from a template namespace
     - No longer needed: use ``git clone`` instead

Docs and feedback
*****************

.. list-table::
   :header-rows: 1

   * - Old command
     - Purpose
     - New equivalent
   * - ``da docs``
     - Display the documentation
     - No longer needed: you can access the docs at docs.daml.com, which includes a PDF download for offline use
   * - ``da feedback``
     - Send us feedback
     - No longer needed: see :doc:`/support/support` for how to give feedback.
   * - ``da config-help``
     - Show help about config files
     - No longer needed: config files are documented on this page
   * - ``da changelog``
     - Show release notes
     - No longer needed: see the :doc:`/support/release-notes`
