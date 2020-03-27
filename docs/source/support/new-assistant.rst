.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Moving to the new DAML assistant
################################

We've released a new command-line tool for working with the DAML SDK: DAML Assistant, or ``daml``. Many of its commands are similar to the old SDK Assistant (``da``), but there are some changes:

- Simplified installation process: ``curl -sSL https://get.daml.com/ | sh`` for Linux and Mac
- Overhaul and simplification of templates:

  - ``daml new`` takes arguments in a consistent order:

    - ``daml new proj`` creates a new project named ``proj`` with a skeleton template
    - ``daml new proj quickstart-java`` creates a new project with the quickstart-java template

  - ``daml new`` templates are built-in to the SDK
  - Mix-in template mechanism is gone (``da add``)
  - No more publishing or subscribing of templates on Bintray: use Github and ``git clone`` to distribute templates outside of the SDK

- Use ``daml build`` to compile your project into a DAR
- ``daml start`` components don't run in the background, and you stop them with ``ctrl+c``

  As a result, there are no equivalents to ``da stop`` and ``da restart``

- No ``da run`` equivalent, but:

  - ``daml sandbox`` is the same as ``da run sandbox --``
  - ``daml navigator`` is the same as ``da run navigator --``
  - ``daml damlc`` is the same as ``da run damlc --``
- ``daml.yaml`` configuration file replaces ``da.yaml`` - read more about this in the next section

Migrating a ``da`` project to ``daml``
======================================

Migrating with daml init
************************

You can migrate an existing project using the ``daml init`` command. To use it, go to the project root on the command line and run ``daml init``. This will create a ``daml.yaml`` file based on ``da.yaml``.

Some things to keep in mind when using ``daml init`` to migrate projects:

- If your project uses an SDK version prior to 0.12.15, the generated ``daml.yaml`` will use SDK version 0.12.15 instead. Support for previous SDK versions in the new assistant is limited.

- ``daml.yaml`` adds ``exposed-modules`` and ``dependencies`` fields, which are needed for ``daml build``. Depending on your DAML project, may have to adjust these fields in the generated ``daml.yaml``.

Migrating manually
******************

To migrate the project manually:

1. Upgrade your project to SDK version 0.12.15 or later.
2. Convert your project's ``da.yaml`` file into a ``daml.yaml`` file.

   The two files are very similar: ``daml.yaml`` is the ``project`` section of ``da.yaml``, plus some additional packaging information. Here is an example of a ``daml.yaml`` file, from the quickstart-java template:

   .. code-block:: yaml

      sdk-version: 0.12.14
      name: my_project
      source: daml
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

   Here is the corresponding ``da.yaml`` file:

   .. code-block:: yaml

      project:
        sdk-version: 0.12.12
        scenario: Main:setup
        name: foobar
      source: daml
        parties:
        - Alice
        - Bob
        - USD_Bank
        - EUR_Bank
      version: 2

The extra fields in ``daml.yaml`` are related to the new packaging functionality in ``damlc``. When you build a DAML project with ``daml build`` (or ``daml start``) it creates a ``.dar`` package from your project inside the ``dist/`` folder. (You can supply a different target location by passing the ``-o`` option.) To create the package properly, the new config file ``daml.yaml`` needs the following additional fields that were not present in ``da.yaml``:

- ``version``: The version number for the DAML project, which becomes the version number for the compiled package.
- ``exposed-modules``: When the ``.dar`` file is built, this determines what modules are exposed for users of the package.
- ``dependencies``: The DAML packages that this project depends on. ``daml-prim`` and ``daml-stdlib`` together give access to the basic definitions of DAML - **you should add them both as dependencies**. Additional dependencies can only be added by giving the path to the ``.dar`` file of the other package.

You can now use ``daml`` commands with your project.

Switching from old commands to new ones
=======================================

This section goes through the ``da`` commands, and gives the ``daml`` equivalent where there is one.

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
     - ``daml version``
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
     - No direct equivalent; ``daml navigator`` is equivalent to ``da run navigator``
   * - ``da sandbox``
     - Launch Sandbox
     - No direct equivalent; ``daml sandbox`` is equivalent to ``da run sandbox``
   * - ``da compile``
     - Compile a DAML project into a .dar file
     - ``daml build``
   * - ``da run <component>``
     - Run an SDK component
     - ``daml navigator``, ``daml sandbox``, etc as above
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
     - ``daml new <target path> <name of template>``
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
     - No longer needed: you can access the docs at `docs.daml.com <https://docs.daml.com/>`__, which includes a PDF download for offline use
   * - ``da feedback``
     - Send us feedback
     - No longer needed: see :doc:`/support/support` for how to give feedback.
   * - ``da config-help``
     - Show help about config files
     - No longer needed: config files are documented on this page
   * - ``da changelog``
     - Show release notes
     - No longer needed: see the :doc:`/support/release-notes`
