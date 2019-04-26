.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML assistant (``daml``)
#########################

``daml`` is a command-line tool that does a bunch of useful stuff with the SDK. Using ``daml``, you can:

- Install new SDK versions (but you need to update your config file yourself)

  ``daml install <version>``
- Launch the tools in the SDK:

  - Launch :doc:`DAML Studio </daml/daml-studio>`: ``da studio``
  - Launch :doc:`Sandbox </tools/sandbox>` and :doc:`Navigator <tools/navigator/index>` together: ``daml start``
  - Launch Sandbox: ``daml sandbox``
  - Launch Navigator: ``daml navigator``
  - Launch :doc:`Extractor </tools/extractor>`: ``daml extractor``
- Compile a DAML project: ``daml build``
- Create new project based off built-in template: ``daml new <path to create project in> <name of template>``

Configuration files
*******************

Global config file
==================

The ``daml`` configuration file, ``daml.yaml`` is in TODO directory.

Specifies global configuration options. TODO list.

TODO example file.

Project config file
===================

The project configuration file must be in the root of your project. Specifies project configuration and can override global configuration properties.

.. Make sure to include this from old docs: Some tools, like the Navigator, require parties to be configured before they are started. Do this in the ``project.parties`` property of the ``daml.yaml`` file for the project.

TODO example files.

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
     - ``daml new  <path to create project in> <name of template>``
   * - ``da project``
     - Manage SDK projects
     - No longer needed
   * - ``da template``
     - Manage SDK templates
     - No longer needed
   * - ``da upgrade``
     - Upgrade SDK version
     - ``daml install <version>``
   * - ``da list``
     - List installed SDK versions
     - ``daml version`` prints the current SDK version in use.
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
     - ``daml navigator``
   * - ``da sandbox``
     - Launch Sandbox
     - ``daml sandbox``
   * - ``da compile``
     - Compile a DAML project into a .dar file
     - ``daml build``
   * - ``da path <component>``
     - Show the path to an SDK component
     - No equivalent
   * - ``da run``
     - Run an SDK component
     - ``daml studio``, ``daml navigator``, etc
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

Building .dar files
*******************

Compiling your DAML source code into a DAML archive (a ``.dar`` file)::

  daml build

Configuring compilation
=======================

In your project's ``daml.yaml``. The variables you can set are:

``project.name``
  The name of the project.

``project.source``
  The path to the source code.

``project.output-path``
  The directory to store generated ``.dar`` files, the default is ``target``.

The generated ``.dar`` file will be stored in
``${project.output-path}/${project.name}.dar``.


.. _assistant-manual-managing-releases:

Managing SDK releases
*********************

``daml`` automatically checks for updates and notifies you if there is
a new version. But when there's a new version, you need to specify that you want a project to use it in the project config file.

