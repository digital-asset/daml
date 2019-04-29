.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Moving to the new DAML assistant
################################

New command-line tool for working with the DAML SDK: DAML Assistant, or ``daml``. Many commands are similar to using the old SDK Assistant (``da``), but with some changes:

- Simplified installation process
- Consistent behaviour of the ``new`` command
- In-built templates mechanism mostly gone, replaced with more standard mechanism of cloning from open-source repositories
- Components don't run in the background, so you stop them with ``ctrl+c``

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
