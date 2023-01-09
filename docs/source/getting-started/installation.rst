.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Installing the SDK
##################

Install the Dependencies
************************

The Daml SDK currently runs on Windows, macOS and Linux.

You need to install:

1. `Visual Studio Code <https://code.visualstudio.com/download>`_.
2. JDK 11 or greater. If you don't already have a JDK installed, try `Eclipse Adoptium <https://adoptium.net>`_.

   As part of the installation process you may need to set up the ``JAVA_HOME`` variable. You can find instructions for this process on :doc:`Windows,macOS, and Linux here <path-variables>`.

Choose Daml Enterprise or Daml Open Source
******************************************

Daml comes in two variants: Daml Enterprise or Daml Open Source. Both include the best in class SDK, Canton and all of the components that you need to write and deploy multi-party 
applications in production, but they differ in terms of enterprise and non-functional capabilities:


.. list-table::
   :widths: 20 10 10
   :header-rows: 1

   * - Capability
     - Enterprise
     - Open Source
   * - `Sub-Transaction Privacy <https://docs.daml.com/concepts/ledger-model/ledger-privacy.html>`_
     - Yes
     - Yes
   * - `Transaction Processing <https://docs.daml.com/canton/architecture/overview.html#node-scaling>`_
     - Parallel (fast)
     - Sequential (slow)
   * - `High Availability <https://docs.daml.com/canton/usermanual/ha.html>`_
     - Yes
     - No
   * - `Horizontal scalability <https://docs.daml.com/canton/usermanual/ha.html#sequencer>`_
     - Yes
     - No
   * - `Ledger Pruning <https://docs.daml.com/canton/usermanual/pruning.html>`_
     - Yes
     - No
   * - Local contract store in PostgreSQL
     - Yes
     - Yes
   * - Local contract store in Oracle
     - Yes
     - No
   * - PostgreSQL driver
     - Yes
     - Yes
   * - Oracle driver
     - Yes
     - No
   * - Besu driver
     - Yes
     - No
   * - Fabric driver
     - Yes
     - No
   * - `Profiler <https://docs.daml.com/tools/profiler.html>`_
     - Yes
     - No
   * - `Non-repudiation Middleware <https://docs.daml.com/tools/non-repudiation.html>`_
     - Yes (early access)
     - No


Install Daml Open Source SDK
****************************

Windows 10
==========

Download and run the installer_, which will install Daml and set up the PATH variable for you.

.. _mac-linux-sdk:

Mac and Linux
=============

Open a terminal and run:

.. code-block:: shell

   curl -sSL https://get.daml.com/ | sh

The installer will setup the ``PATH`` variable for you. In order for it to take effect, you will have to
log out and log in again.

If the ``daml`` command is not available in your terminal after logging out and logging in again, you need to set the ``PATH`` environment variable
manually. You can find instructions on how to do this :doc:`here <path-variables>`.

.. _installing_daml_enterprise:

Install Daml Enterprise
***********************

If you have a license for Daml Enterprise, you
can install it as follows:


- On Windows, download the installer from Artifactory_ instead of Github
  releases. 
- On Linux and MacOS, download the corresponding tarball,
  extract it and run ``./install.sh``. Afterwards, modify the
  :ref:`global daml-config.yaml <global_daml_config>` and add an entry
  with your Artifactory API key. The API key can be found in your
  Artifactory user profile.

.. code-block:: yaml

   artifactory-api-key: YOUR_API_KEY

This will be used by the assistant to download other versions automatically from artifactory.

If you already have an existing installation, you only need to add
this entry to ``daml-config.yaml``. To overwrite a previously
installed version with the corresponding Daml Enterprise version, use
``daml install --force VERSION``.

Download Manually
*****************

If you want to verify the SDK download for security purposes before installing, you can look at :doc:`our detailed instructions for manual download and installation <manual-download>`.

Next Steps
**********

- Follow the :doc:`getting started guide </getting-started/index>`.
- Use ``daml --help`` to see all the commands that the Daml assistant (``daml``) provides.
- If you run into any other problems, you can use the :doc:`support page </support/support>` to get in touch with us.



