.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Installing the SDK
##################

1. Installing the Dependencies
******************************

The Daml SDK currently runs on Windows, macOS and Linux.

You need to install:

1. `Visual Studio Code <https://code.visualstudio.com/download>`_.
2. JDK 11 or greater. If you don't already have a JDK installed, try `Eclipse Adoptium <https://adoptium.net>`_.

   As part of the installation process you may need to set up the ``JAVA_HOME`` variable. You can find instructions for this process on :doc:`Windows,macOS, and Linux here <path-variables>`.

2. Installing the SDK
*********************

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

Installing Daml Enterprise
**************************

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

Downloading Manually
********************

If you want to verify the SDK download for security purposes before installing, you can look at :doc:`our detailed instructions for manual download and installation <manual-download>`.

Next Steps
**********

- Follow the :doc:`getting started guide </getting-started/index>`.
- Use ``daml --help`` to see all the commands that the Daml assistant (``daml``) provides.
- If you run into any other problems, you can use the :doc:`support page </support/support>` to get in touch with us.

.. toctree::
   :hidden:

   path-variables
   manual-download
