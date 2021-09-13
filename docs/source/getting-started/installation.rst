.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Installing the SDK
##################

1. Install the dependencies
***************************

The Daml Connect SDK currently runs on Windows, macOS and Linux.

You need to install:

1. `Visual Studio Code <https://code.visualstudio.com/download>`_.
2. JDK 8 or greater. If you don't already have a JDK installed, try `Eclipse Adoptium <https://adoptium.net>`_.

   As part of the installation process you might need to set up the ``JAVA_HOME`` variable. You can find here the instructions on how to do it on :doc:`Windows,macOS, and Linux <path-variables>`.

2. Install the SDK
*******************

Windows 10
==========

Download and run the installer_, which will install Daml and set up your PATH.

.. _mac-linux-sdk:

Mac and Linux
=============

To install the SDK on Mac or Linux open a terminal and run:

.. code-block:: shell

   curl -sSL https://get.daml.com/ | sh

The installer will setup the ``PATH`` variable for you. In order for it to take effect, you will have to
log out and log in again.

Installing the Enterprise Edition
*********************************

If you have a license for the enterprise edition of Daml Connect, you
can install it as follows:

On Windows, download the installer from Artifactory_ instead of Github
releases. On Linux and MacOS download the corresponding tarball,
extract it and run ``./install.sh``. Afterwards, modify the
:ref:`global daml-config.yaml <global_daml_config>` and add an entry
with your Artifactory API key. The API key can be found in your
Artifactory user profile.

.. code-block:: yaml

   artifactory-api-key: YOUR_API_KEY

This will be used by the assistant to download other versions automatically from artifactory.

If you already have an existing installation, you only need to add
this entry to ``daml-config.yaml``. To overwrite a previously
installed version with the corresponding enterprise edition, use
``daml install --force VERSION``.

Next steps
**********

- Follow the :doc:`getting started guide </getting-started/index>`.
- Use ``daml --help`` to see all the commands that the Daml assistant (``daml``) provides.
- If the ``daml`` command is not available in your terminal after logging out and logging in again, you need to set the ``PATH`` environment variable
  manually. You can find instructions on how to do this :doc:`here <path-variables>`.
- If you run into any other problems, you can use the :doc:`support page </support/support>` to get in touch with us.



Alternative: manual download
****************************

If you want to verify the SDK download for security purposes before installing, you can look at :doc:`our detailed instructions for manual download and installation <manual-download>`.

.. toctree::
   :hidden:

   path-variables
   manual-download
