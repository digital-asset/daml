.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Installing the SDK
##################

1. Install the dependencies
***************************

The SDK currently runs on Windows, macOS and Linux.

You need to install:

1. `Visual Studio Code <https://code.visualstudio.com/download>`_.
2. JDK 8 or greater. If you don't already have a JDK installed, try `AdoptOpenJDK <https://adoptopenjdk.net>`_.

   As part of the installation process you might need to set up the ``JAVA_HOME`` variable. You can find here the instructions on how to do it on :doc:`Windows,macOS, and Linux <path-variables>`.

2. Install the SDK
*******************

Windows 10
==========

Download and run the installer_, which will install DAML and set up your PATH.

Mac and Linux
=============

To install the SDK on Mac or Linux:

1. In a terminal, run:

   .. code-block:: shell

      curl -sSL https://get.daml.com/ | sh

2. Add ``~/.daml/bin`` to your PATH. You can find the Mac OS and Linux instructions :doc:`here <path-variables>`.

Next steps
**********

- Follow the :doc:`getting started guide </getting-started/index>`.
- Use ``daml --help`` to see all the commands that the DAML assistant (``daml``) provides.
- If you run into any problems, :doc:`use the support page </support/support>` to get in touch with us.

Alternative: manual download
****************************

If you want to verify the SDK download for security purposes before installing, you can look at :doc:`our detailed instructions for manual download and installation <manual-download>`.

.. toctree::
   :hidden:

   path-variables
   manual-download

