.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Installing the SDK
##################

1. Install the dependencies
***************************

The SDK currently runs on Windows, MacOS or Linux.

You need to install:

1. `Visual Studio Code <https://code.visualstudio.com/download>`_.
2. JDK 8 or greater.

   You can get the JDK from `Zulu 8 JDK <https://www.azul.com/downloads/zulu/>`_ or `Oracle 8 JDK <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_ (requires you to accept Oracle's license).

2. Install the SDK
*******************

Mac and Linux
=============

To install the SDK on Mac or Linux:

1. Run::

     curl -sSL https://get.daml.com/ | sh
2. If prompted, add ``~/.daml/bin`` to your PATH.

   If you don't know how to do this, try following `these instructions for MacOS <https://hathaway.cc/2008/06/how-to-edit-your-path-environment-variables-on-mac/>`_ or `these instructions for Windows <https://www.java.com/en/download/help/path.xml>`_.

Windows
=======

We support running the SDK on Windows 10. To install the SDK on Windows, download and run the installer from `github.com/digital-asset/daml/releases/latest <https://github.com/digital-asset/daml/releases/latest>`__.

Next steps
**********

- Follow the :doc:`quickstart guide <quickstart>`.
- Use ``daml --help`` to see all the commands that the DAML assistant (``daml``) provides.
- If you run into any problems, :doc:`use the support page </support/support>` to get in touch with us.
