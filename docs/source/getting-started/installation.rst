.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Installing the SDK
##################

1. Install the dependencies
***************************

The SDK currently runs on MacOS or Linux. For Windows support, we recommend installing the SDK on a virtual machine running Linux.

You need to install:

1. `Visual Studio Code <https://code.visualstudio.com/download>`_.
2. `JDK 8 <https://openjdk.java.net/install/>`_.

2. Install the SDK
*******************

Mac and Linux
=============

To install the SDK on Mac or Linux:

1. Run::

     curl -sSL https://get.daml.com/ | sh
2. If prompted, add ``~/.daml/bin`` to your PATH.

Windows
=======

To install the SDK on Windows, download and run the installer from `github.com/digital-asset/daml/releases/latest <https://github.com/digital-asset/daml/releases/latest>`__.

.. _setup-maven-project:

3. Configure Maven
******************

To use the Java bindings (and to follow the quickstart guide), you need to install Maven and configure it to use the Digital Asset repository:

#. Install `Maven <https://maven.apache.org/>`_.
#. Download `settings.xml <https://bintray.com/repo/downloadMavenRepoSettingsFile/downloadSettings?repoPath=%2Fdigitalassetsdk%2FDigitalAssetSDK>`_.
#. Copy the downloaded file to ``~/.m2/settings.xml``. If you already have ``~/.m2/settings.xml``, integrate the downloaded file with it instead.

Next steps
**********

- Follow the :doc:`quickstart guide <quickstart>`.
- Read the :doc:`introduction <introduction>` page.
- Use ``daml --help`` to see all the commands that the DAML assistant (``daml``) provides.
- If you run into any problems, :doc:`use the support page </support/support>` to get in touch with us.
