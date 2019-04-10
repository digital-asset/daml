.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Installing the SDK
##################

1. Install the dependencies
***************************

The SDK currently runs on MacOS or Linux. For Windows support, we recommend installing the SDK on a virtual machine running Linux.

You need to install:

1. `Visual Studio Code <https://code.visualstudio.com/download>`_.
2. `JDK 8 or greater <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_.

2. Set up the SDK Assistant
***************************

The SDK is distributed via a command-line tool called the :doc:`SDK Assistant </tools/assistant>`. To install the SDK Assistant:

#. Download the latest installer for your platform:

   - .. rst-class:: cta-alt
   
        `Linux installer <https://cta-redirect.hubspot.com/cta/redirect/5388578/05b2410e-fba4-4d42-b125-f7fd2dc3ba5d>`_
     
     .. raw:: html

        <!--HubSpot Call-to-Action Code --><span class="hs-cta-wrapper" id="hs-cta-wrapper-05b2410e-fba4-4d42-b125-f7fd2dc3ba5d"><span class="hs-cta-node hs-cta-05b2410e-fba4-4d42-b125-f7fd2dc3ba5d" id="hs-cta-05b2410e-fba4-4d42-b125-f7fd2dc3ba5d"><!--[if lte IE 8]><div id="hs-cta-ie-element"></div><![endif]--><a href="https://cta-redirect.hubspot.com/cta/redirect/5388578/05b2410e-fba4-4d42-b125-f7fd2dc3ba5d"  target="_blank" ><img class="hs-cta-img" id="hs-cta-img-05b2410e-fba4-4d42-b125-f7fd2dc3ba5d" style="border-width:0px;" src="https://no-cache.hubspot.com/cta/default/5388578/05b2410e-fba4-4d42-b125-f7fd2dc3ba5d.png"  alt="Linux installer"/></a></span><script charset="utf-8" src="https://js.hscta.net/cta/current.js"></script><script type="text/javascript"> hbspt.cta.load(5388578, '05b2410e-fba4-4d42-b125-f7fd2dc3ba5d', {}); </script></span><!-- end HubSpot Call-to-Action Code -->

   - .. rst-class:: cta-alt
   
        `Mac installer <https://cta-redirect.hubspot.com/cta/redirect/5388578/1b93ea71-77c6-4e0e-adbb-de072226d474>`_
     
     .. raw:: html

        <!--HubSpot Call-to-Action Code --><span class="hs-cta-wrapper" id="hs-cta-wrapper-1b93ea71-77c6-4e0e-adbb-de072226d474"><span class="hs-cta-node hs-cta-1b93ea71-77c6-4e0e-adbb-de072226d474" id="hs-cta-1b93ea71-77c6-4e0e-adbb-de072226d474"><!--[if lte IE 8]><div id="hs-cta-ie-element"></div><![endif]--><a href="https://cta-redirect.hubspot.com/cta/redirect/5388578/1b93ea71-77c6-4e0e-adbb-de072226d474"  target="_blank" ><img class="hs-cta-img" id="hs-cta-img-1b93ea71-77c6-4e0e-adbb-de072226d474" style="border-width:0px;" src="https://no-cache.hubspot.com/cta/default/5388578/1b93ea71-77c6-4e0e-adbb-de072226d474.png"  alt="Mac installer"/></a></span><script charset="utf-8" src="https://js.hscta.net/cta/current.js"></script><script type="text/javascript"> hbspt.cta.load(5388578, '1b93ea71-77c6-4e0e-adbb-de072226d474', {}); </script></span><!-- end HubSpot Call-to-Action Code -->

   - Windows installer coming soon - `sign up to be notified <https://hub.daml.com/sdk/windows>`_

   The SDK is distributed under the :download:`Apache 2.0 license </LICENSE>` (:download:`NOTICES </NOTICES>`).

#. Run the installer:

   .. code::

     sh ./da-cli-<version>.run

#. Follow the instructions in the output to update your ``PATH``.

#. Run ``da setup``, which will download and install the SDK.

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
- Use ``da --help`` to see all the commands that the SDK Assistant provides.
- If you run into any problems, :doc:`use the support page </support/support>` to get in touch with us.
