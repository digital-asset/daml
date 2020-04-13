.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

- Follow the :doc:`getting started guide </getting-started/index>`.
- Use ``daml --help`` to see all the commands that the DAML assistant (``daml``) provides.
- If you run into any problems, :doc:`use the support page </support/support>` to get in touch with us.

Alternative: manual download
****************************

The cURL command above will automatically download and run the DAML installation script from GitHub (using TLS). If you require a higher level of security, you can instead install the SDK by manually downloading the compressed tarball, verifying its signature, extracting it and manually running the install script.

Note that the Windows installer is already signed (within the binary itself), and that signature is checked by Windows before starting it. Nevertheless, you can still follow the steps below to check its external signature file.

To do that:

1. Go to https://github.com/digital-asset/daml/releases. Confirm your browser sees a valid certificate for the github.com domain.
2. Download the artifact (*Assets* section, after the release notes) for your platform as well as the corresponding signature file. For example, if you are on macOS and want to install release 0.13.27, you would download the files ``daml-sdk-0.13.27-macos.tar.gz`` and ``daml-sdk-0.13.27-macos.tar.gz.asc``. Note that for Windows you can choose between the tarball, which follows the same instructions as the Linux and macOS ones (but assumes you have a number of typical Unix tools installed), or the installer, which ends with ``.exe``. Regardless, the steps to verify the signature are the same.
3. To verify the signature, you need to have ``gpg`` installed (see https://gnupg.org for more information on that) and the Digital Asset Security Public Key imported into your keychain. Once you have ``gpg`` installed, you can import the key by running::

     gpg --keyserver pool.sks-keyservers.net --search 4911A8DFE976ACDFA07130DBE8372C0C1C734C51

   This should come back with a key belonging to ``Digital Asset Holdings, LLC <security@digitalasset.com>``, created on 2019-05-16 and expiring on 2021-05-15. If any of those details are different, something is wrong. In that case please contact Digital Asset immediately.
4. Once the key is imported, you can ask ``gpg`` to verify that the file you have downloaded has indeed been signed by that key. Continuing with our example of v0.13.27 on macOS, you should have both files in the current directory and run::

     gpg --verify daml-sdk-0.13.27-macos.tar.gz.asc

   and that should give you a result that looks like::

     gpg: assuming signed data in 'daml-sdk-0.13.27-macos.tar.gz'
     gpg: Signature made Wed Sep 25 11:57:28 2019 BST
     gpg:                using RSA key E8372C0C1C734C51
     gpg: Good signature from "Digital Asset Holdings, LLC <security@digitalasset.com>" [unknown]
     gpg: WARNING: This key is not certified with a trusted signature!
     gpg:          There is no indication that the signature belongs to the owner.
     Primary key fingerprint: 4911 A8DF E976 ACDF A071  30DB E837 2C0C 1C73 4C51

   Note: This warning means you have not told gnupg that you trust this key actually belongs to Digital Asset. The ``[unknown]`` tag next to the key has the same meaning: ``gpg`` relies on a web of trust, and you have not told it how far you trust this key. Nevertheless, at this point you have verified that this is indeed the key that has been used to sign the archive.

5. The next step is to extract the tarball and run the install script (unless you chose the Windows installer, in which case the next step is to double-click it)::

     tar xzf daml-sdl-0.13.27-macos.tar.gz
     cd sdk-0.13.27
     ./install.sh

6. Just like for the more automated install procedure, you may want to add ``~/.daml/bin`` to your ``$PATH``.
