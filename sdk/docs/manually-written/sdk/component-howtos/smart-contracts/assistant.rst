.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _daml-assistant:

Daml Assistant
##############

Overview
********
The Daml Assistant provides a uniform CLI and installation experience for the components of the Daml SDK.
Use the Daml Assistant to develop your Daml code.

.. _daml-assistant-install:

Install
*******
For Linux and MacOS, run

.. code:: bash

  curl -sSL https://get.daml.com/ | sh

This will install the latest stable Daml SDK. To install a specific version, run:

.. code:: bash

  curl -sSL https://get.daml.com/ | sh -s 3.4.0

For Windows 10/11, download and run the installer from the `GitHub releases page <https://github.com/digital-asset/daml/releases>`__.

These methods will install the Daml SDK with its Daml Assistant, and add daml to your PATH, though you may need to restart your terminal for this to take effect.

Available SDK versions can be found on the `GitHub releases page <https://github.com/digital-asset/daml/releases>`__.

Configure
*********
Daml assistant uses a ``daml-config.yaml`` file to configure SDK installation behaviour.  
On Linux/MacOS, this will be in ``$DAML_HOME``. On Windows, it will be in ``%DAML_HOME%``.  

.. _global_daml_config:

In this file you can configure the following:

- | ``auto-install``
  | Value: true or false
  | Default: true on Linux and MacOS, false on Windows.
  | Determines whether the Daml Assistant should automatically install an SDK if a package uses it, and it isn’t installed.
- | ``update-check``
  | Value: Integer or never
  | Default: 86400 (1 day)
  | How often the Daml Assistant will check for new versions of the SDK, in seconds.
- | ``artifactory-api-key``
  | Value: String
  | Default: None
  | If you have a license for Daml EE, you can use this to specify the Artifactory API key displayed in your user profile. The assistant will use this to download the EE edition.

Operate
*******

.. _daml-assistant-version-management:

Version Management
==================
Daml Assistant supports installation of several versions of the SDK at the same time.  

Run ``daml install <sdk-version>`` to install additional SDKs. The version string can be found on the `GitHub releases page <https://github.com/digital-asset/daml/releases>`__.  

Run ``daml uninstall <sdk-version>`` to remove installed instances.  

Run ``daml version`` to list all installed instances. This will also show the default SDK version (the highest version), as well as the selected version, if specified by a ``daml.yaml`` file.  
If the ``daml.yaml`` specifies an SDK version you do not have installed, the daml version output will specify this. If ``auto-install`` is set to ``true`` in ``daml-config.yaml`` above, running any package command (such as ``daml build``) will first install this SDK for you.

Daml Command - Build and Clean
==============================
Given a ``daml.yaml`` and ``.daml`` source files, the ``daml build`` command will generate a .dar for this package. See :ref:`How to build Daml Archives <build_howto_build_dar_files>` for how to define a package and build it to a DAR.  
The ``daml clean`` command will remove any Daml artifact files created in your package during a daml build, including the .dar.

.. _daml-assistant-test:

Daml Command - Test
===================
The ``daml test`` command runs all daml scripts defined within a package.
Daml Scripts are top level values of type ``Script ()``, from the ``daml-script`` package. This package mimics a Canton Ledger Client for quick iterative testing,
and direct support within :ref:`Daml Studio <daml-studio>`. Daml Test runs these scripts against a reference Ledger called the IDE Ledger, which implements the core functionality of the Canton Ledger
without the complexity of multi-participant setups. It is most useful for verifying the fundamentals of your ledger model, before moving onto integration testing via
the Ledger API directly, or the Daml Codegen.
Daml Test also provides code coverage information for templates and choices used.

Other Daml Commands
===================

- | ``daml studio``:
  | See :ref:`Daml Studio <daml-studio>`

  .. _daml-assistant-new:

- | ``daml new <name>``: 
  | Creates a package skeleton with the given name/directory. Use the ``--template`` flag to select a different package template. Run ``daml new --list`` for a list of these templates.
- | ``daml start``:
  | Starts a Daml Sandbox and upload this package. See :ref:`Daml Sandbox <sandbox-manual>`
- | ``daml sandbox``:
  | Starts a Daml Sandbox. See :ref:`Daml Sandbox <sandbox-manual>`
- 
  ``daml damlc``:  
  Sub-command for the Daml Compiler.  
  Commands like ``daml build`` and ``daml test`` redirect to this sub-command, and thus are repeated. Some commands are only accessible via ``daml damlc <command>``, as follows:

  - | ``daml damlc inspect FILE``:
    | Pretty print a DALF file or the main DALF of a DAR file.
  - | ``daml damlc inspect-dar FILE``:
    | Given a path to a .dar file, this will give information about the packages contained within the DAR.
  - | ``daml damlc docs``
    | This can be used to generate documentation from Daml Documentation Annotations in Daml code. See :brokenref:`Daml Docs <daml-docs>` for more information.
  - | ``daml damlc lint``:
    | This provides code improvement suggestions for your daml code.

- | ``daml codegen``:
  | See :ref:`Daml Codegen for Java <component-howtos-application-development-daml-codegen-java>` and :ref:`Daml Codegen for Javascript <component-howtos-application-development-daml-codegen-javascript>`
- | ``daml script``:
  | See :brokenref:`Daml Script <daml-script>`
- | ``canton-console``:
  | See :ref:`Canton Console <running-canton-console-against-daml-sandbox>`
- | ``upgrade-check``:
  | See :ref:`Upgrade Check Tool <upgrade-check-tool>`

.. _daml-assistant-upgrade:

Upgrade
*******
The Daml Assistant will automatically upgrade whenever you install a more recent version of the Daml SDK (using the daml install command above).  

If you wish to specify the version of the Daml Assistant (Note that this is just the Assistant, not the SDK, which contains the compiler. The version of the SDK is controlled by the ``daml.yaml``), then use ``daml install <version> --install-assistant=yes``. This can be used for versions that are already installed.

.. _daml-assistant-decomission:

Decomission
***********
Linux/MacOS
===========
Run ``rm -rf $DAML_HOME``, then one of the following based on which shell you are running:

- Zsh: ``sed -i -E '/^export PATH=\$PATH:[^ ;\n]+\.daml/bin/d' ~/.zprofile``
- Bash: ``sed -i -E '/^export PATH=\$PATH:[^ ;\n]+\.daml/bin/d' ~/.bash_profile``
- Sh: ``sed -i -E '/^export PATH=\$PATH:[^ ;\n]+\.daml/bin/d' ~/.profile``

Windows
=======
Uninstall Daml via the Add or Remove Programs interface, as you would any other Windows program.


Troubleshoot
************
``Command 'daml' not found, did you mean:``  

Try adding Daml to your PATH manually, by adding ``export PATH=$PATH:$DAML_HOME/bin`` to your ``zprofile``/``bash_profile``/``profile`` file, depending on which shell you are using.

.. Consider adding Sdk version build error - install that SDK
.. Maybe the error for when the enterprise artifactory key is wrong
.. Caution, this section could become very large, we should be conservative with what we include here.


The Daml installation requires around 1.5 GB of disk space. If there is not enough space on disk, the installer outputs the error:
``
resource exhausted (No space left on device)
``

In order to fix this problem, one needs to ensure sufficient disk space overall. In particular, the directory where the Daml SDK installs, 
and the temporary files directory need to have sufficient space (i.e., in the order of 2GiB each):

* On Windows, one can find the Daml SDK home directory in the ``%DAML_HOME%`` environment variable, which default to ``%APPDATA%/daml``, and the
  temporary files directory in the ``%TEMP%`` environment variable
* On Linux and MacOS, one can find the Daml SDK home directory also in the ``DAML_HOME`` environment variable, which defaults to ``~/.daml``, 
  and the temporary files directory in the ``TMPDIR`` environment variable 

If the installer cannot ensure sufficient space in the Daml SDK's home directory (``DAML_HOME``)  and the temporary files directory, 
an alternative is to change the location of these directories before the installation.
To do so, call the installer with ``DAML_HOME`` and ``TMPDIR`` environment variables pointing to directories that have sufficient space:

* On Windows, first set the environment variables. Then run the installation as usual.
* On Linus and MacOS, run in a terminal:

.. code-block: sh
  
  DAML_HOME=<YOUR-TARGET-DIR> TMPDIR=<YOUR-TEMPDIR> curl -sSL https://get.daml.com/ | sh

Avoid choosing directories for ``DAML_HOME`` and ``TMPDIR`` environment variables that are located inside the unzipped installation
directory ``sdk-x.x.x``, as otherwise the installer fails.

Contribute
**********
See the open source GitHub repository: https://github.com/digital-asset/daml 

References
**********
CLI flags
=========
See :ref:`Daml Assistant Flags <daml-assistant-flags>`

Configuration files
===================
See :ref:`Daml Asssistant Configuration Files <daml-assistant-config-files>`
