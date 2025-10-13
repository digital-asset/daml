.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _daml-studio:

Daml Studio
###########

Daml Studio is an integrated development environment (IDE) for Daml. It is an extension on top of `Visual Studio Code <https://code.visualstudio.com>`__ (VS Code), a cross-platform, open-source editor providing a `rich code editing experience <https://code.visualstudio.com/docs/editor/editingevolved>`__.

Install
*******

Install Daml Studio by :ref:`installing the Daml Assistant <daml-assistant-install>`, and ensuring you have `Visual Studio Code <https://code.visualstudio.com>`__ version 1.87 or above installed.

Configure
*********
With Daml Studio open, navigate to ``Extensions`` on the left (Ctrl+Shift+X). Search for `Daml` and click the Cog icon on the first result, then press settings.
The options available are listed in the :ref:`Reference <daml-studio-options>`.

Operate
*******
Navigate to the root of your package (directory containing ``daml.yaml``) or project (directory containing ``multi-package.yaml``) and run
``dpm studio``. This will open Visual Studio Code with the Daml extension installed and running.

Go to definition
================
Press ``F12``, or right click a Daml definition and select "go to definition" to see where a value/type is defined. This will work for dependencies and data-dependencies.

`Visual Studio Code Documentation <https://code.visualstudio.com/docs/editor/editingevolved#_go-to-definition>`__.

Peak definition
===============
Use ``⌥F12`` on Macos, or ``Ctrl + Shift + F10`` on Windows/MacOS, to peak inline at the definition of a value value/type.

`Visual Studio Code Documentation <https://code.visualstudio.com/docs/editor/editingevolved#_peek>`__.

Hover tooltips
==============
Hovering over a Daml value will display its source location and type.
Hovering over a Daml type will display its source location.

`Visual Studio Code Documentation <https://code.visualstudio.com/docs/editor/editingevolved#_hover>`__.

Diagnostics
===========
Errors or warnings that would be raised by calling ``dpm build`` will be shown in-line in the IDE. These diagnostics are affected by the flags
provided in ``build-options``, See :ref:`Daml Yaml Build Options <daml-yaml-build-options>`.

Daml Snippets
=============
Daml IDE provides a set of autocomplete snippets for common daml patterns,
such as default template and choice definitions. Typing the start of these definitions
(such as the word "template" or "choice") will show these snippets.

You can develop your own snippets by following the instructions in
`Creating your own Snippets <https://code.visualstudio.com/docs/editor/userdefinedsnippets>`__ to create an appropriate ``daml.json``
snippet file.

.. _script-results:

Daml script results
===================
Above any top level definition that would be run as part of ``dpm test`` (See :ref:`Daml Test <daml-assistant-test>`), a "Script Results" button will appear.

Clicking this will open a side panel, with the result of running that test, along with information about logging and the ledger
state.  

Changing the Daml code for that test will update the side panel in real time, and will show an error if the code does not compile.  
At the top of the panel is a toggle button between ``table`` and ``transaction`` view.  

**Table view**

Table view shows all contracts that were created during the test, as well as their payloads and which parties observed them.
There are also check boxes on the top for showing contracts that were archived, and for more information about which parties
they were divulged to. When using the ``Show detailed disclosure`` checkbox, the party observed contract column will show one of the following:

- ``S``, the party sees the contract because they are a signatory on the contract.
- ``O``, the party sees the contract because they are an observer on the contract.
- ``W``, the party sees the contract because they witnessed the creation of this contract, e.g.,
   because they are an actor on the ``exercise`` that created it.
- ``D``, the party sees the contract because they have been divulged the contract, e.g.,
   because they witnessed an exercise that resulted in a ``fetch`` of this contract.

**Transaction view**

Transaction view shows the generated transaction, logs and test result.  
Various IDs within this view, such as template names, contract IDs, transaction IDs, etc. will be links to their respective source.

Multi-package environments
==========================
Daml Studio will create a separate compiler environment for each package, based on the contents of each ``daml.yaml``.
If a ``multi-package.yaml`` is present (in the directory that Daml Studio was opened in), Daml Studio will allow Jump To Definition to jump to packages listed in this file,
rather than unpacked from the package-database.

Package environments will be reloaded whenever the relevant ``daml.yaml`` file is changed, or any dependencies DARs are updated.
As a result, running ``dpm build --all`` will usually cause every package environment to reload.
For changes across dependencies (i.e. change to a datatype defined in one packaged and used in another), 
the former packages .dar file must be rebuilt for the latter to use these changes.  

Package environments do not need to use the same SDK version as your main package, Daml Studio will use the version
specified in the relevant packages ``daml.yaml`` to give diagnostics for that file. See :ref:`Automatic SDK Installation <daml-studio-automatic-sdk-installation>`
for how Daml Studio helps with packages using non-installed Daml SDK Versions.

If Daml Studio is unable to start a package environment for a package, for example an unparsable ``daml.yaml``, or missing dependency ``.dar``, 
Daml Studio will show this diagnostic at the top of the package's ``daml.yaml`` file. Saving this file will trigger Daml Studio
to try again.

**Root Multi-package SDK**

As discussed above, each package runs its own environment, these environments are managed by the
root environment, which if not specified, will be the most recent SDK on your system.  
You can override this version by providing a ``daml.yaml`` file at the root of your project (i.e. next to the ``multi-package.yaml``)
containing only the following:

.. code-block:: yaml

  sdk-version: <root-sdk-version>

.. _daml-studio-jump-to-def:

Jump to definition for dependencies
===================================
Daml Studio supports unpacking dependencies to allow the user to jump to their source code.
However, Daml Studio can only do this is the packages DAR is available. A .dar file will hold
the source code it the main package it contains, but not to any dependencies that have been bundled with it.
If you wish to jump to the source code on these transitive dependencies, their .dar paths can be listed either
in the ``daml.yaml`` of your main package, or under the ``dars:`` field in the ``multi-package.yaml``, which exists
only for this purpose. An example is given below:

.. code:: yaml

  packages:
  - my-package
  - libs/my-lib
  dars:
  - ./dars/my-transitive-dependency-1.0.0.dar

.. _daml-studio-automatic-sdk-installation:

Automatic SDK installation
==========================
If you open a ``.daml`` file for a package using an SDK version you do not have installed, Daml Studio
will show a notification for installing this SDK version. This notification will then show
status for the installation, and includes a cancel button. Daml Studio cannot provide diagnostics
to a package for which the SDK version is not installed.

Directory Envrionment Tools (direnv)
====================================
Tools like ``direnv`` are commonly used to set up dependencies and import environment variables
for use with :ref:`environment variable interpolation support <environment-variable-interpolation>`. To make this work in Daml Studio,
you need a VSCode extension that sets this up for other extensions.
In the case of ``direnv`` specifically (i.e. you have a ``.envrc`` file), we
recommend using `this direnv extension by Martin Kühl <https://marketplace.visualstudio.com/items?itemName=mkhl.direnv>`__, which we have verified is compatible.
Other direnv extensions may not correctly pass environment information to the Daml Studio extension.

If the Daml extension detects a ``.envrc`` file, it recommends this extension within the IDE with the
following message:

.. code::

  Found an .envrc file but the recommended direnv VSCode extension is not installed. Daml IDE may fail to start due to missing environment variables.
  Would you like to install the recommended direnv extension or attempt to continue without it?

It also provides a link to the extension on the VS Code extension marketplace.

Limitations
***********

- Jumping to non-local dependencies does not currently retain the build-options and module-prefixes
  for that package. This means that if you jump to a dependency that requires either of these to build,
  the editor shows errors in the source code.
- Some links in the Script Results tab may not resolve correctly cross-package.
- Packages with symlinks between the daml.yaml and source files may not give correct intelligence.

Upgrade
*******
Daml Studio will upgrade as Daml Assistant upgrades, and as new versions of the extension are published to VSCode marketplace.

See :ref:`Daml Assistant Upgrade <daml-assistant-upgrade>` and the ``--replace`` flag in :ref:`Reference <daml-studio-cli-options>` for more details.

Decomission
***********
Open Visual Studio Code (not ``daml studio``), navigate to ``Extensions`` on the left (Ctrl+Shift+X). Search for `Daml` and click the top result.  
This will open the ``Extension: Daml`` page, which includes an uninstall button to the right of the Daml Extension icon.  
Follow the :ref:`Daml Assistant Decomission <daml-assistant-decomission>` for removal Daml itself.

Troubleshoot
************

- 
  ``Error loading webview: Error: Could not register service worker ...`` when clicking ``Script Results``  

  This can occur when you have multiple VSCode instances in the same project. Fix this by closing VSCode, and killing all processes:  
  - Linux/MacOS: ``killall code``
  - Windows: Open Task Manager (Ctrl + Shift + Esc) and kill all VSCode processes.
-
  Issues with processes left behind, or read/write lock errors on build with Daml Studio open

  Please report this on the `GitHub Issues page <https://github.com/digital-asset/daml/issues>`__, then disable ``Multi-IDE`` support as
  shown in the :ref:`Reference <daml-studio-multi-ide-support>`.
- 
  Any other issues

  It may help to read the Daml Studio output logs, and include them in a bug report on `GitHub Issues page <https://github.com/digital-asset/daml/issues>`__.  
  On the top bar, click View->Output, the on the drop-down menu on the top right of the new window (May currently say ``Tasks``), select ``Daml Language Server``.

Contribute
**********
See the open source GitHub repository: https://github.com/digital-asset/daml 

References
**********

.. _daml-studio-options:

Daml Studio Extension options
=============================
The Daml Studio Extension contains the following options:

- | ``Autorun All Tests``  
  | Default: false  
  | Run all tests in a file once it's opened, instead of waiting for the user to select individual tests.
- | ``Extra Arguments``  
  | Extra arguments passed to ``damlc ide``. This can be used to enable additional warnings via ``--ghc-option -W``  
  | Consider using the ``build-options`` field in the ``daml.yaml`` before resorting to this option, as these options are
  | more difficult to distribute.
- | ``Log Level``  
  | Default: Warning  
  | Sets the logging threshold of the daml-ide and multi-ide

  .. _daml-studio-multi-ide-support:
- | ``Multi Package Ide Support``  
  | Default: true  
  | Enables support for multi-package projects in the IDE
- | ``Profile``  
  | Default: false
  | Profile the daml ide plugin, may effect performance
- | ``Telemetry``
  | Default: From consent popup  
  | Controls whether you send Daml usage data to Digital Asset

.. _daml-studio-cli-options:

Daml Studio CLI options
=======================
The ``daml studio`` CLI command (which opens the Daml Studio editor) takes the following flags:

- 
  ``--replace``

  Value: ``published``, ``always``, ``never``  

  Default: ``published``  

  This flag controls when the ``daml studio`` command replaces the VSCode extension in your editor.  

  - ``published`` will always use the most recent published (to the VSCode marketplace) version of the Daml Studio VSCode extension.  
  - ``always`` will use the extension bundled with whichever SDK version is being used (as selected by the ``daml.yaml``, See :ref:`Daml Assistant version management <daml-assistant-version-management>`.  
  - ``never`` will not change the extension that is installed.  

  If you need to use a much older version of Daml, you may need to use ``--replace=always`` to launch Daml Studio correctly.
