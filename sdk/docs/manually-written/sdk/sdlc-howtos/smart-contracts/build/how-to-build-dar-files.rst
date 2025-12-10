.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _build_howto_build_dar_files:

How to build Daml Archive (.dar) files
######################################

This guide shows you how to organize the source code defining your Daml workflows and how to build and package that code as Daml Archive (.dar) files, which you can deploy to the ledger or use to develop applications against. The guide is organized into the following smaller how-tos:

-  How to define and build one or more Daml packages
-  How to manage dependencies on third-party Daml packages
-  How to decide what Daml code to put into what package

If you would like to learn more about the exact relationship between Daml package and Daml Archive (.dar) files, see :brokenref:`Daml packages and Daml Archive (.dar) files <daml-packages-and-archives>`. However, you do not need to know this in detail to use this guide. At a high-level you can just think of a Daml archive file to be the result of building a specific Daml package.


How to define Daml packages
===========================

Single package
--------------

All Daml packages require a daml.yaml file. Create this file at the root of your package directory. You will need the following information to populate this file:

-  SDK Version: call `dpm version` to determine the installed SDK versions
-  Package name: lower-skewer-case name that is unique to your package and company.

Add the following to your ``daml.yaml``, replacing the ``<place-holders>`` as appropriate.

.. code:: yaml

  sdk-version: <such as 3.4.8>
  name: <your-package-name>
  version: 1.0.0
  source: daml
  dependencies:
    - daml-prim
    - daml-stdlib

The source code (``.daml`` files) for the package are placed in the directory specified by the source field above. Create a daml folder at the root of your package.
Write your .daml files in this directory, the file name must match the module header in the file, treating dots as directories, as shown:

-  ``MyModule.daml`` contains ``module MyModule where``
-  ``Path/To/My/Module.daml`` contains ``module Path.To.My.Module where``

Directory and .daml file names must be in UpperCamel casing.

The ``dpm new <package-name>`` command provides pre-made templates for various package structures and tutorials, see :subsiteref:`this DPM page<dpm-operate>` for more information.


.. _multi-package-build:

Multiple packages
--------------------

Your Daml project will usually need at least two packages, for workflows and testing. Daml provides support for building and developing these packages via the ``multi-package.yaml`` file.
In a directory above your package(s) directories, create a ``multi-package.yaml`` file. List the relative paths to your packages in this file using the following structure:

.. code:: yaml

  packages:
    - ./my-package1
    - ./my-package2

These are paths to the directories containing the ``daml.yaml``, not to the ``daml.yaml`` itself.

Environment variables in configuration files
----------------------------------------------

When your project has more than one package, consider using environment variables to avoid duplication of information like the ``sdk-version``.
Replace the ``sdk-version`` field with ``sdk-version: $SDK_VERSION`` (or any other valid environment variable name), and ensure this variable is set before building.
``SDK_VERSION=2.10.0 dpm build --all``

Variables can also be placed inline, and are supported on all string fields in the daml.yaml, as the following example shows:

.. code:: yaml

  sdk-version: $SDK_VERSION
  name: my-package-$PACKAGE_SUFFIX
  version: 1.0.$MAIN_PATCH
  source: daml
  dependencies:
    - daml-prim
    - daml-stdlib

See :ref:`Environment Variable Interpolation <environment-variable-interpolation>` for more information.

How to build Daml packages
==========================
To build a single package, navigate to its root directory and run ``dpm build``.
To build all packages in a multi-package project, navigate to the directory containing the ``multi-package.yaml`` and run ``dpm build --all``.
By default these will create a Daml Archive (.dar) file for each package built in ``<package-directory>/.daml/dist/<package-name>-<package-version>.dar``.
.dar files are used both for uploading to the Canton Ledger, and for package dependencies.
The location where the .dar is created can be overridden using the ``--output`` flag for dpm build, which can also be provided in the ``daml.yaml`` file under the ``build-options`` field:

.. code:: yaml

  build-options:
    - --output=./output-bin/my-package.dar

See :ref:`Daml Build Options <daml-build-flags>` for a full list of dpm build options, or run ``dpm build --help``, which includes options for changing the :brokenref:`LF <lf-version>` version and configuring warnings. All of these options can also be provided via ``build-options`` above.
Consider reading :ref:`Recommended Build Options <recommended-build-options>` for our recommended set of warning flags.

If you face issues when changing configuration options like the ``sdk-version``, or the LF version, cleaning the package(s) may help. To clean a single package, run ``daml clean`` from the package directory. To clean all packages in a project, run ``daml clean --all`` from the directory containing the ``multi-package.yaml``

How to depend on Daml packages
==============================
Dependencies in Daml are specified by their Daml Archive (.dar) file. To add a dependency to your package, add the paths to your dependency .dar files to your ``daml.yaml`` as follows:

.. code:: yaml

  ...
  data-dependencies:
    - ./path/to/your/dep.dar
    - ./path/to/a/package/.daml/dist/my-package-1.0.0.dar

Note the use of ``data-dependencies`` instead of the previously covered ``dependencies`` field, the latter is reserved for ``daml-prim``, ``daml-stdlib``, and the optional testing library :brokenref:`daml-script <daml-script>`.
Once added to the ``daml.yaml``, modules from the dependency .dar can be imported from the modules of this package. In the event of collision between module names, either with this package or other dependencies, see :ref:`module-prefixes <daml-yaml-module-prefixes>`.

When depending on .dar files from packages listed in the ``multi-package.yaml``, calling ``dpm build`` and ``dpm build --all`` will build the relevant packages in the correct order for you.

How to manage dependencies on third-party Daml packages
=======================================================
To build :brokenref:`composed transactions <how-to-compose>`, you will need to depend on the .dar files of third-party applications. At the time of writing there is no dedicated package repository for Daml Archives. However .dar files are reasonably small and change infrequently. You thus best check them into your repository, in a dars/vendored directory.
If you instead retrieve the .dar files as part of a build step, check the hashes of these dars as part of this step.

If you intend to distribute your .dar files for others to build on, include the retrieval process in your documentation.

Depending on daml-script test libraries
---------------------------------------

The ``daml-script`` library is not cross compatible with other releases from different Daml SDK versions. Therefore, when using Daml script test code shared by third-party apps, we recommend you to vendor in that Daml script code.
For example, by checking it into a ``daml/vendored/<vendored-package-name>`` directory in your repository. A good example is the Canton Network Token Standard test harness provided by splice here: https://github.com/DACH-NY/canton-network-node/tree/main/token-standard/splice-token-standard-test.
Adding these packages to your ``multi-package.yaml`` will ensure they are built as needed.

How to decide what Daml code to put into what package
=====================================================

.. todo: deduplicate with the TSA application /3.3/sdlc-howtos/sdlc-best-practices.html#dar-file-modularization

Use the following criteria to organize your project into separate packages:

**Separate workflow definitions from their tests**
    Place tests for workflow definitions in a separate package to the workflows, to avoid distributing and uploading said tests to the ledger. Specifically avoid uploading the daml-script package to any production ledger.

**Separate public APIs from implementations**
   If your application includes public APIs, intended to be used by other applications, define these APIs using Daml interfaces and place these interfaces in a different package to their implementation. See for example the interfaces defined in the Canton Network Token Standard here: https://github.com/DACH-NY/canton-network-node/blob/da5dbe251b17f9c4c5d3e96840f486d14dc8e43e/token-standard/splice-api-token-holding-v1/daml/Splice/Api/Token/HoldingV1.daml

**Separate by business domains**
   Consider splitting workflows from different business domains into separate packages, so that stakeholders from one domain do not need to audit and vet the workflows from others domains that they do not directly interact with.
