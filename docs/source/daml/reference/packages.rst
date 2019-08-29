.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


Reference: DAML packages
########################

This page gives reference information on DAML package dependencies:

.. contents:: :local:

DAML archives
*************

When a DAML project is build with ``daml build``, build artifacts are generated in the hidden
directory ``.daml/dist/`` relative to the project root directory. The main build artifact of a
project is the `DAML archive`, recognized by the ``.dar`` file ending. DAML archives are platform
independent. They can be deployed on a ledger (see :ref:`deploy <deploy-ref_index>`) or can be
imported into other projects as a package dependency.

Importing DAML archives
***********************

A DAML project can import DAML archive dependencies. Note that currently there is no tooling for
DAML package management. To import a package ``Bar`` in project ``Foo``, add the file path of the
``Bar`` DAML archive to the ``dependencies`` section of the `daml.yaml` project file:

.. code-block:: yaml

  sdk-version: 0.0.0
  name: foo
  source: daml
  version: 1.0.0
  exposed-modules:
    - Some.Module
    - Some.Other.Module
  dependencies:
    - daml-prim
    - daml-stdlib
    - /home/johndoe/bar/.daml/dist/bar.dar

The import path needs to be the relative or absolute path pointing to the created DAML archive of
the ``bar`` project. The archive can reside anywhere on the local file system. Note that the SDK
versions of the packages ``foo`` and ``bar`` need to match, i.e. it is an error to import a package
that was created with an older SDK.

Once a package has been added to the dependencies of the ``foo`` project, modules of ``bar`` can be
imported as usual with the ``import Some.Module`` directive (see :ref:`Imports <daml-ref-imports>`).
If both projects ``foo`` as well as ``bar`` contain a module with the same name, the import can be
disambiguated by adding the package name in front of the module name, e.g. ``import "bar"
Some.Module``.

Note that all modules of package ``foo`` that should be available as imports of other packages need
to be exposed by adding them to the ``exposed-modules`` stanza of the `daml.yaml` file. If the
``exposed-modules`` stanza is omitted, all modules of the project are exposed by default.
