.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


Reference: DAML packages
########################

This page gives reference information on DAML package dependencies:

.. contents:: :local:

Building DAML archives
**********************

When a DAML project is compiled, the compiler produces a `DAML archive`. These are platform-independent packages of compiled DAML code that can be uploaded to a DAML ledger or imported in other DAML projects.

DAML archives have a ``.dar`` file ending. By default, when you run ``daml build``, it will generate the ``.dar`` file in the ``.daml/dist`` folder in the project root folder. For example, running ``daml build`` in project ``foo`` with project version ``0.0.1`` will result in a DAML archive ``.daml/dist/foo-0.0.1.dar``.

You can specify a different path for the DAML archive by using the ``-o`` flag:

.. code-block:: sh

  daml build -o foo.dar

For details on how to upload a DAML archive to the ledger, see the :ref:`deploy documentation <deploy-ref_overview>`. The rest of this page will focus on how to import a DAML packages in other DAML projects.

.. _inspecting_dars:

Inspecting DARs
***************

To inspect a DAR and get information about the packages inside it, you
can use the ``daml damlc inspect-dar`` command. This is often useful
to find the package id of the project you just built.

You can run ``daml damlc inspect-dar /path/to/your.dar`` to get a
human-readable listing of the files inside it and a list of packages
and their package ids. Here is a (shortened) example output:

.. code-block:: sh

  $ daml damlc inspect-dar .daml/dist/create-daml-app-0.1.0.dar
  DAR archive contains the following files:

  create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d.dalf
  create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/daml-prim-75b070729b1fbd37a618493652121b0d6f5983b787e35179e52d048db70e9f15.dalf
  create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/daml-stdlib-0.0.0-a535cbc3657b8df953a50aaef5a4cd224574549c83ca4377e8219aadea14f21a.dalf
  create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/daml-stdlib-DA-Internal-Template-d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662.dalf
  create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/data/create-daml-app-0.1.0.conf
  create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/User.daml
  create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/User.hi
  create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/User.hie
  META-INF/MANIFEST.MF

  DAR archive contains the following packages:

  create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d "29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d"
  daml-stdlib-DA-Internal-Template-d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662 "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662"
  daml-prim-75b070729b1fbd37a618493652121b0d6f5983b787e35179e52d048db70e9f15 "75b070729b1fbd37a618493652121b0d6f5983b787e35179e52d048db70e9f15"
  daml-stdlib-0.0.0-a535cbc3657b8df953a50aaef5a4cd224574549c83ca4377e8219aadea14f21a "a535cbc3657b8df953a50aaef5a4cd224574549c83ca4377e8219aadea14f21a"

In addition to the human-readable output, you can also get the output
as JSON. This is easier to consume programatically and it is more
robust to changes across SDK versions:

.. code-block:: sh

  $ daml damlc inspect-dar --json .daml/dist/create-daml-app-0.1.0.dar
  {
      "packages": {
          "29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d": {
              "path": "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d.dalf",
              "name": "create-daml-app",
              "version": "0.1.0"
          },
          "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662": {
              "path": "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/daml-stdlib-DA-Internal-Template-d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662.dalf",
              "name": null,
              "version": null
          },
          "75b070729b1fbd37a618493652121b0d6f5983b787e35179e52d048db70e9f15": {
              "path": "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/daml-prim-75b070729b1fbd37a618493652121b0d6f5983b787e35179e52d048db70e9f15.dalf",
              "name": "daml-prim",
              "version": "0.0.0"
          },
          "a535cbc3657b8df953a50aaef5a4cd224574549c83ca4377e8219aadea14f21a": {
              "path": "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/daml-stdlib-0.0.0-a535cbc3657b8df953a50aaef5a4cd224574549c83ca4377e8219aadea14f21a.dalf",
              "name": "daml-stdlib",
              "version": "0.0.0"
          }
      },
      "main_package_id": "29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d",
      "files": [
          "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d.dalf",
          "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/daml-prim-75b070729b1fbd37a618493652121b0d6f5983b787e35179e52d048db70e9f15.dalf",
          "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/daml-stdlib-0.0.0-a535cbc3657b8df953a50aaef5a4cd224574549c83ca4377e8219aadea14f21a.dalf",
          "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/daml-stdlib-DA-Internal-Template-d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662.dalf",
          "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/data/create-daml-app-0.1.0.conf",
          "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/User.daml",
          "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/User.hi",
          "create-daml-app-0.1.0-29b501bcf541a40e9f75750246874e0a35de72e00616372da435e4b69966db5d/User.hie",
          "META-INF/MANIFEST.MF"
      ]
  }

Note that ``name`` and ``version`` will be ``null`` for packages in DAML-LF < 1.8.

Importing DAML packages
***********************

There are two ways to import a DAML package in a project: via ``dependencies``, and via ``data-dependencies``. They each have certain advantages and disadvantages. To summarize:

* ``dependencies`` allow you to import a DAML archive as a library. The definitions in the dependency will all be made available to the importing project. However, the dependency must be compiled with the same DAML SDK version, so this method is only suitable for breaking up large projects into smaller projects that depend on each other, or to reuse existing libraries.

* ``data-dependencies`` allow you to import a DAML archive (.dar) or a DAML-LF package (.dalf), including packages that have already been deployed to a ledger. These packages can be compiled with any previous SDK version. On the other hand, not all definitions can be carried over perfectly, since the DAML interface needs to be reconstructed from the binary.

The following sections will cover these two approaches in more depth.

Importing a DAML package via dependencies
=========================================

A DAML project can declare a DAML archive as a dependency in the ``dependencies`` field of ``daml.yaml``. This lets you import modules and reuse definitions from another DAML project. The main limitation of this method is that the dependency must be for the same SDK version as the importing project.

Let's go through an example. Suppose you have an existing DAML project ``foo``, located at ``/home/user/foo``, and you want to use it as a dependency in a project ``bar``, located at ``/home/user/bar``.

To do so, you first need to generate the DAML archive of ``foo``. Go into ``/home/user/foo`` and run ``daml build -o foo.dar``. This will create the DAML archive, ``/home/user/foo/foo.dar``.

.. TODO (#4925): Make the above step redundant by letting users declare projects directly. Then update this doc.

Next, we will update the project config for ``bar`` to use the generated DAML archive as a depndency. Go into ``/home/user/bar`` and change the ``dependencies`` field in ``daml.yaml`` to point to the created `DAML archive`:

.. code-block:: yaml

  dependencies:
    - daml-prim
    - daml-stdlib
    - ../foo/foo.dar

The import path can also be absolute, for example, by changing the last line to:

.. code-block:: yaml

    - /home/user/foo/foo.dar

When you run ``daml build`` in ``bar`` project, the compiler will make the definitions in ``foo.dar`` available for importing. For example, if ``foo`` exports the module ``Foo``, you can import it in the usual way:

.. code-block:: daml

  import Foo

By default, all modules of ``foo`` are made available when importing ``foo`` as a dependency. To limit which modules of ``foo`` get exported, you may add an ``exposed-modules`` field in the ``daml.yaml`` file for ``foo``:

.. code-block:: yaml

  exposed-modules:
  - Foo

Importing a DAML archive via data-dependencies
==============================================

You can import a DAML archive (.dar) or DAML-LF package (.dalf) using ``data-dependencies``. Unlike ``dependencies``, this can be used when the DAML SDK versions do not match.

For example, you can import ``foo.dar`` as follows:

.. code-block:: yaml

  dependencies:
  - daml-prim
  - daml-stdlib
  data-dependencies:
  - ../foo/foo.dar

When importing packages this way, the DAML compiler will try to reconstruct the original DAML interface from the compiled binaries. However, to allow ``data-dependencies`` to work across SDK versions, the compiler has to abstract over some details which are not compatible across SDK versions. This means that there are some DAML features that cannot be recovered when using ``data-dependencies``. In particular:

#. Export lists cannot be recovered, so imports via ``data-dependencies`` can access definitions that were originally hidden. This means it is up to the importing module to respect the data abstraction of the original module. Note that this is the same for all code that runs on the ledger, since the ledger does not provide special support for data abstraction.

#. If you have a ``dependency`` that limits the modules that can be accessed via ``exposed-modules``, you can get an error if you also have a ``data-dependency`` that references something from the hidden modules (even if it is only reexported). Since ``exposed-modules`` are not available on the ledger in general, we recommend to not make use of them and instead rely on naming conventions (e.g., suffix module names with ``.Internal``) to make it clear which modules are part of the public API.

#. Prior to DAML-LF version 1.8, typeclasses could not be reconstructed. This means if you have a package that is compiled with an older version of DAML-LF, typeclasses and typeclass instances will not be carried over via data-dependencies, and you won't be able to call functions that rely on typeclass instances. This includes the template functions, such as ``create``, ``signatory``, and ``exercise``, as these rely on typeclass instances.

#. Starting from DAML-LF version 1.8, when possible, typeclass instances will be reconstructed by re-using the typeclass definitions from dependencies, such as the typeclasses exported in ``daml-stdlib``. However, if the typeclass signature has changed, you will get an instance for a reconstructed typeclass instead, which will not interoperate with code from dependencies. Furthermore, if the typeclass definition uses the ``FunctionalDependencies`` language extension, this may cause additional problems, since the functional dependencies cannot be recovered. So this is something to keep in mind when redefining typeclasses and when using ``FunctionalDependencies``.

#. Certain advanced type system features cannot be reconstructed. In particular, ``DA.Generics`` and ``DeriveGeneric`` cannot be reconstructed. This may result in certain definitions being unavailable when importing a module that uses these advanced features.

.. TODO (#4932): Add warnings for advanced features that aren't supported, and add a comment on item #4.

Because of their flexibility, data-dependencies are a tool that is recommended for performing DAML model upgrades. See the :ref:`upgrade documentation <upgrade-overview>` for more details.

.. _module_collisions:

Handling module name collisions
*******************************

Sometimes you will have multiple packages with the same module name. In that case, a simple import will fail, since the compiler doesn't know which version of the module to load. Fortunately, there are a few tools you can use to approach this problem.

The first is to use package qualified imports. Supposing you have packages with different names, ``foo`` and ``bar``, which both expose a module ``X``. You can select which one you want with a package qualified import.

To get ``X`` from ``foo``:

.. code-block:: daml

  import "foo" X

To get ``X`` from ``bar``:

.. code-block:: daml

  import "bar" X

To get both, you need to rename the module as you perform the import:

.. code-block:: daml

  import "foo" X as FooX
  import "bar" X as BarX

Sometimes, package qualified imports will not help, because you are importing two packages with the same name. For example, if you're loading different versions of the same package. To handle this case, you need the ``--package`` build option.

Suppose you are importing packages ``foo-1.0.0`` and ``foo-2.0.0``. Notice they have the same name ``foo`` but different versions. To get modules that are exposed in both packages, you will need to provide module aliases. You can do this by passing the ``--package`` build option. Open ``daml.yaml`` and add the following ``build-options``:

.. code-block:: yaml

  build-options:
  - '--package'
  - 'foo-1.0.0 with (X as Foo1.X)'
  - '--package'
  - 'foo-2.0.0 with (X as Foo2.X)'

This will alias the ``X`` in ``foo-1.0.0`` as ``Foo1.X``, and alias the ``X`` in ``foo-2.0.0`` as ``Foo2.X``. Now you will be able to import both ``X`` by using the new names:

.. code-block:: daml

  import qualified Foo1.X
  import qualified Foo2.X

It is also possible to add a prefix to all modules in a package using
the ``module-prefixes`` field in your ``daml.yaml``. This is
partiuclarly useful for upgrades where you can map all modules of
version ``v`` of your package under ``V$v``. For the example above you
can use the following:

.. code-block:: yaml

  module-prefixes:
    foo-1.0.0: Foo1
    foo-2.0.0: Foo2

That will allow you to import module ``X`` from package ``foo-1.0.0``
as ``Foo1.X`` and ``X`` from package ``-foo-2.0.0`` as ``Foo2``.

You can also use more complex module prefixes, e.g., ``foo-1.0.0:
Foo1.Bar`` which will make module ``X`` available under
``Foo1.Bar.X``.
