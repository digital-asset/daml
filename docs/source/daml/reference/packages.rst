.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


Reference: DAML packages
########################

This page gives reference information on DAML package dependencies:

.. contents:: :local:

DAML archives
*************

When a DAML project is compiled, the compiler produces a `DAML archive`. These are platform-independent packages of compiled DAML code that can be uploaded to a DAML ledger or imported in other DAML projects.

DAML archives have a ``.dar`` file ending. By default, when you run ``daml build``, it will generate the ``.dar`` file in the ``.daml/dist`` folder in the project root folder. For example, running ``daml build`` in project ``foo`` with project version ``0.0.1`` will result in a DAML archive ``.daml/dist/foo-0.0.1.dar``.

You can specify a different path for the DAML archive by using the ``-o`` flag:

.. code-block:: sh

  daml build -o foo.dar

For details on how to upload a DAML archive to the ledger, see the :ref:`deploy documentation <deploy-ref_overview>`. The rest of this page will focus on how to import a DAML archive as a dependncy in other DAML projects.

Importing a DAML archive via dependencies
*****************************************

A DAML project can declare a DAML archive as a dependency in the ``dependencies` field of ``daml.yaml``. This lets you import modules and reuse definitions from another DAML project.

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

  exposed-modules:
  - Foo

**Important Limitation:** For DAML archive imports via ``dependencies`` to work, the archive should be compiled with the same DAML SDK version. Otherwise, compilation will fail. If matching the SDK version is not possible, see the next section.

Importing a DAML archive via data-dependencies
**********************************************

A secondary method for importing a DAML archive, which can be used when the DAML SDK versions do not match, is to import a DAML archive via the ``data-dependencies`` field in ``daml.yaml``:

.. code-block:: yaml

  dependencies:
  - daml-prim
  - daml-stdlib
  data-dependencies:
  - ../foo/foo.dar

You can also import a ``.dalf`` file via data-dependencies.

When importing packages this way, the DAML compiler will attempt to reconstruct the DAML interface from the compiled DAML-LF binaries included in the DAML archive.

To allow `data-dependencies` to work across SDK versions, they have to abstract over some details which are not compatible across SDK versions. This means that there are some DAML features that cannot be recovered when using `data-dependencies`.

The first disadvantage is that the reconstruction process used for data-dependencies is slower than the direct import process used for dependencies, so it will negatively affect the speed of compilation.

The second disadvantage, which has far-reaching consequences, is that not everything can be perfectly reconstructed via data-dependencies. In particular:

#. Export lists cannot be reconstructed, so imports via data-dependencies can access definitions that were originally hidden. This means it is up to the importing module to respect the data encapsulation of the original module. On the positive side, the encapsulation can also be ignored on purpose, to facilitate upgrades of DAML models to newer SDK versions.

#. Certain advanced type system features also cannot be reconstructed, as they are erased in the process of compiling DAML LF binaries. This includes the ``DataKinds``, ``DeriveGeneric``, and ``FunctionalDependencies`` extensions from GHC. This may result in some definitions being unavailable when importing a module that uses these advanced features.

#. Prior to DAML LF version 1.8, typeclasses could not be reconstructed from DAML archives. This means if you have an archive that is compiled with an older version of DAML LF, typeclasses and typeclass instances will not be carried over via data-dependencies, and you will not be able to call functions that rely on typeclass instances.

#. When possible, typeclass instances will be reconstructed using the typeclass definitions from dependencies (such as the typeclass definitions from ``daml-stdlib``). But if the typeclass methods or signature has changed, you will get an instance for a reconstructed typeclass instead, which will not interoperate with code from dependencies. So this is something to keep in mind when typeclass definitions have changed.

.. TODO (#4932): Add warnings for advanced features that aren't supported, and add a comment on bullet #2.

Given this long list of disadvantages, data-dependencies are a tool that is only recommended when dependencies cannot be used. In particular, data-dependencies should only be used to interface with deployed code on a ledger, such as to interact with a deployed DAML model or to upgrade of a deployed DAML model. See the :ref:`upgrade documentation <upgrade-overview>` for more details on the latter.

Handling module name collisions
*******************************

Sometimes you will have multiple packages with the same module name. In that case, a simple import will fail, since the compiler doesn't know which package to load. Fortunately, there are a few tools you can use to approach this problem.

The first is to use package qualified imports. Supposing you have packages with different names, ``foo`` and ``bar``, which both expose a module ``X``. You can select which on you want with package qualified imports.

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

Now you will be able to import both ``X`` by using the new names:

.. code-block:: daml

  import qualified Foo1.X
  import qualified Foo2.X
