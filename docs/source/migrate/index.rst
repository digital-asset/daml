.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Migrating DAML ledgers
######################

Building migration projects
---------------------------

When we want to replace a DAML package ``foo-1.0.0`` that is deployed on a DAML ledger with an
updated version, say ``foo-2.0.0``, we need to migrate all the existing contracts that are active
and whose templates are defined in either ``foo-1.0.0`` or whose template definition depends directly
or indirectly on data defined in ``foo-1.0.0``. To help a DAML deployment with this task, the DAML
assistant offers the command ``daml migrate``.

The ``migrate`` command takes as inputs the package ``foo-1.0.0`` and its new version ``foo-2.0.0``
and creates a new project containing generated code to migrate contracts from ``foo-1.0.0`` to
``foo-2.0.0``. Run ``daml migrate --help`` to see its full usage description:

.. code-block:: shell

  Usage: daml migrate TARGET_PATH SOURCE FROM_PATH TO_PATH

  Available options:
    TARGET_PATH              Path where the new project should be located
    SOURCE                   Path to the main source file ('source' entry of the
                             project configuration files of the input projects).
    FROM_PATH                Path to the dar-package from which to migrate from
    TO_PATH                  Path to the dar-package to which to migrate to
    -h,--help                Show this help text

For example, to create a migration project from ``foo-1.0.0`` to ``foo-2.0.0`` run

.. code-block:: shell

  daml migrate foo-upgrade-2.0.0 daml/Foo.daml foo-1.0.0/.daml/dist/foo-1.0.0.dar foo-2.0.0/.daml/dist/foo-2.0.0.dar

This generates a migration project in the directory ``foo-upgrade-2.0.0``. To build it, change
directory to ``foo-upgrade-2.0.0`` and run

.. code-block:: shell

  ./build.sh

respectively ``.\build.cmd`` if your on a Windows system.  Note that you can **not** use the usual
``daml build`` command to build the migration project.

How migrations work and when it is necessary to write code manually
-------------------------------------------------------------------

It is important to understand that the ``daml migrate`` command will not always succeed in
generating a migration project that will compile. To understand why, let's assume that the
``foo-1.0.0`` consists of a single module:

.. code-block:: shell

  foo-1.0.0
  ├── daml
  │   └── Foo.daml
  ├── daml.yaml
  └── ui-backend.conf

where the ``Foo.daml`` file contains

.. code-block:: daml

  daml 1.2
  module Foo where

  template Foo
    with
      a : Int
      p : Party
    where
      signatory p

The package ``foo-2.0.0`` contains exactly the same modules, but a new template ``Bar`` has been
added to the ``Foo`` module.

.. code-block:: daml

  daml 1.2
  module Foo where

  template Foo
    with
      a : Int
      p : Party
    where
      signatory p

  template Bar
    with
      t : Text
      p : Party
    where
      signatory p


If we generate a migration project with ``daml migrate`` as above, the directory contents of the
``foo-2.0.0-upgrade/daml`` directory is

.. code-block:: shell

  daml
  ├── FooAInstances.daml
  ├── FooBInstances.daml
  └── Foo.daml

For every template that was defined in the module ``Foo`` in the ``foo-1.0.0`` package, you will
find two new templates. One to upgrade contract instances of this template to the changed template
defined in the module ``Foo`` in the package ``foo-2.0.0`` and one to rollback the process. Here is
an example:

.. code-block:: daml

   template FooUpgrade
    with
        op : Party
    where
        signatory op
        nonconsuming choice Upgrade: ContractId B.Foo
            with
                inC : ContractId A.Foo
                sigs : [Party]
            controller sigs
                do
                    d <- fetch inC
                    assert $ fromList sigs == fromList (signatory d)
                    create $ conv d

   template FooRollback
    with
        op : Party
    where
        signatory op
        nonconsuming choice Rollback: ContractId A.Foo
            with
                inC : ContractId B.Foo
                sigs : [Party]
            controller sigs
                do
                    d <- fetch inC
                    assert $ fromList sigs == fromList (signatory d)
                    create $ conv d

Currently, we only support one upgrade/rollback contract template pair. In the future we might
extend the ``migrate`` command with different possible upgrade/rollback templates. The above
contract template offers a choice to input a contract instance defined in ``foo-1.0.0``, convert it
with the ``conv`` function and create a new one that follows the data type definition in
``foo-2.0.0``. The heart of the migration code is the ``conv`` function. It is defined in the DAML
standard library in the module ``DA.Upgrade`` and has the following type signature:

.. code-block:: daml

  conv : (Generic a repA, Generic b repB, Conv repA repB) => a -> b

From its type signature you can see that it can convert any two data types that are instances of the
``Generic`` class and whose generic representation can be converted itself.

The ``migrate`` command takes care of defining generic instances for all relevant data types in the
two packages. In our example, you'll find them in the files ``FooAInstances.daml`` and
``FooBInstances.daml``.

Generic representations can be converted when they are ``isomorphic``. That means the corresponding
data types defined in package ``foo-1.0.0`` and ``foo-2.0.0`` have exactly the same shape. For
example the following two data types are isomorphic:

.. code-block:: daml

  data Either a b = Left a | Right b
  data UpDown a b = Up a | Down b

while the following data types are not

.. code-block:: daml

  data Either a b = Left a | Right b
  data Maybe a = Just a | Nothing

When the package ``foo-2.0.0`` contains an extended data type of the ``Foo`` template that is not
isomorphic, the build will fail. For example, let's assume the ``Foo`` module in the ``foo-2.0.0``
package has been extended to

.. code-block:: daml

  daml 1.2
  module Foo where

  template Foo
    with
      a : Int
      b : Text
      p : Party
    where
      signatory p


Here is typical error message in this case:

.. code-block:: shell


  daml/Foo.daml:22:30: error:
  • Could not deduce (DA.Upgrade.Conv
  (DA.Generics.M1
  DA.Generics.S
  ('DA.Generics.MetaSel
  ('DA.Generics.MetaSel0
  ('Some "p")
  'DA.Generics.NoSourceUnpackedness
  'DA.Generics.NoSourceStrictness))
  (DA.Generics.Rec0 Party))
  (DA.Generics.S1
  ('DA.Generics.MetaSel
  ...

The important hint is that the compiler is not able to deduce that our data type is an instance of
the ``DA.Upgrade.Conv`` class and hence not convertible. In this case you will have to add your own
upgrade/rollback templates to ``daml/Foo.daml``, that describe how to convert a contract of the
template in question of package ``foo-1.0.0`` to one of ``foo-2.0.0`` and vice versa. For example

.. code-block:: daml

   template FooUpgrade
    with
        op : Party
    where
        signatory op
        nonconsuming choice Upgrade: ContractId B.Foo
            with
                inC : ContractId A.Foo
                sigs : [Party]
            controller sigs
                do
                    d <- fetch inC
                    assert $ fromList sigs == fromList (signatory d)
                    create $ Foo with a = d.a; b = "updated"; p = d.p

   template FooRollback
    with
        op : Party
    where
        signatory op
        nonconsuming choice Rollback: ContractId A.Foo
            with
                inC : ContractId B.Foo
                sigs : [Party]
            controller sigs
                do
                    d <- fetch inC
                    assert $ fromList sigs == fromList (signatory d)
                    create $ Foo with a = d.a; p = d.p

In a future iteration, we will also be able to automatically convert data types for which merely
embeddings/projections exists instead of full isomorphisms and the above example would compile
without further manual help.

Deploying the migration
-----------------------

Once you've succeeded building the ``foo-upgrade-2.0.0`` package you can deploy it on the ledger
together with the ``foo-2.0.0`` package. Optionally you can bundle it with the ``foo-2.0.0`` package
into a single DAML archive by running

.. code-block:: shell

  daml damlc merge-dars foo-2.0.0/.daml/dist/foo-2.0.0.dar foo-upgrade-2.0.0/.daml/dist/foo-upgrade-2.0.0.dar --package-name foo-2.0.0-with-upgrades.dar

You find more information on how to deploy DAML archive packages :ref:`here <deploy-ref_index>` .
After the ``foo-upgrade-2.0.0`` package has been deployed on the ledger, there exists for every
contract defined in ``foo-1.0.0`` a DAML workflow with a choice to upgrade it to ``foo-2.0.0`` or
roll it back. A future iteration might add DAML triggers to carry out the upgrade process
automatically.
