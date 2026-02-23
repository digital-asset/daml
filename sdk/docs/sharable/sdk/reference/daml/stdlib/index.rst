.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _stdlib-reference-base:

The Standard Library
====================

The Daml standard library is a collection of Daml modules that are bundled with the SDK, and can be used to implement Daml applications.

The :ref:`Prelude <module-prelude-72703>` module is imported automatically in every Daml module. Other modules must be imported manually, just like your own project's modules. For example:

.. code-block:: daml

   import DA.Optional
   import DA.Time

Here is a complete list of modules in the standard library:

* :doc:`Prelude <Prelude>`
* :doc:`DA.Action <DA-Action>`
* :doc:`DA.Action.State <DA-Action-State>`
* :doc:`DA.Action.State.Class <DA-Action-State-Class>`
* :doc:`DA.Assert <DA-Assert>`
* :doc:`DA.Bifunctor <DA-Bifunctor>`
* :doc:`DA.ContractKeys <DA-ContractKeys>`
* :doc:`DA.Crypto.Text <DA-Crypto-Text>`
* :doc:`DA.Date <DA-Date>`
* :doc:`DA.Either <DA-Either>`
* :doc:`DA.Exception <DA-Exception>`
* :doc:`DA.Fail <DA-Fail>`
* :doc:`DA.Foldable <DA-Foldable>`
* :doc:`DA.Functor <DA-Functor>`
* :doc:`DA.Internal.Interface.AnyView <DA-Internal-Interface-AnyView>`
* :doc:`DA.Internal.Interface.AnyView.Types <DA-Internal-Interface-AnyView-Types>`
* :doc:`DA.List <DA-List>`
* :doc:`DA.List.BuiltinOrder <DA-List-BuiltinOrder>`
* :doc:`DA.List.Total <DA-List-Total>`
* :doc:`DA.Logic <DA-Logic>`
* :doc:`DA.Map <DA-Map>`
* :doc:`DA.Math <DA-Math>`
* :doc:`DA.Monoid <DA-Monoid>`
* :doc:`DA.NonEmpty <DA-NonEmpty>`
* :doc:`DA.NonEmpty.Types <DA-NonEmpty-Types>`
* :doc:`DA.Numeric <DA-Numeric>`
* :doc:`DA.Optional <DA-Optional>`
* :doc:`DA.Record <DA-Record>`
* :doc:`DA.Semigroup <DA-Semigroup>`
* :doc:`DA.Set <DA-Set>`
* :doc:`DA.Stack <DA-Stack>`
* :doc:`DA.Text <DA-Text>`
* :doc:`DA.TextMap <DA-TextMap>`
* :doc:`DA.Time <DA-Time>`
* :doc:`DA.Traversable <DA-Traversable>`
* :doc:`DA.Tuple <DA-Tuple>`
* :doc:`DA.Validation <DA-Validation>`
* :doc:`GHC.Show.Text <GHC-Show-Text>`
* :doc:`GHC.Tuple.Check <GHC-Tuple-Check>`

