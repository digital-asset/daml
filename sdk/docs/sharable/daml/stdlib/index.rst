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

.. toctree::
   :maxdepth: 3
   :titlesonly:

   Prelude <Prelude>
   DA.Action <DA-Action>
   DA.Action.State <DA-Action-State>
   DA.Action.State.Class <DA-Action-State-Class>
   DA.Assert <DA-Assert>
   DA.Bifunctor <DA-Bifunctor>
   DA.Crypto.Text <DA-Crypto-Text>
   DA.Date <DA-Date>
   DA.Either <DA-Either>
   DA.Exception <DA-Exception>
   DA.Foldable <DA-Foldable>
   DA.Functor <DA-Functor>
   DA.Internal.Interface.AnyView <DA-Internal-Interface-AnyView>
   DA.Internal.Interface.AnyView.Types <DA-Internal-Interface-AnyView-Types>
   DA.List <DA-List>
   DA.List.BuiltinOrder <DA-List-BuiltinOrder>
   DA.List.Total <DA-List-Total>
   DA.Logic <DA-Logic>
   DA.Map <DA-Map>
   DA.Math <DA-Math>
   DA.Monoid <DA-Monoid>
   DA.NonEmpty <DA-NonEmpty>
   DA.NonEmpty.Types <DA-NonEmpty-Types>
   DA.Numeric <DA-Numeric>
   DA.Optional <DA-Optional>
   DA.Record <DA-Record>
   DA.Semigroup <DA-Semigroup>
   DA.Set <DA-Set>
   DA.Stack <DA-Stack>
   DA.Text <DA-Text>
   DA.Time <DA-Time>
   DA.Traversable <DA-Traversable>
   DA.Tuple <DA-Tuple>
   DA.Validation <DA-Validation>
   GHC.Show.Text <GHC-Show-Text>
   GHC.Tuple.Check <GHC-Tuple-Check>

