.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _da-model-daml:

Daml: Define Contract Models Compactly
######################################

As described in preceding sections, both the integrity and privacy notions depend on
a contract model, and such a model must specify:

#. a set of allowed actions on the contracts, and
#. the signatories, contract observers, and
#. an optional agreement text associated with each contract, and
#. the optional key associated with each contract and its maintainers.

The sets of allowed actions can in general be infinite. For instance,
the actions in the IOU contract model considered earlier can be instantiated for an
arbitrary obligor and an arbitrary owner. As enumerating all
possible actions from an infinite set is infeasible, a more
compact way of representing models is needed.

Daml provides exactly that: a compact representation of a contract model.
Intuitively, the allowed actions are:

#. **Create** actions on all instances of templates such that
   the template arguments satisfy the `ensure` clause of the
   template

#. **Exercise** actions on a contract corresponding to
   choices on that template, with given
   choice arguments, such that:

   #. The actors match the controllers of the choice.
      That is, the controllers define the :ref:`required authorizers <da-ledgers-required-authorizers>` of the choice.
   #. The choice observers match the observers annotated in the choice.
   #. The exercise kind matches.
   #. All assertions in the update block hold for the given choice arguments.
   #. Create, exercise, fetch and key statements in the update block are represented
      as create, exercise and fetch actions and key assertions in the consequences of the exercise
      action.

#. **Fetch** actions on a contract corresponding to
   a *fetch* of that instance inside of an update block.
   The actors must be a non-empty subset of the contract stakeholders.
   The actors are determined dynamically as follows: if the fetch appears in an update block of a choice
   `ch` on a contract `c1`, and the fetched contract ID resolves to a contract `c2`, then the actors are defined as the
   intersection of (1) the signatories of `c1` union the controllers of `ch` with (2) the stakeholders of `c2`.

   A :ref:`fetchbykey` statement also produces a **Fetch** action with the actors determined in the same way.
   A :ref:`lookupbykey` statement that finds a contract also translates into a **Fetch** action, but all maintainers of the key are the actors.

#. **NoSuchKey** assertions corresponding to a :ref:`lookupByKey` update statement for the given key that does not find a contract.

An instance of a Daml template, that is, a **Daml contract**,
is a triple of:

#. a contract identifier
#. the template identifier
#. the template arguments

The signatories of a Daml contract are derived from the template arguments and the explicit signatory annotations on the contract template.
The contract observers are also derived from the template arguments and include:

1. the observers as explicitly annotated on the template
2. all controllers `c` of every choice defined using the syntax :code:`controller c can...` (as opposed to the syntax :code:`choice ... controller c`)

For example, the following template exactly describes the contract model
of a simple IOU with a unit amount, shown earlier.

.. literalinclude:: ./daml/SimpleIou.daml
   :language: daml
   :start-after: SNIPPET-START
   :end-before: SNIPPET-END

.. _da-daml-model-controller-observer:

In this example, the owner is specified as an observer, since it must be able to see the contract to exercise the :code:`Transfer` and :code:`Settle` choices on it.

The template identifiers of contracts are created
through a content-addressing scheme. This means every contract is
self-describing in a sense: it constrains its stakeholder annotations
and all Daml-conformant actions on itself. As a consequence, one can
talk about "the" Daml contract model, as a single contract model encoding all possible
instances of all possible templates. This model is subaction-closed;
all exercise and create actions done within an update block are also
always permissible as top-level actions.
