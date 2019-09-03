.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


Generic Templates
#################

Sometimes different DAML templates have a common structure. Typically this occurs when there is some logic (usually in choices) that can be applied to many different underlying contracts, such as assets. Generic templates allow you to abstract template code from some of the data that would occur in the contracts. Let's see a simple example to understand what this means.

Example: Generic Proposal
*************************

Suppose we want to model a propose and accept workflow. This means that a party can propose a contract to a specific party, who may accept it with the terms of that contract. We see this pattern occur frequently for different types of contracts. Of course, we could implement one proposal template for every underlying template. However this is tedious and error prone. Alternatively, we can write it once and for all using a generic template.

This is how a generic Proposal template looks in DAML.

.. literalinclude:: ../code-snippets/Proposal.daml
   :language: daml
   :start-after: -- start proposal body
   :end-before: -- end proposal body

There are several things to notice in this short template.

Firstly, where there is usually just a template name, there is now a *template header* ``Template t => Proposal t``. Here ``Proposal`` takes a single type parameter ``t`` representing the type of the underlying asset. The ``Template`` constraint says that ``t`` is not just any type but a contract template with signatories, choices, etc.

Secondly, the ``asset`` parameter to the template has the abstract type ``t``. We don't know anything about ``asset`` other than the fact that it has a template type. However this is all we need to implement the proposal template.

Notice that the signatories of the proposal are obtained from the signatories of the underlying contract. This is done by calling the overloaded ``signatory`` method. The observers on the other hand are independent of the underlying asset.

Finally let's look at the ``Accept`` choice which characterizes the propose and accept workflow. The receiver can ``Accept`` which results in a contract of the underlying asset type being created. We are able to call ``create`` on the asset since we know it satisfies the ``Template`` constraint.

Template Instances
******************

The above template soundly represents the proposal workflow, but we have not yet used it on a concrete (non-generic) template. We call the concrete instantiation a *template instance*.

Let's introduce a very simple ``Coin`` contract that we can use in the proposal.

.. literalinclude:: ../code-snippets/Coin.daml
   :language: daml
   :start-after: -- start coin
   :end-before: -- end coin

We would like to model an issuing party (e.g. a bank) proposing a coin contract for an individual to accept. To do this, we need to explicitly state our intention to use ``Coin`` in a ``Proposal``. We do this using the ``template instance`` syntax.

.. literalinclude:: ../code-snippets/Coin.daml
   :language: daml
   :start-after: -- start instance
   :end-before: -- end instance

Note that we must choose a name, here ``CoinProposal``, for creating contracts of this template in some client languages (for example when using the Java ledger bindings).

With a template instance in place, we can create and exercise choices on contracts of this type.

.. literalinclude:: ../code-snippets/Coin.daml
   :language: daml
   :start-after: -- start scenario
   :end-before: -- end scenario

Note that we use two different data types, ``Coin`` and ``Proposal``, to construct the underlying asset and the proposal data. The bank is able to create the coin proposal which Alice can then accept. This scenario results in two contract creations: first the proposal contract, which is consumed to give rise to the coin contract.
