.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

===================================================
 Contract Keys, Disclosure and Contract Quantities
===================================================

*Maintainers*: Silvan Villiger, Simon Meier, Ognjen Maric

*Last Updated*: 2018-02-22

*Status*: Draft

*Intended audience*: DA Product and Engineering


.. _daml-contract-keys-introduction:

Introduction
============

The purpose of this document is to discuss the topics Contract Keys, Disclosure
and Contract Quantities. In particular, it lists the requirements, analyses
potential solutions and proposes an implementation plan with phased delivery
milestones.

Questions and Comments
----------------------

Feel free to ask questions, make comments, add action items by adding them
directly under the relevant paragraph in the following style:

* Q (Alice): Can I transfer coins to you?

  * Q (Bob): Always!

* TODO (Charlie): Ask Alice for coins.

Here is a list of globally open questions, comments and action items:

* Empty

.. _Goals:

Goals
=====

This section provides a comprehensive and prioritised list of goals. They should
be understood as a whish list rather than fixed requirements as we're optimizing
for the best cost-benefit ratio. In particular, we aim for keeping the cost of
the chosen solution low by considering its implementation and subsequent
maintenance effort.


.. _`Canonical Example`:

Canonical Example
-----------------

We start with providing a benchmark example of an online-shop that will
subsequently be mofified and extended to illustrate the various goals and
solutions. The example models a broker that offers an item at a certain price to
a client. Further, it tracks the number of items the broker has in stock. The
client can purchase as many items as he likes on a first-come first-served basis
as long as the broker has enough items in his inventory.

.. ExcludeFromDamlParsing
.. code-block:: daml

  template Stock
    with
      broker: Party
      client: Party
      item: Text
      quantity: Integer
    where
      ensure quantity >= 0
      signatory broker
      observer client

      controller broker can
        Dispatch with delta: Integer
          returning ContractId Stock
          to create this with quantity = quantity - delta

  template Offer
    with
      broker: Party
      client: Party
      item: Text
      price: Integer
    where
      signatory broker

      controller client can
        anytime Purchase with quantity: Integer
          returning ContractId OfferHandler
          to create OfferHandler with broker; client; item; price; quantity

  template OfferHandler
    with
      broker: Party
      client: Party
      item: Text
      price: Integer
      quantity: Integer
    where
      controller client can
        Process with stockCid: ContractId Stock
          returning ContractId Deal
          to do
            broker does exercise stockCid Dispatch with delta = quantity
            create Deal with broker; client; item; price; quantity

  template Deal
    with
      broker: Party
      client: Party
      item: Text
      price: Integer
      quantity: Integer
    where
      signatory broker
      signatory client

      agreement toText client <> " bought " <> toText quantity <> " of "
                item <> " from " <> toText broker <> " at " <> toText price

  test it =
    scenario
      let broker = 'Krusty'
      let client = 'Homer'
      let item = "Burger"
      stockCid <- broker commits create Stock with broker; client; item
                                                   quantity = 10
      offerCid <- broker commits create Offer with broker; client; item
                                                   price = 5

      offerHandlerCid <-
        client commits exercise offerCid Purchase with quantity=2

      dealCid <- client commits exercise offerHandlerCid Process with stockCid

      c <- client commits fetch dealCid
      assert (c == Deal with broker; client; item; price=5; quantity=2)

We could alternatively add the ``stockCid`` as an argument to the ``purchase``
choice. However, this is not practical for a genuine choice that tends to get
exercised manually as it would require logic to find the current contract-id in
all UIs and tools that use the choice. Instead, we usually go for a separate
``*Handler`` contract and a nanobot that performs the lookup and automatically
exercises the process choice. Note that this leads to quite a bit of boilerplate
code and is one of the motivations for Contract Keys.

.. _`Deterministic Contract Keys`:

Deterministic Contract Keys
---------------------------

*Priority*: HIGH

It's common to have contracts that reference other contracts. The offer contract
above, for example, references a stock of the broker. We could store the
stocks's contract-id as a field in the offer contract but that would result in a
stale contract-id whenever the stock's quantity changes. We can avoid this by
referencing the stock with a natural key, such as the combination of the
broker's party literal and the item's name, and by providing the current
contract-id to the purchase choice or a corresponding ``*Handler`` contract as
described in the `Canonical Example`_. However, the boilerplate code resulting
from this could be avoided if we had a way to reference contracts directly with
a deterministic key.

.. _`Natural Contract Keys`:

Natural Contract Keys
---------------------

*Priority*: MEDIUM

One candidate for (reasonably) `Deterministic Contract Keys`_ are natural keys.
They are well-established in the context of traditional data modeling and are
therefore likely to be useful in a distributed setting as well.

An example for a natural key in the case of the stock contract above is the
combination of the broker's party literal and the item's name:

.. ExcludeFromDamlParsing
.. code-block:: daml

  template Stock
    with
      broker: Party
      client: Party
      item: Text
      quantity: Integer
    where
      key broker; item
      ensure quantity >= 0
      signatory broker
      observer client

      controller broker can
        Dispatch with delta: Integer
          returning Stock.Key
          to create this with quantity = quantity - delta

  template Offer
    with
      stock: Stock.Key
      client: Party
      price: Integer
    where
      signatory stock.broker

      controller client can
        anytime Purchase with quantity: Integer
          returning Deal.Key
          to do
            broker does exercise stock Dispatch with delta = quantity
            create Deal with stock; client; price; quantity


The record of ``Stock`` contains the union of both key and value fields. The key fields can be
extract from a record instance using the ``key: c -> c.Key`` builtin. Upon contract creation full record is
provided but choices can be exercised by providing only the key. The type of the key record can be referred to via
``Stock.Key``.

Note in this context that we currently tend to return the contract-ids of all
created contracts such that the caller can directly act on them. This can become
quite verbose when a choice has many side-effects. With the possibility to
exercise choices on natural keys, it would become optional to return those
contract-ids.

.. _`Surrogate Contract Keys`:

Surrogate Contract Keys
-----------------------

*Priority*: HIGH

Another well-established and hence potentially useful candidate for
`Deterministic Contract Keys`_ are surrogate keys. The idea is to
automatically assign a key to a contract, and give the option reuse that key
when updating a contract via a consuming choice. In the following example this
is accomplished via hyptothetical builtin ``update``:

.. ExcludeFromDamlParsing
.. code-block:: daml

  template Item
    with
      description: Text
      importer: Party
      manufacturer: Party
      refNo: Text
      manual: Text
    where
      controller import can
        ChangeManufacturer with manufacturer; refNo
          returning Item.Key
          to update this with manufacturer; refNo

      controller manfacturer can
        ChangeManual with manual
          returning Item.Key
          to update this with manual

  template Stock
    with
      broker: Party
      client: Party
      item: Item.Key
      quantity: Integer
    where
      ensure quantity >= 0
      signatory broker
      observer client

      controller broker can
        Dispatch with delta: Integer
          returning Stock.Key
          to update this with quantity = quantity - delta


The ``update`` function is similar to ``create`` with the exception that it
does not update or allow updating the key. Hence it can only be used in a
context where the contract it is updating is being archived - ie. inside the
same contract in a consuming choice. In order to use it from other
contracts, it would need to be wrapped in a choice that describes the external
controllers and potentially specifies further rules. The ``update`` function
could be used for `Natural Contract Keys`_ as well.

Note that the contract key would be generated with the original ``create``. In
fact, the contract-id of the first revision could serve as contract key.

* Q (Simon): what's they type of ``update``? I'm slightly wary of the
  very special circumstances in which it can be called. Why not expose
  ``freshId :: Update ContractId`` and use it as a `Natural Contract Keys`_?

* A (Sofus): It's intended to be symmetric with ``create`` having same
  signature. But it's constrained to only work when immediately followed by
  ``this`` -- ie. only works where ``this`` is defined. I imagine ``update
  this with``
  being syntactic sugar for ``create this with`` where hidden field ``key`` is
  passed along too. I think the question is whether you should be allowed to
  create a constract with same ``Key`` as an archived one, without using
  ``update this`` ? Ie. can you raise contracts from the grave?


Nested templates
~~~~~~~~~~~~~~~~

A common use case for keys in DAML is to express a parent/child relationship,
as seen between ``Stock`` and ``Offer``. DAML syntax could be extended to
express this relationship so that ``Offer`` is a nested template of
``Stock`` with access to all fields of its' parent. The canonical example
would look as follows:

.. ExcludeFromDamlParsing
.. code-block:: daml

  template Stock
    with
      broker: Party
      client: Party
      item: Text
      quantity: Integer
    where
      ensure quantity >= 0
      signatory broker
      observer client

      controller broker can
        Dispatch with delta: Integer
          returning Stock.Key
          to update this with quantity = quantity - delta

    template Offer
      with
        client: Party
        price: Integer
      where
        signatory broker

        controller client can
          anytime Purchase with quantity: Integer
            returning Deal.Key
            to do
              broker does exercise stock Dispatch with delta = quantity
              create Deal with stock; client; price; quantity

In this example ``Offer`` holds an implicit key to parent ``Stock``. All
access to fields of ``Stock`` would implicitly be fetching parent record via
the key (TODO: but by who?).

The nested template syntax can be used both for surrogate keys and natural
keys (just add ``keys broker; item`` to ``Stock``).

*comment*
   - How to handle terminating choices in parent? Cascading delete? Or only
     allowed when no children are left? (These foreign key concerns apply
     having keys to other contracts in general)

.. _`Predictable Choice Outcomes`:

Predictable Choice Outcomes
---------------------------

*Priority*: HIGH

It’s important to retain the property that the controller of a choice knows what
he agrees to. This suggests that `Deterministic Contract Keys`_ should get
pinned in the application of a participant such that the controller of a choice
has a chance to confirm the specific revision that will be used before
submitting a command. Consequently, a command that references a contract key
should fail if a parallel command to modify the referenced contract gets
processed first by the sequencer. Similarly, the processing of a ``*Handler``
contract would fail in our current approach if the contract-id provided by the
nanobot got archived in the meantime.

The importance of this property becomes obvious e.g. in the case of a checkout
choice on a shopping cart contract:

.. ExcludeFromDamlParsing
.. code-block:: daml

  template Offer
    with
      broker: Party
      client: Party
      item: Text
      price: Integer
    where
      key broker; client; item

      controller client can
        anytime Purchase with quantity: Integer
          returning Deal.Key
          to create Deal with broker; client; item; price; quantity

      controller broker can
        Modify with priceNew: Integer
          returning Offer.Key
          to create this with price = priceNew

  template Cart
    with
      offer: Offer.Key
      quantity: Integer
    where
      controller offer.client can
        Purchase with quantity: Integer
          returning Deal.Key
          to client does
            exercise offer Purchase with quantity

An application orchestrating these contracts could inform the client about the
pinned price on a checkout screen and throw an error if that price changed prior
to the completion of the purchase.


.. _`Reference Data Disclosure`:

Reference Data Disclosure
-------------------------

*Priority*: MEDIUM

Given that it should be the choice controller that pins a specific contract
revisions as described in `Predictable Choice Outcomes`_, contract keys could
currently only be used to reference contracts that have the choice controller as
a stakeholder. This seems limiting as it should be possible to look up any
reference data contracts that got disclosed to the exercising party. We can
currently work around this limitation by adding observers as contract
stakeholders with a dummy choice as in the stock contract above. However, this
approach becomes problematic if the broker would like to disclose the stock
contract publicly or to a larger group of parties. In that case, it might be
better to disclose the stock contract off-ledger and to give all observers the
ability to reference it:

.. ExcludeFromDamlParsing
.. code-block:: daml

  template Stock
    with
      broker: Party
      item: Text
      quantity: Integer
    where
      key broker; item
      observer public
      ensure quantity >= 0
      signatory broker

      controller broker can
        Dispatch with delta: Integer
          returning Stock.Key
          to create Stock with broker; client; item
                               quantity = quantity - delta

  template Offer
    with
      Stock.Key stock
      client: Party
      price: Integer
    where
      signatory broker

      controller client can
        anytime Purchase with quantity: Integer
          returning Deal.Key
          to broker does
            exercise stock Dispatch with quantity
            create Deal with broker; client; item; quantity

Note that aside from making observers stakeholders, another viable workaround
for not having disclosure support is to use a separate ``*Handler`` contract
that gets processed by a party that is a stakeholder of the referenced contract.
For example, the broker could do process the purchase explicitly:

.. ExcludeFromDamlParsing
.. code-block:: daml

  template Offer
    with
      broker: Party
      client: Party
      item: Text
      price: Integer
    where
      signatory broker

      controller client can
        anytime Purchase with quantity: Integer
          returning OfferHandler.Key
          to create OfferHandler with broker; client; item; price; quantity

  template OfferHandler
    with
      broker: Party
      client: Party
      item: Text
      price: Integer
      quantity: Integer
    where
      controller broker can
        Process
          returning Deal.Key
          to broker does
            exercise stock Dispatch with quantity
            create Deal with client; item; price; quantity

      controller client can
        Cancel
          returning {}
          to return {}

This removes the need to disclose the stock contract out-of-band. However, the
client looses visibility of it and a purchase won't be completed instantly.


.. _`Uniqueness Constraints`:

Uniqueness Constraints
----------------------

*Priority*: MEDIUM

We currently have no direct way to express uniqueness constraints in DAML. For
example, we can’t express that there should be at most one stock contract per
broker and item combination. A possible work-around is to use a generator
contract that registers keys or provides new ids by increasing an internal
counter. However, this is inherently sequential and hence likely to become a
performance bottleneck.


.. _Replay Protection:

Replay Protection
-----------------

*Priority*: MEDIUM

Contract `Uniqueness Constraints`_ would prevent a second contract with
identical key from being created. However, they wouldn't protect from network
message replays in all cases. For example, a duplicate create message of a
contract with a key would go through if the contract created by the first
message gets consumed before the second message arrives. A possible solution is
to create explicit replay protection contracts that only get archived after a
configurable time in the future:

.. ExcludeFromDamlParsing
.. code-block:: daml

  template ReplayProtection
    with
      client: Party
      orderId: Text
      timeout: Time
    where
      key client; orderId

      controller client can
        Cleanup
          returning {}
          to after timeout

  template Offer
    with
      broker: Party
      client: Party
      item: Text
      price: Integer
    where
      signatory broker

      controller client can
        anytime Purchase with quantity: Integer; orderId: Text; timeout: Time
          returning ReplayProtection.Key
          to do
            create ReplayProtection with client; orderId; timeout
            create OfferHandler with broker; client; item; price; quantity


.. _`Message Deduplication`:

Message Deduplication
---------------------

*Priority*: MEDIUM

Aside from network message replays, applications might duplicate messages as
well. This case tends to be slightly harder to deal with because the two
messages are not necessarily identical. For example, an application that crashed
and recovered might resend a message with a different timestamp. As for `Replay
Protection`_, a possible solution is to create explicit dedupe contracts.
However, the appliction would have to ensure that any duplicate messages it
might generate have the same dedupe contract key as the original one.


.. _`Concurrent Quantity Manipulations`:

Concurrent Quantity Manipulations
---------------------------------

*Priority*: LOW

Given that the core purpose of ledgers is to track quantities such as cash
amounts and stock holdings, it doesn't come as a surprise that a significant
portion of our current application code deals with them. Deterministic contract
keys would already make it easier to reference the most recent revision of
contracts with quantities such as our stock contract. However, another problem
that we often encounter is that two or more parallel quantity modifications
conflict. For example, there would be conflicts in the stock contract if
multiple clients accept offers concurrently. Similarly, we would expect
conflicts in a naive implementation of multiple applications that send / receive
payments on the same cash account. This problem won't be addressed by contract
keys because of the requirement described in `Predictable Choice Outcomes`_. We
currently try to avoid conflicts by implementing relatively complex wallet
strategies in nanobots. Given how prevalent quantity manipulations are, however,
a better solution would be desirable.

A promising idea is to allow quantity deltas to be specified explicitly in
transactions. Given that the addition operation commutes, the sequencer could
reorder such actions which would allow for them being sent concurrently.
Things get a bit more complicated when accounting for lower and upper bounds
on quantities as they partially break commutativity. However, it would
still be beneficial to allow parallel commands as long as the resulting
quantity stays within the specified bounds.

The priority of this goal is set to "Low" because of the various options at hand
and their potential implications on our platform. In particular, it seems
reasonable to focus on contract keys first before looking at quantities.

*Questions*:

* Q (Ognjen): Do we have use cases for quantity reads from DAML? For example,
  issuing a monthly account statement

* Q (Ognjen): Upper limits on quantities: do we need them?




.. _`Concurrent Field Manipulations`:

Concurrent Field Manipulations
------------------------------

*Priority*: LOW

Quantities are not the only contract fields that would benefit from the option
to be updateable concurrently. A whole class of potentially useful examples are
commutative replicated data types (CmRDT) such as sets with element addition.
Interestingly, those types get studied and applied in the context of
collaborative document editing, a field that is likely to share some
similarities with distributed contract processing. Yet another example are
compare-and-set operations on individual fields. Last but not least, effective
dating might fall into this category as well because it can be interpreted as
list of time and quantity tuples that can be updated concurrently as long as the
timeline doesn't get negative anywhere.


.. _Options:

Options
=======

The purpose of this section is to provide an overview of the analysed options
for the implementation of Contract Keys, Uniqueness, Disclosure and Contract
Quantities.


.. _`Contract Keys`:

Contract Keys
-------------


.. _`Local Key Lookup`:

Local Key Lookup
~~~~~~~~~~~~~~~~

One way to implement deterministic contract keys is to resolve them to
contract-ids during local interpretation via pointwise lookups:

1. Change DAML such that choices can be exercised on `Natural Contract Keys`_.
2. Add the option to provide a map from contract keys (or their hash) to
   contract-ids when interpreting a transaction. Adapt the DAML engine to check
   for every encountered contract key whether it is in the provided map. If it
   is, the provided contract-id gets used. Otherwise, the matching contract-id
   gets fetched from the active contract set with a pointwise lookup. If no
   matching contract-id can be found the interpretation fails.
3. Return the calculated eval trace and an enriched map from contract keys to
   contract-ids that contains both the contract-ids that were provided upfront
   as well as the ones looked up during interpretation.
4. Add an option to simulate the interpretation of a transaction, that is to
   calculate the eval trace and to enrich the contract key map without
   submitting the command.

In particular, this would satisfy the requirement for `Predictable Choice
Outcomes`_ as a transaction could be simulated in a first step with an empty
contract key map to pin the contract-ids and to inspect the results.
Subsequently, the transaction could be executed in a predictable fashion by
providing the obtained contract key map.

*Comments*:

* The participant could choose to dismiss predictability by executing a
  transaction with an empty contract key map
* It would be possible to implement this without enforcing contract key
  `Uniqueness Constraints`_. In particular, we could choose to fail when
  encountering duplicates during local key lookup or even to select a matching
  contract randomly as some nanobots do in the current approach. However, given
  the confusion that could arise in case of uniqueness violations, it would
  certainly be better to satisfy both goals jointly.
* The approach would probably also work for `Surrogate Contract Keys`_ but they
  would have to be sent to participants together with new contract-id revisions.


.. _`Primitive Natural Keys`:

Primitive Keys
~~~~~~~~~~~~~~

Another option is to add `Deterministic Contract Keys`_ as a primitive to the
ledger model. The requirement for `Predictable Choice Outcomes`_ can be satisfied
by sending contract-ids with the transaction that pin specific contract revisions.
One advantage of this over `Local Key Lookup`_ is that the sequencer could provide
both the pinned contract-ids and the current ones to a choice and hence facilitate
`Explicit Concurrency Control`_.

*Comments*:

* This would simplify the allocation of contract-ids, which currently requires a
  reasonably complicated two-step approach: the submitter uses synthetic
  transaction-local ids, which get translated to globally unique ones *after*
  the transaction has been committed and thereby assigned a globally unique id.
  The reason for the two-step approach is that the submitter is not trusted by
  all parties to not introduce a clash of contract-ids.
* `Surrogate Contract Keys`_ are simply an implicit, hidden key field that get
  automatically filled upon initial creation of a contract. The key type is
  also referred to as ``Stock.Key`` - but this does not have any fields that
  can be accessed.
* `Uniqueness Constraints`_ are a must.


.. _Uniqueness:

Uniqueness
----------

In the case of `Natural Contract Keys`_ we first need to define the maintainers
that enforce the uniqueness constraints. A natural choice is to require one or
more contract signatories to be part of the key and to give this obligation to
them. In the current architecture, the sequencer could then perform this check
on their behalf. In the Sirius architecture, this could probably be done via
explicit confirmations instead.

In the case of `Surrogate Contract Keys`_ we could use the contract-id of the
first revision as contract key and therefore simplty inherit the current logic
to enforce contract-id uniqueness.

* Q (Andrae): It is probably worth noting that even without Values, it may be
  desirable to support Uniqueness constraints over a subset of fields of a
  template.

.. _Disclosure:

Disclosure
----------

In order to provide contract disclosure, we first need to find a way to describe
the observers of contracts. Observers can be groups of parties. For example, the
broker of the `Canonical Example`_ might want to disclose the stock contract to
a public group, i.e. to everyone on the ledger. Further, we need to specify how
contracts can be sent to the observers off-ledger such that they can still use
those contracts in workflows.

* TODO (Simon): Add definition for Obligables / Signatories / Stakeholders vs
  Observers and describe approach to off-ledger disclosure.


.. _`Contract Quantities`:

Contract Quantities
-------------------

.. _`Primitive Quantity`:

Primitive Quantity
~~~~~~~~~~~~~~~~~~

Interestingly, the quantities described in `Concurrent Quantity Manipulations`_
share a lot of similarities with the active flag that tracks the consumption of
contracts. In particular, the active flag could be interpreted as a quantity
field that is bounded between 0 and 1. Therefore, we could consider replacing
the active flag by a quantity and give the modeler the option to specify its
upper bound. Such a quantified contract would have an intuitive interpretation
as a stack of contracts that can efficiently be processed in bulk. For example,
a creation of an Iou with amount 50$ could be interpreted as a bulk creation of
50 one-dollar contracts and a transfer of 20$ as a bulk exercise on 20 of the 50
one-dollar contracts. This intuition could be leveraged further by providing a
special syntax along the following lines:

.. ExcludeFromDamlParsing
.. code-block:: daml

  template stock (broker: Party) (c1: Party) (c2: Party) (item: Text) limit 1000 =
    broker agrees toText broker <> " has 1 of " <> item
    while awaiting
    { dispatch: broker chooses then pure {}
    , observe1: c1 chooses such that False then pure {}
    , observe2: c2 chooses such that False then pure {}
    };

  template offer (broker: Party) (client: Party) (item: Text)
    (price: Integer) (stockCid: ContractId) limit 1 =
    broker agrees toText broker <> " offers " <> item <> " to " <> toText client
    while awaiting
    { purchase: whenever client chooses quantity: Integer then
        update
        [ broker exercises dispatch on quantity of stockCid
        ; create (deal broker client item price quantity)
        ]
    };

*Comments*:

* The main advantage of this approach is its intuitive interpretation.
* Asset quantity reads would be possible but would have to pin the quantity
  to satisfy the requirement for `Predictable Choice Outcomes`_. In particular,
  they would fail if there is a parallel quantity modification that arrives first.
* It's not obvious how to extend the approach to other types of `Concurrent
  Field Manipulations`_.
* Also, it's unclear how to best identify a specific revision of the quantity
  given that the contract-id remains constant.
* With regards to disclosure, the problem described in `Reference Data
  Disclosure`_ still exists. For example, if the clients were no stakeholders of
  the stock asset, the broker would have to disclose that contract and its
  quantity (or a sufficient part of it). Alternatively, we could fall back to a
  separate ``*Handler`` contract that gets processed by the broker concurrently.
* When combining this approach with `Primitive Natural Keys`_, referenced value
  fields don't seem to make sense for limits greater than 1 because it wouldn't
  be clear how to modify them after creation.


.. _`Quantity Fields`:

Quantity Fields
~~~~~~~~~~~~~~~

Rather than promoting contract quantities to a primitive, we could offer an
``addQuantity`` action that has special support in the platform to facilitate
concurrent quantity field manipulations as long as the resulting value doesn't
violate the specified bounds:

.. ExcludeFromDamlParsing
.. code-block:: daml

  template stock (broker: Party) (c1: Party) (c2: Party) (item: Text) (quantity: Integer) =
    key broker, item
    ensure quantity >= 0
    in broker agrees toText broker <> " has " <> toText quantity <> " of " <> item
    while awaiting
    { dispatch: broker chooses delta: Integer then
        addQuantity quantity (-1 * delta)
    , observe1: c1 chooses such that False then pure {}
    , observe2: c2 chooses such that False then pure {}
    };

  template offer (broker: Party) (client: Party) (item: Text) (price: Integer) =
    broker agrees toText broker <> " offers " <> item <> " to " <> toText client
    while awaiting
    { purchase: whenever client chooses quantity: Integer then
        update
        [ broker exercises dispatch with quantity on stock {broker = broker, item = item}
        ; create (deal broker client item price quantity)
        ]
    };

The ``addQuantity`` action would have to be private in the sense that it could
only be called from within the contract it modifies. In order to manipulate
quantities from other contracts, it would have to be wrapped in a choice that
describes the external controllers and potentially specifies further rules.

*Comments*:

* Other types of `Concurrent Field Manipulations`_ seem straight-forward with
  this approach but would all have to be supported explicitly by the platform.
* Potentially incompatible with the `Local Key Lookup`_ approach because the
  contract-id would change with every quantity manipulation.

* Q (Ognjen): Is the proposal sufficient to implement effective dating? Do we
  want to implement it purely inside of DAML, and if so, what is the best way?


.. _`Explicit Concurrency Control`:

Explicit Concurrency Control
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A more radical approach would be to go in the opposite direction of `Primitive
Quantity`_ and implement the active flag as a normal boolean contract field with
assertions in every choice that check its value to be true. This would only be
possible if the choice has access to the most recent revision of a
`Deterministic Contract Keys`_. However, with `Primitive Natural Keys`_ or
Primitive Surrogate Keys the sequencer could provide both the pinned
contract-id and the most recent one to the choice and hence give full
concurrency control to the DAML author. In particular, any `Concurrent Field
Manipulations`_ could be implemented in DAML by comparing the values associated
with the two contract-ids.

TODO (Silvan): This requires more work, i.p. a comment on performance
implications in both the Apollo and the Sirius architecture.

* Q (Andrae): I would appreciate some discussion of how this would work in the
  presence of DAML-synthesised keys.

.. _`Implementation Proposal`:

Implementation Proposal
=======================

The purpose of this section is to describe an implementation proposal with
phased delivery milestones.

Phase 1: Local Key Lookup
-------------------------

The approach described in `Local Key Lookup`_ seems straight-forward to
implement and already adds a lot of value on its own. Therefore, it makes sense
to deliver this as a first step. There is some freedom to choose which type of
deterministic keys to go for initially but `Natural Contract Keys`_ seem to be
less intrusive and add slightly more value given that we currently have to use
nanobots for natural key lookups. A method to enforce `Uniqueness`_ constraints
should be implemented as well if possible to avoid a large category of potential
problems. However, it would be an option to drop this if necessary.

TODO (Silvan): This needs more work


.. _`Technical Specification`:

Technical Specification
=======================

The purpose of this section is to describe the impact on the tech stack in detail.

DAML 1.x Syntax Specification
-----------------------------

* Specifying key parameters
* Referencing contracts by keys

TODO (Sofus): Write section


Ledger Server
-------------

* DAML Engine
* Ledger API
* Sequencer
  - uniqueness checks

TODO (???): Write section


Ledger Client
-------------

* Codegen
  - mapping DAML keys to target languages

* ODS
  - reference data via foreign keys?
  - daml keys → sql composite primary key

* App-Fw
  - Query and command construction

TODO (???): Write section


.. _`Related Work and Requirements`:

Related Work
============


.. _`DAML Query`:

DAML Query
----------

The approach to implement Contract Keys with a `Local Key Lookup`_ essentially
allows to perform an exercise on a single contract that got returned by the
following SQL query:

`SELECT templateName.value FROM templateName WHERE templateName.key=<given key>`

The DAML Query effort takes a similar perspective in that it plans to allow
exercising contracts that got returned by more general SQL queries.

The main differences between the two proposals are that Contract Keys address
the desire to perform pointwise key lookups inside legally binding contract
choices to avoid the current redundancy of specifying the query details in a
nanobot and the corresponding match inside the exercised choice. DAML Query, on
the other hand, addresses the desire to concisely express execution strategies
that follow the pattern of querying some contracts and exercising a choice on
them. Both proposals are valid and can coexist because they are in fact quite
orthogonal. In particular, the pointwise contract key lookups can't be extended
much further as a more general query would not be verifiable anymore by the
involved stakeholders. Conversly, the more general DAML queries can't be used in
contract choices for the same reason and will therefore only be available in
scenarios and execution strategies. The main touchpoint between the two
initiatives is the syntax that gets chosen for a pointwise key lookup.


Market Setup and RBAC (Role Based Access Control)
-----------------------------------------------------

Requirements for modelling market structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* expressing hierarchy and relationships between legal entities
* expressing relationships between participants in the system
* expressing relationships between participants and legal entities or participants and accounts
* expressing what actions party with a given role can do in such a way that will allow defining new permissions, 
  revoking existing ones or making permissions more granular/parametric
* permissions with regard to a domain object can be granted to more than one party, 
  eg. two parties allowed to control assets in one account with a restriction 
  that one of these parties controls equity assets and the other controls options
* it should be possible to create completely new kind of capability in the future and grant it to a party, 
  for instance adding collateral agent role to an account that is only allowed to lock assets
* it should be possible to collect permissions into groups (capabilities) that can then form higher level groups 
  and ultimately will be used to express in a compossible manner what a participant with a given role is allowed to do 
  or what permissions are granted when some relationship is created

Design description
~~~~~~~~~~~~~~~~~~

The design is based on decoupling representation of domain objects and modification of the state of the ledger. 
Any modification to the state of the ledger is accepted iff a Party presents a permission token corresponding to such change. 
Permission token expresses delegation of rights to a Party that can act on that token. 
This technique allows for describing authorization rules on a higher level.

Account Controller example
~~~~~~~~~~~~~~~~~~~~~~~~~~

Account Controller is an entity that is in control of assets in a given account that is they can split, merge, transfer and lock assets.
Given requirements above we would like to be able to have two Account Controllers for one account, limit Account Controller role
for some entity to handle assets of a certain type and define a new role that has a subset of Account Controller's permissions
such as only be able to split, merge and lock assets.

DAML example
~~~~~~~~~~~~

.. ExcludeFromDamlParsing
.. code-block:: daml

  template Account (data : AccountData) = await
  { obl:
    data[operator] must choose until 9999-01-01T00:00:00Z such that False then pure {}
  };
  
  type AccountID = <accountId : Text>;
  type AccountData = {
    operator : Party,
    accountId: AccountID
  };


  template Asset (data : AssetData) = 
  ensure data[quantity] > 0
  in await 
  { obl:
    data[operator] must choose until 9999-01-01T00:00:00Z such that False then pure {}
  , burn:
    data[operator] chooses then update [pure data]
  };

  type SymbolID = <symbolId : Text>;
  type AssetData = {
    operator : Party,
    symbol : SymbolID,
    quantity : Integer,
    accountId : AccountID,
    locked : Bool
  };

  type AssetType = <equity: Unit, warrant: Unit, option: Unit, ...>;
  type AssetPermissionData = {
    operator : Party,
    actor : Party,
    participantId : ParticipantIdentifier,
    accountId: AccountID,
    assetType : List AssetType};

RBAC in DAML with disclosure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach assumes disclosure of Asset and Account contracts to participants granted
corresponding permission contracts.

.. ExcludeFromDamlParsing
.. code-block:: daml

  template AssetLockPermission (data : AssetPermissionData) = await {
    obl: 
    data[operator] must choose … 
  , executeLock:  
    whenever data[actor] chooses 
    accountCid : ContractId Account, assetCid : ContractId Asset,
    lockAmount : Integer
    then update
    [ archive assetCid and create locked asset contract and remaining unlocked asset contract ]
  };


RBAC in DAML without disclosure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

That approach assumes that data of Asset and Account contracts is disclosed to participants
that were granted the right to act on this contracts off-ledger.

.. ExcludeFromDamlParsing
.. code-block:: daml

  template AssetLockPermission (data : AssetPermissionData) = await 
  { obl: data[operator] must choose … 
  , obs: data[actor] chooses such that False … 
  , revoke: data[operator] chooses then pure {} 
  };

  template AssetLockRequest (data : AssetLockRequestData) =
  await { 
    obl: data[actor] must choose …
  , executeLock: data[operator] chooses then
    update [ verify permission token and do lock asset ]
  };

  type AssetLockRequestData = {
    operator : Party,
    actor : Party,
    participantCid : ContractId Participant,
    accountCid : ContractId Account,
    assetPermissionCid : AssetPermissionCID,
    assetCid : ContractId Asset,
    quantity: Integer
  };

* Q: How can an AssetLockRequest be created? Through a choice on the
  AssetLockPermission contract?

* A: Yes, that makes sense. For simplicity in my code example I was using bare ``create``.

RBAC in the context of Contract Keys proposal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

From the preliminary analysis the RBAC imlementation would benefit from Contract Keys proposal in similar way
to examples analyzed above eliminating the need for ContractId lookups in the nanobots. Additionally, 
having Contract Keys could help decrease the number of contracts that needs to be disclosed to participants. 
In the example it wouldn't be necessary to disclose information about Account contract.
Having said that, it seems that Contract Keys feature is largely orthogonal to RBAC design.

* Q (Silvan): Why would the Account contract not need to be disclosed anymore with Contract Keys?

* A (Pawel): It actually depends on the content of Account contract. In my example model,
  Account only had accountId in it, proof that account with given accountId exists
  and accountId is unique. In that case, Account Controller doesn't really need to see
  contract content, because there's nothing else to see. If there was additional data
  in Account contract, but it was irrelevant for asset control workflows,
  that data wouldn't need to be shared with Account Controller.

.. _Appendix:

Appendix
========

The following sections contain valuable left-overs from previous work.

TODO (???): Fold this content into the sections above if appropriate.


.. _`Abstract Ledger Model for Value and Quantity Fields`:

Abstract Ledger Model for Value and Quantity Fields
---------------------------------------------------

This is a write-up of an abstract ledger model for contracts with `Primitive
Natural Keys`_ as well as `Quantity Fields`_.

.. ExcludeFromDamlParsing
.. code-block:: daml

  import Data.Map.Strict as M
  import Control.Monad (foldM, guard)

  type Key = Int
  type Value = Int
  type Quantity = Int
  type Party = Int

  -- A contract consists of a key, value, and a quantity. The first two
  -- are always explicitly referenced on all actions on a contract. In
  -- contrast, for quantities, we allow just the effect - the change of
  -- quantity - to be specified.
  type Contract = (Key, Value, Quantity)

  -- A type synonym for referenced parts, to highlight the difference
  type ContractRef = (Key, Value)

  -- This is the set of primitive operations on quantities that's
  -- supported. It could potentially be extended to support operations
  -- similar to compare-and-set or CRDTs.
  data Change = ChQuantity Int
    deriving (Eq, Show)

  -- This gives the semantics to changes
  applyChange :: Change -> Quantity -> Maybe Quantity
  applyChange (ChQuantity i) v
    | v + i >= 0  = Just $ v + i
    | otherwise   = Nothing

  -- The action data type. We distinguish between consuming exercises,
  -- which pin the quantities, and non-consuming ones, which do not,
  -- and we add an `Update` constructor for performing quantity updates.
  -- Note that this does not imply that contract updates can
  -- appear as top-level constructs in DAML code. No action model specified
  -- by DAML template stores will have Updates as top-level actions. Note
  -- that this invalidates the subaction-closure property defined in the DA Ledger
  -- Report.
  --
  -- Furthermore, all actions are assumed to pin all the values. We
  -- currently assume single actors for exercises. Adding multiple
  -- actors is straightforward (by replacing `Party` by `Set Party` in the
  -- constructors).
  --
  -- A check for equality on the quantity can be modeled by exercising
  -- the contract with a singleton list of consequence actions, the
  -- re-creation of the same contract.
  data Action =
      Create Contract
    | Exercise Party Contract [Action]
    | NCExercise Party ContractRef [Action]
    | Update ContractRef Change
    deriving (Eq, Show)

  -- Effects correspond to actions in a straightforward fashion
  data Effect = ECreate Contract | EArchive Contract | EUpdate ContractRef Change

  actionEffect :: Action -> [Effect]
  actionEffect (Create c) = [ECreate c]
  actionEffect (Exercise act c tr) = EArchive c : concatMap actionEffect tr
  actionEffect (NCExercise act ref tr) = concatMap actionEffect tr
  actionEffect (Update ref vf) = [EUpdate ref vf]

  type State = M.Map Key (Value, Quantity)

  initState :: State
  initState = M.empty

  apply :: State -> Effect -> Maybe State

  apply s (ECreate (k, v, q)) = do
    -- Creating duplicate keys is forbidden.
    guard $ M.notMember k s
    return $ M.insert k (v, q) s

  apply s (EArchive (k, v, q)) = do
    -- Archives ensure that both the values and the quantities match
    guard $ M.lookup k s == Just (v, q)
    return $ M.delete k s

  apply s (EUpdate (k, v) vf) = do
    (v', q) <- M.lookup k s
    -- Updates only check the value
    guard $ v' == v
    -- A failing change fails the update
    newQ <- applyChange vf q
    return $ M.insert k (v, newQ) s

  execTransaction :: [Action] -> Maybe State
  execTransaction acts = foldM apply initState $ concatMap actionEffect acts


*Questions*:

* Q (Ognjen): How should quantity reads be reflected in the abstract ledger model?


.. _`Authorization Model for Value and Quantity Fields`:

Authorization Model for Value and Quantity Fields
-------------------------------------------------

This is a write-up of the authorization model for contracts with `Primitive
Natural Keys`_ as well as `Quantity Fields`_.

The authorization-related definitions of the DA Ledger Tech Report
v1.1 rely on the "no-duplicate creates" property of consistent
ledgers. Contract keys violate this property and render the
current definitions inapplicable, as shown in this example:

- `P` creates a contract `c`
- `Q` exercises it, resulting in an obligation for `P`
- `P` creates `c` again
- `Q` exercises it again, resulting in another obligation for `P`

The current definition would deem this ledger well-authorized. The
root problem is that the current definition has no notion of
consumption. By introducing a resource-aware logic for our judgments,
a fragment of separation logic, we obtain a simple model that captures
both authorization and consistency requirements on the ledger. We have
only three types of formulas:

.. code-block:: none

   P = ε | (X, k |-> (v, q)) | P * P

where `ε` is the empty proposition, `X` is a set of parties, `k` is a
key, `v` is a value, `q` is a quantity and `*` is the "separating
conjunction". The idea of separating conjunctions is that
`P1 * P2` is only defined if `P1` and `P2` describe separate key
spaces. Technically, the models of our formulas are sets of elements
of the form `(X, k |-> (v, q))`. The empty proposition `ε` is
satisfied only by the empty set. Then, we define a partial monoid
operation `o` on the models, such that `M_1 o M_2` is the union of
`M_1` and `M_2` if `M_1` and `M_2` do not contain two contracts with
identical keys, and it is undefined otherwise. This forms a partial
commutative monoid, as is standard for models of the logic of bunched
implications (a very small fragment of which we use).

Next, we define our authorization and consistency judgments.
These are Hoare triples on commits and ledgers:

1. Create

   .. code-block:: none

      signatories c ⊆ X
      -------------------------
      {} (X, Create c) {(X, c)}

   where we assume that `c` is of the form `k |-> (v, q)` for some
   `k`, `v` and `q`. Intuitively, `Create c` consumes nothing and
   produces `(X, c)`, but only if `X` includes all signatories (if
   any) of `c`.

2. Exercise

   .. code-block:: none

      act ∈ Y
      {P} map (X ∪ Y,) tr {Q}
      ---------------------------------------
      {(X, c) * P} (Y, Exercise act c tr) {Q}

   An exercise consumes some authorization for `c` by some set of
   parties `X`; it combines `X` with the set `Y` of the committers
   of the exercise, and then maps all elements of `tr` to commits with
   `X ∪ Y` as committers. If we can derive a judgment for such a `tr`, we
   can combine it with the consumption of `c`.

   All of this is conditional on the actor being among the
   committers of the exercise.

3. Non-consuming exercise

   .. code-block:: none

      act ∈ Y
      {(X, k |-> (v, q)) * P} map (X ∪ Y,) tr {Q}
      ------------------------------------------------------------
      {(X, k |-> (v, q)) * P} (Y, NCExercise act (k, v) tr) {Q}

   Similar to exercise, but:

   a) a `NCExercise` need not specify the quantity

   b) the exercised contract is available to the transaction

4. Update

   .. code-block:: none

      change q ~> q'
      -----------------------------------------------------------------
      {(X, k |-> (v, q))} (Y, Update (k, v) change) {(X, k |-> (v, q'))}

   An update applies the specified change. Note that the rule is
   conditioned on the change succeeding. It does not affect the
   authorization of the contract in any way.

5. Frame

   .. code-block:: none

      {P} C {Q}
      -----------------
      {P * R} C {Q * R}

   This is a standard separation logic rule, allowing us to focus on
   what each action does; this stays unchanged in any (compatible)
   context.

6. Cons

   .. code-block:: none

      {P} C {Q}
      {Q} CS {R}
      --------------
      {P} C # CS {R}

   Finally, viewing a ledger as a composed program, this is the
   standard Hoare rule for sequential composition.


A ledger `L` is then consistent and authorized if we can derive a
judgment `{} L {P}` for some `P`. Note that multiple judgments are
in principle possible: if an `Exercise c` is committed in a context
`(X1, c) * (X2, c)`, then it is possible that two judgments can be
derived, one with the precondition `(X1, c)` and another one with a
precondition `(X2, c)`. If `{} L {P}`, then `P` gives us the
currently valid authorizations, based on the ledger.


References
==========
