.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Glossary of concepts
####################

Daml
****

**Daml** is a programming language for writing `smart contracts <#contract>`__, that you can use to build an application based on a `ledger <#ledger-daml-ledger>`__. You can run Daml contracts on many different ledgers.

Contract
===========================

A **contract** is an item on a `ledger <#ledger-daml-ledger>`__. They are created from blueprints called `templates <#template>`__, and include:

- data (parameters)
- roles (`signatory`_, `observer`_)
- `choices <#choice>`__ (and `controllers <#controller>`__)

Contracts are immutable: once they are created on the ledger, the information in the contract cannot be changed. The only thing that can happen to it is that the contract can be `archived <#active-contract-archived-contract>`__.

Active contract, archived contract
----------------------------------

When a `contract <#contract>`__ is created on a `ledger <#ledger-daml-ledger>`__, it becomes **active**. But that doesn't mean it will stay active forever: it can be **archived**. This can happen:

- if the `signatories <#signatory>`__ of the contract decide to archive it
- if a `consuming choice <#consuming-choice>`__ is exercised on the contract

Once the contract is archived, it is no longer valid, and `choices <#choice>`__ on the contract can no longer be exercised.

Template
========

A **template** is a blueprint for creating a `contract <#contract>`__. This is the Daml code you write.

For full documentation on what can be in a template, see :doc:`/daml/reference/templates`.

Choice
======

A **choice** is something that a `party <#party>`__ can `exercise <#exercise>`__ on a `contract <#contract>`__. You write code in the choice body that specifies what happens when the choice is exercised: for example, it could create a new contract.

Choices give you a way to transform the data in a contract: while the contract itself is immutable, you can write a choice that `archives <#active-contract-archived-contract>`__ the contract and creates a new version of it with updated data.

A choice can only be exercised by its `controller <#controller>`__. Within the choice body, you have the `authorization <#authorization-signing>`__ of all of the contract's `signatories <#signatory>`__.

For full documentation on choices, see :doc:`/daml/reference/choices`.

Consuming choice
----------------

A **consuming choice** means that, when the choices is exercised, the `contract <#contract>`__ it is on will be `archived <#active-contract-archived-contract>`__. The alternative is a `nonconsuming choice <#nonconsuming-choice>`__.

Consuming choices can be `preconsuming <#preconsuming-choice>`__ or `postconsuming <#postconsuming-choice>`__.

Preconsuming choice
~~~~~~~~~~~~~~~~~~~

A `choice <#choice>`__ marked **preconsuming** will be `archived <#active-contract-archived-contract>`__ at the start of that `exercise <#exercise>`__.

Postconsuming choice
~~~~~~~~~~~~~~~~~~~~

A `choice <#choice>`__ marked **postconsuming** will not be `archived <#active-contract-archived-contract>`__ until the end of the `exercise <#exercise>`__ choice body.

Nonconsuming choice
--------------------

A **nonconsuming choice** does NOT `archive <#active-contract-archived-contract>`__ the `contract <#contract>`__ it is on when `exercised <#exercise>`__. This means the choice can be exercised more than once on the same `contract <#contract>`__.

Disjunction choice, flexible controllers
----------------------------------------

A **disjunction choice** has more than one `controller <#controller>`__.

If a contract uses **flexible controllers**, this means you don't specify the controller of the `choice <#choice>`__ at `creation <#create>`__ time of the `contract <#contract>`__, but at `exercise <#exercise>`__ time.


.. _glossary-party:

Party
=====

A **party** represents a person or legal entity. Parties can `create contracts <#create>`__ and `exercise choices <#exercise>`__.

`Signatories <#signatory>`_, `observers <#observer>`__, `controllers <#controller>`__, and `maintainers <#maintainer>`__ all must be parties, represented by the ``Party`` data type in Daml.

.. Something about how they work in the `execution engine`.

Signatory
---------

A **signatory** is a `party <#party>`__ on a `contract <#contract>`__. The signatories MUST consent to the `creation <#create>`__ of the contract by `authorizing <#authorization-signing>`__ it: if they don't, contract creation will fail.

For documentation on signatories, see :doc:`/daml/reference/templates`.

Observer
--------

An **observer** is a `party <#party>`__ on a `contract <#contract>`__. Being an observer allows them to see that instance and all the information about it. They do NOT have to `consent to <#authorization-signing>`__ the creation.

For documentation on observers, see :doc:`/daml/reference/templates`.

Controller
----------

A **controller** is a `party <#party>`__ that is able to `exercise <#exercise>`__ a particular `choice <#choice>`__ on a particular `contract <#contract>`__.

Controllers must be at least an `observer`_, otherwise they can't see the contract to exercise it on. But they don't have to be a `signatory`_. this enables the :doc:`propose-accept pattern </daml/patterns/initaccept>`.

Choice Observer
---------------

A **choice observer** is a `party <#party>`__ on a `choice <#choice>`__. Choice observers are guaranteed to see the choice being exercised and all its consequences with it.

Stakeholder
-----------

**Stakeholder** is not a term used within the Daml language, but the concept refers to the `signatories <#signatory>`__ and `observers <#observer>`__ collectively. That is, it means all of the `parties <#party>`__ that are interested in a `contract <#contract>`__.

Maintainer
----------

The **maintainer** is a `party <#party>`__ that is part of a `contract key <#contract-key>`__. They must always be a `signatory`_ on the `contract <#contract>`__ that they maintain the key for.

It's not possible for keys to be globally unique, because there is no party that will necessarily know about every contract. However, by including a party as part of the key, this ensures that the maintainer *will* know about all of the contracts, and so can guarantee the uniqueness of the keys that they know about.

For documentation on contract keys, see :doc:`/daml/reference/contract-keys`.

Authorization, signing
======================

The Daml runtime checks that every submitted transaction is **well-authorized**, according to the :doc:`authorization rules of the ledger model </concepts/ledger-model/ledger-integrity>`, which guarantee the integrity of the underlying ledger.

A Daml update is the composition of update actions created with one of the items in the table below. A Daml update is well-authorized when **all** its contained update actions are well-authorized. Each operation has an associated set of parties that need to authorize it:

.. list-table:: Updates and required authorization
   :header-rows: 1

   * - Update action
     - Type
     - Authorization
   * - ``create``
     - ``(Template c) => c -> Update (ContractId c)``
     - All signatories of the created contract
   * - ``exercise``
     - ``ContractId c -> e -> Update r``
     - All controllers of the choice
   * - ``fetch``
     - ``ContractId c -> e -> Update r``
     - One of the union of signatories and observers of the fetched contract
   * - ``fetchByKey``
     - ``k -> Update (ContractId c, c)``
     - Same as ``fetch``
   * - ``lookupByKey``
     - ``k -> Update (Optional (ContractId c))``
     - All key maintainers

At runtime, the Daml execution engine computes the required authorizing parties from this mapping. It also computes which parties have given authorization to the update in question. A party is giving authorization to an update in one of two ways:

- It is the signatory of the contract that contains the update action.
- It is element of the controllers executing the choice containing the update action.

Only if all required parties have given their authorization to an update action, the update action is well-authorized and therefore executed. A missing authorization leads to the abortion of the update action and the failure of the containing transaction.

It is noteworthy, that authorizing parties are always determined only from the local context of a choice in question, that is, its controllers and the contract's signatories. Authorization is never inherited from earlier execution contexts.

Standard library
================

The **Daml standard library** is a set of `Daml` functions, classes and more that make developing with Daml easier.

For documentation, see :doc:`/daml/stdlib/index`.

Agreement
=========

An **agreement** is part of a `contract <#contract>`__. It is text that explains what the contract represents.

It can be used to clarify the legal intent of a contract, but this text isn't evaluated programmatically.

See :doc:`/daml/reference/templates`.

Create
======

A **create** is an update that creates a `contract <#contract>`__ on the `ledger <#ledger-daml-ledger>`__.

Contract creation requires `authorization <#authorization-signing>`__ from all its `signatories <#signatory>`__, or the create will fail. For how to get authorization, see the :doc:`propose-accept </daml/patterns/initaccept>` and :doc:`multi-party agreement </daml/patterns/multiparty-agreement>` patterns.

A `party <#party>`__ `submits <#submitting-commands-writing-to-the-ledger>`__ a create `command <#commands>`__.

See :doc:`/daml/reference/updates`.

Exercise
========

An **exercise** is an action that exercises a `choice <#choice>`__ on a `contract <#contract>`__ on the `ledger <#ledger-daml-ledger>`__. If the choice is `consuming <#consuming-choice>`__, the exercise will `archive <#active-contract-archived-contract>`__ the contract; if it is `nonconsuming <#nonconsuming-choice>`__, the contract will stay active.

Exercising a choice requires `authorization <#authorization-signing>`__ from all of the `controllers <#controller>`__ of the choice.

A `party <#party>`__ `submits <#submitting-commands-writing-to-the-ledger>`__ an exercise `command <#commands>`__.

See :doc:`/daml/reference/updates`.

Daml Script
===========

**Daml Script** provides a way of testing Daml code during development. You can run Daml Script inside `Daml Studio <#daml-studio>`__, or write them to be executed on `Sandbox <#sandbox>`__ when it starts up.

They're useful for:

- expressing clearly the intended workflow of your `contracts <#contract>`__
- ensuring that parties can exclusively create contracts, observe contracts, and exercise choices that they are meant to
- acting as regression tests to confirm that everything keeps working correctly

In Daml Studio, Daml Script runs in an emulated ledger. You specify a linear sequence of actions that various parties take, and these are evaluated in order, according to the same consistency, authorization, and privacy rules as they would be on a Daml ledger. Daml Studio shows you the resulting `transaction <#transactions>`__ graph, and (if a Daml Script fails) what caused it to fail.

See :ref:`testing-using-script`.

.. Damle, Daml runtime, Daml execution engine
.. ==========================================

.. The **Daml runtime** (sometimes also called the Daml execution engine or Damle)...

Contract key
============

A **contract key** allows you to uniquely identify a `contract <#contract>`__ of a particular `template <#template>`__, similarly to a primary key in a database table.

A contract key requires a `maintainer <#maintainer>`__: a simple key would be something like a tuple of text and maintainer, like ``(accountId, bank)``.

See :doc:`/daml/reference/contract-keys`.

.. _dar-file-dalf-file:

DAR file, DALF file
===================

A ``.dar`` file is the result of compiling Daml using the `Assistant <#assistant>`__.

You upload ``.dar`` files to a `ledger <#ledger-daml-ledger>`__ in order to be able to create contracts from the templates in that file.

A ``.dar`` contains multiple ``.dalf`` files. A ``.dalf`` file is the output of a compiled Daml package or library. Its underlying format is `Daml-LF <#daml-lf>`__.

.. Package, module, library
.. ========================

.. TODO ask Robin

Developer tools
***************

Assistant
=========

**Daml Assistant** is a command-line tool for many tasks related to Daml. Using it, you can create Daml projects, compile Daml projects into `.dar files <#dar-file-dalf-file>`__, launch other developer tools, and download new SDK versions.

See :doc:`/tools/assistant`.

Studio
======

**Daml Studio** is a plugin for Visual Studio Code, and is the IDE for writing Daml code.

See :doc:`/daml/daml-studio`.

Sandbox
=======

**Sandbox** is a lightweight ledger implementation. In its normal mode, you can use it for testing.

You can also run the Sandbox connected to a PostgreSQL back end, which gives you persistence and a more production-like experience.

See :doc:`/tools/sandbox`.

Navigator
=========

**Navigator** is a tool for exploring what's on the ledger. You can use it to see what contracts can be seen by different parties, and `submit commands <#submitting-commands-writing-to-the-ledger>`__ on behalf of those parties.

Navigator GUI
-------------

This is the version of Navigator that runs as a web app.

See :doc:`/tools/navigator/index`.

Extractor
=========

**Extractor** is a tool for extracting contract data for a single party into a PostgreSQL database.

See :doc:`/tools/extractor`.

Building applications
*********************

Application, ledger client, integration
=======================================

**Application**, **ledger client** and **integration** are all terms for an application that sits on top of the `ledger <#ledger-daml-ledger>`__. These usually `read from the ledger <#reading-from-the-ledger>`_, `send commands <#submitting-commands-writing-to-the-ledger>`__ to the ledger, or both.

There's a lot of information available about application development, starting with the :doc:`/app-dev/app-arch` page.

Ledger API
==========

The **Ledger API** is an API that's exposed by any `Daml ledger <#ledger-daml-ledger>`__.
Alternative names: **Daml Ledger API** and **gRPC Ledger API** if disambiguation from other technologies is needed.
See :doc:`/app-dev/ledger-api` page.
It includes the following :doc:`services </app-dev/services>`.

Command submission service
--------------------------

Use the **command submission service** to `submit commands <#submitting-commands-writing-to-the-ledger>`__ - either create commands or exercise commands - to the `ledger <#ledger-daml-ledger>`__. See :ref:`command-submission-service`.

Command completion service
--------------------------

Use the **command completion service** to find out whether or not `commands you have submitted <#submitting-commands-writing-to-the-ledger>`__ have completed, and what their status was. See :ref:`command-completion-service`.

Command service
---------------

Use the **command service** when you want to `submit a command <#submitting-commands-writing-to-the-ledger>`__ and wait for it to be executed. See :ref:`command-service`.

Transaction service
-------------------

Use the **transaction service** to listen to changes in the `ledger <#ledger-daml-ledger>`__, reported as a stream of `transactions <#transactions>`__. See :ref:`transaction-service`.

Active contract service
-----------------------

Use the **active contract service** to obtain a party-specific view of all `contracts <#contract>`__ currently `active <#active-contract-archived-contract>`__ on the `ledger <#ledger-daml-ledger>`__. See :ref:`active-contract-service`.

Package service
---------------

Use the **package service** to obtain information about Daml packages available on the `ledger <#ledger-daml-ledger>`__. See :ref:`package-service`.

Ledger identity service
-----------------------

Use the **ledger identity service** to get the identity string of the `ledger <#ledger-daml-ledger>`__ that your application is connected to. See :ref:`ledger-identity-service`.

Ledger configuration service
----------------------------

Use the **ledger configuration service** to subscribe to changes in `ledger <#ledger-daml-ledger>`__ configuration. See :ref:`ledger-configuration-service`.

Ledger API libraries
====================

The following libraries wrap the `ledger API <#ledger-api>`__ for more native experience applications development.

Java bindings
-------------

An idiomatic Java library for writing `ledger applications <#application-ledger-client-integration>`__. See :doc:`/app-dev/bindings-java/index`.

Scala bindings
--------------

An idiomatic Scala library for writing `ledger applications <#application-ledger-client-integration>`__. See :doc:`/app-dev/bindings-scala/index`.

Reading from the ledger
=======================

`Applications <#application-ledger-client-integration>`__ get information about the `ledger <#ledger-daml-ledger>`__ by **reading** from it. You can't query the ledger, but you can subscribe to the transaction stream to get the events, or the more sophisticated active contract service.

Submitting commands, writing to the ledger
==========================================

`Applications <#application-ledger-client-integration>`__ make changes to the `ledger <#ledger-daml-ledger>`__ by **submitting commands**. You can't change it directly: an application submits a command of `transactions <#transactions>`__. The command gets evaluated by the runtime, and will only be accepted if it's valid.

For example, a command might get rejected because the transactions aren't `well-authorized <#authorization-signing>`__; because the contract isn't `active <#active-contract-archived-contract>`__ (perhaps someone else archived it); or for other reasons.

This is echoed in `scenarios <#scenario>`__, where you can mock an application by having parties submit transactions/updates to the ledger. You can use ``submit`` or ``submitMustFail`` to express what should succeed and what shouldn't.

Commands
--------

A **command** is an instruction to add a transaction to the `ledger <#ledger-daml-ledger>`__.

.. Events
.. ======

.. TODO.

.. _daml-lf:

Daml-LF
=======

When you compile Daml source code into a `.dar file <#dar-file-dalf-file>`__, the underlying format is **Daml-LF**. Daml-LF is similar to Daml, but is stripped down to a core set of features. The relationship between the surface Daml syntax and Daml-LF is loosely similar to that between Java and JVM bytecode.

As a user, you don't need to interact with Daml-LF directly. But internally, it's used for:

- executing Daml code on the Sandbox or on another platform
- sending and receiving values via the Ledger API (using a protocol such as gRPC)
- generating code in other languages for interacting with Daml models (often called “codegen”)

.. include:: ../ctn/tutorials/concepts.inc

General concepts
****************

Ledger, Daml ledger
===================

**Ledger** can refer to a lot of things, but a ledger is essentially the underlying storage mechanism for a running Daml applications: it's where the contracts live. A **Daml ledger** is a ledger that you can store Daml contracts on, because it implements the `ledger API <#ledger-api>`__.

Daml ledgers provide various guarantees about what you can expect from it, all laid out in the :doc:`/concepts/ledger-model/index` page.

When you're developing, you'll use `Sandbox <#sandbox>`__ as your ledger.

.. _trust-domain:

Trust domain
============

A **trust domain** encompasses a part of the system (in particular, a Daml ledger) operated by a single real-world entity. This subsystem may consist of one or more physical nodes. A single physical machine is always assumed to be controlled by exactly one real-world entity.


.. Transaction
.. ===========

.. A transaction is composed of a series of actions.

.. Create (trans)action
.. --------------------

.. Exercise (trans)action
.. ----------------------

.. Fetch (trans)action
.. -------------------

.. Commit
.. ======

.. Privacy, visibility
.. ===================

.. Consistency
.. ===========

.. Conformance
.. ===========
