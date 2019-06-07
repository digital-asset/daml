.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Glossary of concepts
####################

DAML
****

**DAML** is a programming language for writing `smart contracts`, that you can use to build an application based on a `ledger` . You can run DAML contracts on many different `ledgers`.

Contract, contract instance
===========================

A **contract** is an item on a `ledger`. They are created from blueprints called `templates`, and include:

- data (parameters)
- roles (`signatory`, `observer`)
- `choices` (and `controllers`)

Contracts are immutable: once they are created on the ledger, the information in the contract cannot be changed. The only thing that can happen to it is that the contract can be `archived`.

They're sometimes referred to as a **contract instance** to make clear that this is an instantiated contract, as opposed to a `template`.

Active contract, archived contract
----------------------------------

When a `contract` is created on a `ledger`, it becomes **active**. But that doesn't mean it will stay active forever: it can be **archived**. This can happen:

- if the `signatories` of the contract decide to archive it
- if a `consuming choice` is exercised on the contract

Once the contract is archived, it is no longer valid, and `choices` on the contract can no longer be exercised.

Template
========

A **template** is a blueprint for creating a `contract`. This is the DAML code you write.

For full documentation on what can be in a template, see :doc:`/daml/reference/templates`.

Choice
======

A **choice** is something that a `party` can `exercise` on a `contract`. You write code in the choice body that specifies what happens when the choice is exercised: for example, it could create a new contract.

Choices give you a way to transform the data in a contract: while the contract itself is immutable, you can write a choice that `archives` the contract and creates a new version of it with updated data.

A choice can only be exercised by its `controller`. Within the choice body, you have the `authorization` of all of the contract's `signatories`.

For full documentation on choices, see :doc:`/daml/reference/choices`.

Consuming choice, preconsuming choice, postconsuming choice
------------------------------------------------------------

A **consuming choice** means that, when the choices is exercised, the `contract` it is gets `archived`. The alternative is a `nonconsuming choice`.

A `choice` marked **preconsuming** will get `archived` at the start of that `exercise`.

A `choice` marked **postconsuming** will not be `archived` until the end of the `exercise` choice body.

Nonconsuming choice
--------------------

A **nonconsuming choice** does NOT `archive` the `contract` it is on when `exercised`. This means the choice can be exercised more than once on the same `contract instance`. 

Disjunction choice, flexible controllers
----------------------------------------

A **disjunction choice** has more than one `controller`.

If a contract uses **flexible controllers**, this means you don't specify the `controller` of the `choice` at `creation` time of the `contract`, but at `exercise` time.

Party
=====

A **party** represents a person or legal entity. Parties can `create contracts` and `exercise choices`.

`Signatories`, `observers`, `controllers`, and `maintainers` all must be parties, represented by the ``Party`` data type in DAML.

.. Something about how they work in the `execution engine`.

Signatory
---------

A **signatory** is a `party` on a `contract instance`. The signatories MUST consent to the `creation` of the contract by `authorizing` it: if they don't, contract creation will fail.

For documentation on signatories, see :doc:`/daml/reference/templates`.

Observer
--------

An **observer** is a `party` on a `contract instance`. Being an observer allows them to see that instance and all the information about it. They do NOT have to `consent to` the creation.

For documentation on observers, see :doc:`/daml/reference/templates`.

Controller
----------

A **controller** is a `party` that is able to `exercise` a particular `choice` on a particular `contract`.

Controllers must be at least `observer`, otherwise they can't see the contract to exercise it on. But they don't have to be a `signatory`. Allows the :doc:`propose-accept pattern </daml/patterns/initaccept>`.

Stakeholder
-----------

**Stakeholder** is not a term used within the DAML language, but the concept refers to the `signatories` and `observers` collectively. That is, it means all of the `parties` that are interested in a `contract instance`. 

Maintainer
----------

The **maintainer** is a `party` that is part of a `contract key`. They must always be a `signatory` on the `contract` that they maintain the key for.

It's not possible for keys to be globally unique, because there is no party that will necessarily know about every contract. However, by including a party as part of the key, this ensures that the maintainer *will* know about all of the contracts, and so can guarantee the uniqueness of the keys that they know about.

For documentation on contract keys, see :doc:`/daml/reference/contract-keys`.

Authorization, signing
======================

The `DAML runtime` checks that every submitted transaction is **well-authorized**, according to the :doc:`authorization rules of the ledger model </concepts/ledger-model/ledger-integrity>`, which guarantee the integrity of the underlying ledger.

A DAML update is the composition of update actions created with one of the items in the table below. A DAML update is well-authorized when **all** its contained update actions are well-authorized. Each operation has an associated set of parties that need to authorize it:

.. list-table:: Updates and required authorization
   :header-rows: 1

   * - Update action
     - Type
     - Authorization
   * - ``create``
     - ``(Template c) => c -> Update (ContractId c)``
     - All signatories of the created contract instance
   * - ``exercise``
     - ``ContractId c -> e -> Update r``
     - All controllers of the choice
   * - ``fetch``
     - ``ContractId c -> e -> Update r``
     - One of the union of signatories and observers of the fetched contract instance
   * - ``fetchByKey``
     - ``k -> Update (ContractId c, c)``
     - Same as ``fetch``
   * - ``lookupByKey``
     - ``k -> Update (Optional (ContractId c))``
     - All key maintainers

At runtime, the DAML execution engine computes the required authorizing parties from this mapping. It also computes which parties have given authorization to the update in question. A party is giving authorization to an update in one of two ways:

- It is the signatory of the contract that contains the update action.
- It is element of the controllers executing the choice containing the update action.

Only if all required parties have given their authorization to an update action, the update action is well-authorized and therefore executed. A missing authorization leads to the abortion of the update action and the failure of the containing transaction.

It is noteworthy, that authorizing parties are always determined only from the local context of a choice in question, that is, its controllers and the contract's signatories. Authorization is never inherited from earlier execution contexts.

Standard library
================

The **DAML standard library** is a set of `DAML` functions, classes and more that make developing with DAML easier.

For documentation, see :doc:`/daml/reference/standard-library`. 

Agreement
=========

An **agreement** is part of a `contract`. It is text that explains what the contract represents.

It can be used to clarify the legal intent of a contract, but this text isn't evaulated programmatically. 

See :doc:`/daml/reference/templates`.

Create
======

A **create** is an update that creates a `contract instance` on it the `ledger`.

Contract creation requires `authorization` from all its `signatories`, or the create will fail. For how to get authorization, see the :doc:`propose-accept </daml/patterns/initaccept>` and :doc:`multi-party agreement </daml/patterns/multiparty-agreement>` patterns.

A `party` `submits` a create `command`.

See :doc:`/daml/reference/updates`.

Exercise
========

An **exercise** is an action that `exercises` a `choice` on a `contract instance` on the `ledger`. If the choice is `consuming`, the exercise will `archive` the contract instance; if it is `nonconsuming`, the contract instance will stay active.

Exercising a choice requires `authorization` from all of the `controllers` of the choice.

A `party` `submits` an exercise `command`.

See :doc:`/daml/reference/updates`.

Scenario
========

A **scenario** is a way of testing DAML code during development. You can run scenarios inside `DAML Studio`, or write them to be executed on `Sandbox` when it starts up.

They're useful for:

- expressing clearly the intended workflow of your `contracts`
- for making sure that parties can create contracts, observe contracts, and exercise choices (and that they CANNOT create contracts, observe contracts, or exercise choices that they should not be able to)
- acting as DAML unit tests to confirm that everything keeps working correctly

Scenarios emulate a real ledger. You specify a linear sequence of actions that various parties take, and these are evaluated in order, according to the same consistency, authorization, and privacy rules as they would be on a DAML ledger. DAML Studio shows you the resulting `transaction` graph, and (if a scenario fails) what caused it to fail.

See :doc:`/daml/testing-scenarios`.

.. DAMLe, DAML runtime, DAML execution engine
.. ==========================================

.. The **DAML runtime** (sometimes also called the DAML execution engine or DAMLe)...

Contract key
============

A **contract key** allows you to uniquely identify a `contract instance` of a particular `template`, similarly to a primary key in a database table.

A contract key requires a `maintainer`: a simple key would be something like a tuple of text and maintainer, like ``(accountId, bank)``.

See :doc:`/daml/reference/contract-keys`.

DAR file, DALF file
===================

A ``.dar`` file is the result of compiling DAML using the `assistant`. Its underlying format is `DAML-LF`.

You upload ``.dar`` files to a `ledger` in order to be able to create contracts from the templates in that file.

A ``.dar`` contains multiple ``.dalf`` files. A ``.dalf`` file is the output of a compiled DAML package or library.

.. Package, module, library
.. ========================

.. TODO ask Robin

SDK tools
*********

Assistant
=========

**DAML Assistant** is a command-line tool for many tasks related to DAML. Using it, you can create DAML projects, compile DAML projects into `.dar files`, launch other SDK tools, and download new SDK versions.

See :doc:`/tools/assistant`.

Studio
======

**DAML Studio** is a plugin for Visual Studio Code, and is the IDE for writing DAML code.

See :doc:`/daml/daml-studio`.

Sandbox
=======

**Sandbox** is a lightweight ledger implementation. In its normal mode, you can use it for testing.

You can also run the Sandbox connected to a PostgreSQL back end, which gives you persistence and a more production-like experience.

See :doc:`/tools/sandbox`.

Navigator
=========

**Navigator** is a tool for exploring what's on the ledger. You can use it to see what contracts can be seen by different parties, and submit commands on behalf of those parties.

Navigator GUI
-------------

This is the version of Navigator that runs as a web app.

See :doc:`/tools/navigator/index`.

Navigator Console
-----------------

This is the version of Navigator that runs on the command-line. It has similar functionality to the GUI.

See :doc:`/tools/navigator/console`.

Extractor
=========

**Extractor** is a tool for extracting contract data for a single party into a PostgreSQL database.

See :doc:`/tools/extractor`.

Building applications
*********************

Application, ledger client, integration
=======================================

**Application**, **ledger client** and **integration** are all terms for an application that sits on top of the `ledger`. These usually read from the ledger, send commands to the ledger, or both.

There's a lot of information available about application development, starting with the :doc:`/app-dev/index` page.

Ledger API
==========

The **Ledger API** is an APi that's exposed by any `DAML ledger`. It includes  the following :doc:`services </app-dev/services>`.

Command submission service
--------------------------

Use the **command submission service** to submit `commands` - either create commands or exercise commands - to the `ledger`. See :ref:`command-submission-service`.

Command completion service
--------------------------

Use the **command completion service** to find out whether or not `commands` you have submitted have completed, and what their status was. See :ref:`command-completion-service`.

Command service
---------------

Use the **command service** when you want to submit a `command` and wait for it to be executed. See :ref:`command-service`.

Transaction service
-------------------

Use the **transaction service** to listen to changes in the `ledger`, reported as a stream of `transactions`. See :ref:`transaction-service`.

Active contract service
-----------------------

Use the **active contract service** to obtain a party-specific view of all `contracts` currently `active` on the `ledger`. See :ref:`active-contract-service`.

Package service
---------------

Use the **package service** to obtain information about `DAML packages` available on the `ledger`. See :ref:`package-service`.

Ledger identity service
-----------------------

Use the **ledger identity service** to get the identity string of the `ledger` that your application is connected to. See :ref:`ledger-identity-service`.

Ledger configuration service
----------------------------

Use the **ledger configuration service** to subscribe to changes in `ledger` configuration. See :ref:`ledger-configuration-service`.

Ledger API libraries
====================

The following libraries wrap the `ledger API` for more native experience applications development.

Java bindings
-------------

An idiomatic Java library for writing `ledger applications`. See :doc:`/app-dev/bindings-java/index`.

Node.js bindings
----------------

An idiomatic JavaScript library for writing `ledger applications`. See :doc:`/app-dev/bindings-js`.

Scala bindings
--------------

An idiomatic Scala library for writing `ledger applications`. See :doc:`/app-dev/bindings-java/index`.

gRPC API
--------

The low-level ledger API that all of the other bindings use. Written in gRPC. See :doc:`/app-dev/grpc/index`.

Reading from the ledger
=======================

`Applications` get information about the `ledger` by **reading** from it. You can't query the ledger, but you can subscribe to the transaction stream to get the events, or the more sophisticated active contract service.

Submitting commands, writing to the ledger
==========================================

`Applications` make changes to the `ledger` by **submitting commands**. You can't change it directly: an application submits a command of `transactions`. The command gets evaluated by the `runtime`, and will only be accepted if it's valid.

For example, a command might get rejected because the transactions aren't `well-authorized`; because the contract isn't `active` (perhaps someone else archived it); or for other reasons.

This is echoed in `scenarios`, where you can mock an application by having parties submit transactions/updates to the ledger. You can use ``submit`` or ``submitMustFail`` to express what should succeed and what shouldn't.

.. Events
.. ======

.. TODO.

DAML-LF
=======

When you compile DAML source code into a `.dar file `, the underlying format is **DAML-LF**. DAML-LF is similar to DAML, but is stripped down to a core set of features. The relationship between the surface DAML syntax and DAML-LF is loosely similar to that between Java and JVM bytecode.

As a user, you don't need to interact with DAML-LF directly. But inside the DAML SDK, it's used for:

- executing DAML code on the Sandbox or on another platform
- sending and receiving values via the Ledger API (using a protocol such as gRPC)
- generating code in other languages for interacting with DAML models (often called “codegen”)

General concepts
****************

Ledger, DAML ledger
===================

**Ledger** can refer to a lot of things, but a ledger is essentially the underlying storage mechanism for a running DAML applications: it's where the contracts live. A **DAML ledger** is a ledger that you can store DAML contracts on, because it implements the `ledger API`.

DAML ledgers provide various guarantees about what you can expect from it, all laid out in the :doc:`/concepts/ledger-model/index` page.

When you're developing, you'll use the `sandbox` as your ledger.

If you want to run DAML on a storage mechanism of your choice, you can use the :doc:`/daml-integration-kit/index` to help you do that.

Transaction
===========

A transaction is composed of a series of actions.

Create (trans)action
--------------------

Exercise (trans)action
----------------------

Fetch (trans)action
-------------------

Commit
======

Privacy, visibility
===================

Consistency
===========

Conformance
===========

