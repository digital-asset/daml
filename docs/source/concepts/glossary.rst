.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Glossary of concepts
####################

DAML
****

DAML is a programming language for writing smart contracts.

Contract, contract instance
===========================

Item on a `ledger`. Includes:
- data (parameters)
- roles (`signatory`, `observer`)
- `choices` (and `controllers`)

Active contract, archived contract
----------------------------------

Template
========

Blueprint for creating a `contract`. This is the DAML code you write.

:doc:`/daml/reference/templates`.

Choice
======

Something that a `party` can `exercise` on a `contract`. You choose the consequences, writing code in the choice body.

:doc:`/daml/reference/choices`.

Consuming choice, preconsuming choice, postconsuming choice
------------------------------------------------------------

A consuming `choice` will `archive` the `contract` it is on when `exercised`.

A `choice` marked preconsuming will get `archived` at the start of that `exercise`.

A `choice` marked postconsuming will not be `archived` until the end of the `exercise` choice body.

Nonconsuming choice
--------------------

A nonconsuming choice does NOT `archive` the `contract` it is on when `exercised`, which means the choice can be exercised more than once on the same `contract instance`. 

Disjunction choice, flexible controllers
----------------------------------------

Disjunction choice has more than one `controller`?

Flexible controller means you don't specify the `controller` of the `choice` at `creation` time of the `contract`, but at `exercise` time. 

Party
=====

A party represents a person or legal entity. Parties can `create contracts` and `exercise choices`.

Something about how they work in the `execution engine`.

Signatory
---------

A signatory is a `party` on a `contract instance`. Has to consent to the `creation` of the contract by `authorizing` it. If they don't, contract creation will fail.

:doc:`/daml/reference/templates`.

Observer
--------

An observer is a `party` on a `contract instance`. Being an observer allows them to see that instance and all the information about it. They do NOT have to `consent to` the creation.

:doc:`/daml/reference/templates`.

Controller
----------

A controller is a `party` that is able to `exercise` a particular `choice` on a particular `contract`.

Controller must be at least `observer`, otherwise they can't see the contract to exercise it on. But they don't have to be a `signatory`. Allows the `propose-accept pattern`.

:doc:`/daml/reference/choices`.

Stakeholder
-----------

Not a term used within daml the language, but concept refers to the `signatories` and `observers` collectively: all of the `parties` that are interested in a `contract instance`. 

Maintainer
----------

Maintainer is a `party`. Important for `contract keys`: a key must include a party that is the maintainer, it's how uniqueness of keys is guaranteed.

:doc:`/daml/reference/contract-keys`.

Authorization, signing
======================

The `DAML runtime` checks that every submitted transaction is well-authorized according to the authorization rules of the ledger model https://docs.daml.com/concepts/ledger-model/ledger-integrity.html that guarantee the integrity of the underlying ledger.

A DAML update is the composition of update actions created with one of
  
  * create : (Template c) => c -> Update (ContractId c)
  * exercise : ContractId c -> e -> Update r
  * fetch : (Template c) => ContractId c -> Update c
  * fetchByKey : k -> Update (ContractId c, c)
  * lookupByKey : k -> Update (Optional (ContractId c))

A DAML update is well-authorized if and only if all it's contained update actions are well-authorized. Every operation of the above list has an associated set of parties that need to authorize the operation. The mapping is as follows:

* create -> the (non-empty) set of all signatories of the created contract instance
* exercise -> the (non-empty) set of controllers of the choice
* fetch -> one of the union of signatories and observers of the fetched contract instance
* fetchByKey -> same as fetch
* lookupByKey -> all key maintainers

At runtime the DAML execution engine computes the required authorizing parties from this mapping. It also computes which parties have given authorization to the update in question. A party is giving authorization to an update in one of two ways

1) it is the signatory of the contract that contains the update action
2) it is element of the controllers executing the choice containing the update action

Only if all required parties have given their authorization to an update action, the update action is well-authorized and therefore executed. A missing authorization leads to the abortion of the update action and the failure of the containing transaction.

It is noteworthy, that authorizing parties are always determined only from the local context of a choice in question, that is, its controllers and the contract's signatories. Authorization is never inherited from earlier execution contexts.

The following example shows a DAML contract annotated with required authorizing parties and actually authorizing parties when it is submitted by 'Alice'.

Standard library
================

the standard library is a set of `daml` functions and other stuff that make developing with DAML easier. Basically read the generated docs.

:doc:`/daml/reference/standard-library`. 

Agreement
=========

An agreement is part of a `contract`. Text that explains what the contract represents. Can be used to clarify the legal intent of a contract, but isn't actually evaluated or used at all.

:doc:`/daml/reference/templates`.

Create
======

A create is an action that updates the `ledger`, creating a `contract instance` on it.

Requires `authorization` from all its `signatories`, or the create will fail. Link to `propose-accept` and `multi-party agreement` patterns.

A `party` `submits` a create `command`.

:doc:`/daml/reference/updates`.

Exercise
========

A create is an action that updates the `ledger` by `exercising` a `choice` on a `contract instance` on the ledger. If the choice is `consuming`, the exercise will `archive` the contract instance; if it is `nonconsuming`, the contract instance will stay active.

Requires `authorization` from all of the `controllers` of the choice.

A `party` `submits` an exercise `command`.

:doc:`/daml/reference/updates`.

Scenario
========

A scenario is a way of testing DAML code during development. You can run scenarios inside `DAML Studio`, or write them to be executed on `Sandbox` when it starts up.

They're useful for:

- expressing clearly the intended workflow of your `contracts`
- for making sure that parties can create contracts, observe contracts, and exercise choices (and that they CANNOT create contracts, observe contracts, or exercise choices that they should not be able to)
- acting as DAML unit tests to confirm that everything keeps working correctly

Scenarios emulate a real ledger. You specify a linear sequence of actions that various parties take, and these are evaluated in order, according to the same consistency, authorization, and privacy rules as they would be on a DAML ledger. DAML Studio shows you the resulting `transaction` graph, and (if a scenario fails) what caused it to fail.

Link to :doc:`/daml/testing-scenarios`.

DAMLe, DAML runtime, DAML execution engine
==========================================

The DAML runtime (sometimes also called the DAML execution engine or DAMLe)...

Contract key
============

A contract key is similar to a primary key. Provides a way of uniquely identifying `contract instances` made from a particular `template`.

Key requires a `maintainer`: a simple key would be something like a tuple of text and maintainer, like ``(accountId, bank)``.

:doc:`/daml/reference/contract-keys`.

DAR file, DALF file
===================

A ``.dar`` file is the result of compiling a DAML (module? project? source code?) using the `assistant`.

Underlying format is `DAML-LF`.

You have to upload these files to a `ledger` in order to be able to create contracts from those templates.

.dar contains multiple .dalf files. DALF is output of compiled DAML package or library.

Package, module, library
========================

TODO ask Robin

SDK tools
*********

Assistant
=========

Command-line tool for doing lots of stuff: create daml projects, compile daml projects into `dars`, launch other sdk tools, download new sdk versions.

:doc:`/tools/assistant`.

Studio
======

Plugin for Visual Studio Code. It's the IDE.

:doc:`/daml/daml-studio`.

Sandbox
=======

Lightweight ledger implementation. In normal mode can use for testing; can also run against a postgres back end to get persistence and a more production-like experience.

:doc:`/tools/sandbox`.

Navigator
=========

Tool for exploring what's on the ledger. Can see what parties can see and submit commands. TODO auth.

Navigator GUI
-------------

The version of the Navigator that runs as a web app.

:doc:`/tools/navigator/index`.

Navigator Console
-----------------

The version of the navigator that runs on the command-line.

:doc:`/tools/navigator/console`.

Extractor
=========

Tool for extracting contract data for a single party into a postgres database.

:doc:`/tools/extractor`.

Building applications
*********************

Application, ledger client, integration
=======================================

All terms for an application that sits on top of the ledger and either reads from it or sends commands to it.

Start at :doc:`/app-dev/index`.

Ledger API
==========

Exposed by any DAML ledger. :doc:`/app-dev/services`. Contains the following services.

Command submission service
--------------------------

Use the command submission service to submit `commands` - either create commands or exercise commands - to the `ledger`. :ref:`command-submission-service`.

Command completion service
--------------------------

Use the command completion service to find out whether or not `commands` you have submitted have completed, and what their status was. :ref:`command-completion-service`.

Command service
---------------

Use the command service when you want to submit a `command` and wait for it to be executed. :ref:`command-service`.

Transaction service
-------------------

Use the transaction service to listen to changes in the `ledger`, reported as a stream of `transactions`. :ref:`transaction-service`.

Active contract service
-----------------------

Use the active contract service to obtain a party-specific view of all `contracts` currently `active` on the `ledger`. :ref:`active-contract-service`.

Package service
---------------

Use the package service to obtain information about `DAML packages` available on the `ledger`. :ref:`package-service`.

Ledger identity service
-----------------------

Use the ledger identity service to get the identity string of the `ledger` that your application is connected to. :ref:`ledger-identity-service`.

Ledger configuration service
----------------------------

Use the ledger configuration service to subscribe to changes in `ledger` configuration. :ref:`ledger-configuration-service`.

Ledger API libraries?
====================

The following things wrap the ledger API for more native experience applications development.

Java bindings
-------------

An idiomatic Java library for writing `ledger applications`.

:doc:`/app-dev/bindings-java/index`

Node.js bindings
----------------

An idiomatic JavaScript library for writing `ledger applications`.

:doc:`/app-dev/bindings-js`

Scala bindings
--------------

An idiomatic Scala library for writing `ledger applications`.

:doc:`/app-dev/bindings-java/index`

gRPC API
--------

The low-level ledger API that all of the other bindings use. Written in gRPC. 

:doc:`/app-dev/grpc/index`

Reading from the ledger
=======================

How `applications` get information about the `ledger`. Can't query the ledger, but can subscribe to the transaction stream to get the events, or the more sophisticated active contract service.

Submitting commands, writing to the ledger
==========================================

How `applications` make changes to the `ledger`. Can't just change it: an application submits a command of `transactions` (TODO one or many?). Gets evaluated by the ledger/runtime/something, and will only be accepted if it's valid.

eg might get rejected because the transactions aren't `well-authorized`; because the contract isn't active (maybe someone else archived it); TODO other reasons.

This is echoed in `scenarios`, where you can mock an application by having parties submit transactions/updates to the ledger. Can use ``submit`` or ``submitMustFail`` to express what should succeed and what shouldn't.

TODO what happened to our docs on the read and write path?

Events
======

DAML-LF
=======

When you compile DAML source code into a `.dar file `, the underlying format is DAML-LF. DAML-LF is similar to DAML, but is stripped down to a core set of features. The relationship between the surface DAML syntax and DAML-LF is loosely similar to that between Java and JVM bytecode.

As a user, you don't need to interact with DAML-LF directly. But inside the DAML SDK, it's used for:

- Executing DAML code on the Sandbox or on another platform
- Sending and receiving values via the Ledger API (using a protocol such as gRPC)
- Generating code in other languages for interacting with DAML models (often called “codegen”)

General concepts
****************

Ledger
======

Can refer to a lot of things, but always the underlying storage mechanism for a running DAML applications: it's where the contracts live.

Provides a bunch of guarantees about what you can expect from it, all laid out in the :doc:`/concepts/ledger-model/index`.

When you're developing, you'll use the `sandbox` as your ledger. Also building integrations so you can use others, like (TODO what?). 

If you want to run DAML on a storage mechanism of your choice, you can use the :doc:`/daml-integration-kit/index` to help you do that.

Transaction
===========

A transaction is composed of a series of actions.

Create (trans)action
------------------

Exercise (trans)action
--------------------

Fetch (trans)action
-----------------

Commit
======

Privacy, visibility
===================

Consistency
===========

Conformance
===========

Authorization
=============
