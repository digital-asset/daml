.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

The Ledger API services
#######################


The Ledger API is structured as a set of services. The core services are implemented using `gRPC <https://grpc.io/>`__ and `Protobuf <https://developers.google.com/protocol-buffers/>`__, but most applications will not use this low-level API, instead using the language bindings (e.g. `Java <#java>`__, `Javascript <#javascript>`__).

This page gives more detail about each of the services in the API, and will be relevant whichever way you're accessing it.

If you want to read low-level detail about each service, see the :doc:`protobuf documentation of the API <grpc/proto-docs>`.

Overview
********

The API is structured as two separate data streams:

-  A stream of **commands** TO the ledger that allow an application to cause state changes.
-  A stream of **events** FROM the ledger that indicate all state changes taking place on the ledger.

Commands are the only way an application can cause the state of the ledger to change, and events are the only mechanism to read those changes.

For an application, the most important consequence of these architectural decisions and implementation is that the ledger API is asynchronous. This means:

-  The outcome of commands is only known some time after they are submitted.
-  The application must deal with successful and erroneous command completions separately from command submission.
-  Ledger state changes are indicated by events received asynchronously from the command submission that cause them.

The application must handle these issues, and is a major determinant of application architecture. Understanding the consequences of the API characteristics is important for a successful application design.

For more help understanding these issues so you can build correct, performant and maintainable applications, read the :doc:`application architecture guide </app-dev/app-arch>`.

Submitting commands to the ledger
*********************************

Command submission service
==========================

Use the **command submission service** to submit a command to the ledger. Commands either create a new contract instance, or exercise a choice on an existing contract.

A call to the command submission service will return as soon as the ledger server has parsed the command, and has either accepted or rejected it. This does not mean the command has been executed, only that the server has looked at the command and decided that it's format is acceptable, or has rejected it for syntactical or content reasons.

The on-ledger effect of the command execution will be reported via an event delivered by the `transaction service <#transaction-service>`__, described below. The completion status of the command is reported via the `command completion service <#command-completion-service>`__. Your application should receive completions, correlate them with command submission, and handle errors and failed commands. 

Commands can be labeled with two application-specific IDs, a :ref:`commandId <com.digitalasset.ledger.api.v1.Commands.command_id>`. and a :ref:`workflowId <com.digitalasset.ledger.api.v1.Commands.workflow_id>`, and both are returned in completion events. The ``commandId`` is returned to the submitting application only, and is generally used to implement this correlation between commands and completions. The ``workflowId`` is also returned (via a transaction event) to all applications receiving transactions resulting from a command. This can be used to track commands submitted by other applications.

Command completion service
==========================

Use the **command completion service** to find out the completion status of commands you have submitted.

Completions contain the ``commandId`` of the completed command, and the completion status of the command. This status indicates failure or success, and your application should use it to update its model of commands in flight, and implement any application-specific error recovery.

Command service
===============

Use the **command service** when you want to submit a command and wait for it to be executed. This service is similar to the command submission service, but also receives completions and waits until it knows whether or not the submitted command has completed. It returns the completion status of the command execution.

You can use either the command or command submission services to submit commands to effect a ledger change. The command service is useful for simple applications, as it handles a basic form of coordination between command submission and completion, correlating submissions with completions, and returning a success or failure status. This allow simple applications to be completely stateless, and alleviates the need for them to track command submissions.

Reading from the ledger
***********************

Transaction service
===================

Use the **transaction service** to listen to changes in the ledger state, reported via a stream of transaction events.

Transaction events detail the changes on transaction boundaries - each event denotes a transaction on the ledger, and contains all the update events (create, exercise, archive of contracts) that had an effect in that transaction.

Transaction events contain a :ref:`transactionId <com.digitalasset.ledger.api.v1.Transaction.transaction_id>` (assigned by the server), the ``workflowId``, the ``commandId``, and the events in the transaction.

Transaction events are the primary mechanism by which an application will do its work. Event-driven applications can use them to generate new commands, and state-driven applications will use them to update their state model, by e.g. creating data that represents created contracts.

The transaction service can be initiated to read events from an arbitrary point on the ledger. This is important when starting or restarting and application, and works in conjunction with the `active contract service <#active-contract-service>`__.

Transaction and transaction trees
---------------------------------

``TransactionService`` offers several different subscriptions. The most commonly used is ``GetTransactions``. If you need more details, you can use ``GetTransactionTrees`` instead, which returns transactions as flattened trees, represented as a map of event IDs to events and a list of root event IDs.

.. _verbosity:

Verbosity
---------

The service works in a non-verbose mode by default, which means that some identifiers are omitted:

- Record IDs
- Record field labels
- Variant IDs

You can get these included in requests related to Transactions by setting the ``verbose`` field in message ``GetTransactionsRequest`` or ``GetActiveContractsRequest`` to ``true``.

Active contract service
=======================

Use the **active contract service** to obtain a party-specific view of all the contracts recently active on the ledger.

The active contract service returns the current contract set as a set of created events that would re-create the state being reported, along with the ledger position at which the view of the set was taken.

For state-driven applications, this is most important at application start. They must synchronize their initial state with a known view of the ledger, and without this service, the only way to do this would be to read the Transaction Stream from the beginning of the ledger. This can be prohibitive with a large ledger.

The active contract service overcomes this, by allowing an application to request a snapshot of the ledger, determine the position at which that snapshot was taken, and build its initial state from this view. The application can then begin to receive events via the Transaction Service from the given position, and remain in sync with the ledger by using these to apply updates to this initial state.

Verbosity
---------

See :ref:`verbosity` above.

Utility services
****************

Package service
===============

Use the **package service** to obtain information about DAML programs and packages loaded into the server.

This is useful for obtaining type and metadata information that allow you to interpret event data in a more useful way.

Ledger identity service
=======================

Use the **ledger identity service** to get the identity string of the ledger that it is connected to.

You need to include this identity string when submitting commands. Commands with an incorrect identity string are rejected.

Ledger configuration service
============================

Use the **ledger configuration service** to subscribe to changes in ledger configuration.

This configuration includes maximum and minimum values for the difference in Ledger Effective Time and Maximum Record Time (see `Time Service <#time-service>`__ for details of these).

Time service
============

Use the **time service** to obtain the time as known by the ledger server.

This is important because you have to include two timestamps when you submit a command - the :ref:`Ledger Effective Time (LET) <com.digitalasset.ledger.api.v1.Commands.ledger_effective_time>`, and the :ref:`Maximum Record Time (MRT) <com.digitalasset.ledger.api.v1.Commands.maximum_record_time>`. For the command to be accepted, LET must be greater than the current ledger time.

MRT is used in the detection of lost commands.

Reset service
=============

.. ::note

   This is a sandbox feature and not available on production ledgers.

Use the **reset service** to reset the ledger state, as a quicker alternative to restarting the whole ledger application. This is a sandbox feature 

This resets all state in the ledger, *including the ledger ID*, so clients will have to re-fetch the ledger ID from the identity service after hitting this endpoint.

Services diagram
****************

.. image:: ./images/services.svg
