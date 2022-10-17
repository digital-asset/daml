.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _ledger-api-services:

The Ledger API Services
#######################

The Ledger API is structured as a set of services. The core services are implemented using `gRPC <https://grpc.io/>`__ and `Protobuf <https://developers.google.com/protocol-buffers/>`__, but most applications access this API through the mediation of the language bindings.

This page gives more detail about each of the services in the API, and will be relevant whichever way you're accessing it.

If you want to read low-level detail about each service, see the :doc:`protobuf documentation of the API </app-dev/grpc/proto-docs>`.

Overview
********

The API is structured as two separate data streams:

-  A stream of **commands** TO the ledger that allow an application to submit transactions and change state.
-  A stream of **transactions** and corresponding **events** FROM the ledger that indicate all state changes that have taken place on the ledger.

Commands are the only way an application can cause the state of the ledger to change, and events are the only mechanism to read those changes.

For an application, the most important consequence of these architectural decisions and implementation is that the Ledger API is asynchronous. This means:

-  The outcome of commands is only known some time after they are submitted.
-  The application must deal with successful and erroneous command completions separately from command submission.
-  Ledger state changes are indicated by events received asynchronously from the command submissions that cause them.

The need to handle these issues is a major determinant of application architecture. Understanding the consequences of the API characteristics is important for a successful application design.

For more help understanding these issues so you can build correct, performant and maintainable applications, read the :doc:`application architecture guide </app-dev/app-arch>`.

Glossary
========

- The ledger is a list of ``transactions``. The transaction service returns these.
- A ``transaction`` is a tree of ``actions``, also called ``events``, which are of type ``create``, ``exercise`` or ``archive``. The transaction service can return the whole tree, or a flattened list.
- A ``submission`` is a proposed transaction, consisting of a list of ``commands``, which correspond to the top-level ``actions`` in that transaction.
- A ``completion`` indicates the success or failure of a ``submission``.

.. _ledger-api-submission-services:

Submit Commands to the Ledger
*****************************

.. _command-submission-service:

Command Submission Service
==========================

Use the **command submission service** to submit commands to the ledger. Commands either create a new contract, or exercise a choice on an existing contract.

A call to the command submission service will return as soon as the ledger server has parsed the command, and has either accepted or rejected it. This does not mean the command has been executed, only that the server has looked at the command and decided that its format is acceptable, or has rejected it for syntactic or content reasons.

The on-ledger effect of the command execution will be reported via the `transaction service <#transaction-service>`__, described below. The completion status of the command is reported via the `command completion service <#command-completion-service>`__. Your application should receive completions, correlate them with command submission, and handle errors and failed commands. Alternatively, you can use the `command service <#command-service>`__, which conveniently wraps the command submission and completion services.

.. _change-id:

Change ID
---------

Each intended ledger change is identified by its **change ID**, consisting of the following three components:

- The submitting parties, i.e., the union of :ref:`party <com.daml.ledger.api.v1.Commands.party>` and :ref:`act_as <com.daml.ledger.api.v1.Commands.act_as>`
- the :ref:`application ID <com.daml.ledger.api.v1.Commands.application_id>`
- The :ref:`command ID <com.daml.ledger.api.v1.Commands.command_id>`

Application-specific IDs
------------------------

The following application-specific IDs, all of which are included in completion events, can be set in commands:

- A :ref:`submission ID <com.daml.ledger.api.v1.Commands.submission_id>`, returned to the submitting application only. It may be used to correlate specific submissions to specific completions.
- A :ref:`command ID <com.daml.ledger.api.v1.Commands.command_id>`, returned to the submitting application only; it can be used to correlate commands to completions.
- A :ref:`workflow ID <com.daml.ledger.api.v1.Commands.workflow_id>`, returned as part of the resulting transaction to all applications receiving it. It can be used to track workflows between parties, consisting of several transactions.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.CommandSubmissionService>`.

.. _command-submission-service-deduplication:

Command Deduplication
---------------------

The command submission service deduplicates submitted commands based on their :ref:`change ID <change-id>`.

- Applications can provide a deduplication period for each command. If this parameter is not set, the default maximum deduplication duration is used.
- A command submission is considered a duplicate submission if the Ledger API server is aware of another command within the deduplication period and with the same :ref:`change ID <change-id>`.
- A command resubmission will generate a rejection until the original submission was rejected (i.e. the command failed and resulted in a rejected transaction) or until the effective deduplication period has elapsed since the completion of the original command, whichever comes first.
- Command deduplication is only *guaranteed* to work if all commands are submitted to the same participant. Ledgers are free to perform additional command deduplication across participants. Consult the respective ledger's manual for more details.

For details on how to use command deduplication, see the :doc:`Command Deduplication Guide <command-deduplication>`.

.. _command-completion-service:

Command Completion Service
==========================

Use the **command completion service** to find out the completion status of commands you have submitted.

Completions contain the :ref:`command ID <com.daml.ledger.api.v1.Commands.command_id>` of the completed command, and the completion status of the command. This status indicates failure or success, and your application should use it to update what it knows about commands in flight, and implement any application-specific error recovery.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.CommandCompletionService>`.

.. _command-service:

Command Service
===============

Use the **command service** when you want to submit a command and wait for it to be executed. This service is similar to the command submission service, but also receives completions and waits until it knows whether or not the submitted command has completed. It returns the completion status of the command execution.

You can use either the command or command submission services to submit commands to effect a ledger change. The command service is useful for simple applications, as it handles a basic form of coordination between command submission and completion, correlating submissions with completions, and returning a success or failure status. This allow simple applications to be completely stateless, and alleviates the need for them to track command submissions.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.CommandService>`.

Read From the Ledger
********************

.. _transaction-service:

Transaction Service
===================

Use the **transaction service** to listen to changes in the ledger state, reported via a stream of transactions.

Transactions detail the changes on the ledger, and contains all the events (create, exercise, archive of contracts) that had an effect in that transaction.

Transactions contain a :ref:`transaction ID <com.daml.ledger.api.v1.Transaction.transaction_id>` (assigned by the server), the :ref:`workflow ID <com.daml.ledger.api.v1.Commands.workflow_id>`, the :ref:`command ID <com.daml.ledger.api.v1.Commands.command_id>`, and the events in the transaction.

Subscribe to the transaction service to read events from an arbitrary point on the ledger. This arbitrary point is specified by the ledger offset. This is important when starting or restarting and application, and to work in conjunction with the `active contracts service <#active-contract-service>`__.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.TransactionService>`.

Transaction and transaction Trees
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

.. _transaction-filter:

Transaction Filter
------------------

``TransactionService`` offers transaction subscriptions filtered by templates and interfaces using ``GetTransactions`` calls. A :ref:`transaction filter <com.daml.ledger.api.v1.TransactionFilter>` in ``GetTransactionsRequest``. allows:

- filtering by a party, when the :ref:`inclusive <com.daml.ledger.api.v1.Filters.inclusive>` field is left empty
- filtering by a party and a :ref:`template ID <com.daml.ledger.api.v1.InclusiveFilters.template_ids>`
- filtering by a party and an :ref:`interface ID <com.daml.ledger.api.v1.InterfaceFilter.interface_id>`
- exposing an interface view, when the :ref:`include_interface_view <com.daml.ledger.api.v1.InterfaceFilter.include_interface_view>` is set to ``true``

.. _active-contract-service:

Active Contracts Service
========================

Use the **active contracts service** to obtain a party-specific view of all contracts that are active on the ledger at the time of the request.

The active contracts service returns its response as a stream of batches of the created events that would re-create the state being reported (the size of these batches is left to the ledger implementation). As part of the last message, the offset at which the reported active contract set was valid is included. This offset can be used to subscribe to the "flat transactions" stream to keep a consistent view of the active contract set without querying the active contract service further.

This is most important at application start, if the application needs to synchronize its initial state with a known view of the ledger. Without this service, the only way to do this would be to read the Transaction Stream from the beginning of the ledger, which can be prohibitively expensive with a large ledger.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.ActiveContractsService>`.

Verbosity
---------

See :ref:`verbosity` above.

Transaction Filter
------------------
See :ref:`transaction-filter` above.

.. note::

  The RPCs exposed as part of the transaction and active contracts services make use of offsets.

  An offset is an opaque string of bytes assigned by the participant to each transaction as they are received from the ledger.
  Two offsets returned by the same participant are guaranteed to be lexicographically ordered: while interacting with a single participant, the offset of two transactions can be compared to tell which was committed earlier.
  The state of a ledger (i.e. the set of active contracts) as exposed by the Ledger API is valid at a specific offset, which is why the last message your application receives when calling the ``ActiveContractsService`` is precisely that offset.
  In this way, the client can keep track of the relevant state without needing to invoke the ``ActiveContractsService`` again, by starting to read transactions from the given offset.

  Offsets are also useful to perform crash recovery and failover as documented more in depth in the :ref:`application architecture <dealing-with-failures>` page.

  You can read more about offsets in the `protobuf documentation of the API <../app-dev/grpc/proto-docs.html#ledgeroffset>`__.

.. _ledger-api-utility-services:

Utility Services
****************

.. _party-service:

Party Management Service
========================

Use the **party management service** to allocate parties on the ledger, update party properties local to the participant and retrieve information about allocated parties.

Parties govern on-ledger access control as per :ref:`Daml's privacy model <da-model-privacy>`
and :ref:`authorization rules <da-ledgers-authorization-rules>`.
Applications and their operators are expected to allocate and use parties to manage on-ledger access control as per their business requirements.

For more information, refer to the pages on :doc:`Identity Management</concepts/identity-and-package-management>` and :ref:`the API reference documentation <com.daml.ledger.api.v1.admin.PartyManagementService>`.

.. _user-management-service:

User Management Service
=======================

Use the **user management service** to manage the set of users on a participant node and
their :ref:`access rights <authorization-claims>` to that node's Ledger API services
and as the integration point for your organization's IAM (Identity and Access Management) framework.

In contrast to parties, users are local to a participant node.
The relation between a participant node's users and Daml parties is best understood by analogy to classical databases:
a participant node's users are analogous to database users while Daml parties are analogous to database roles; and further, the rights granted to a user are analogous to the user's assigned database roles.

For more information, consult the :ref:`the API reference documentation <com.daml.ledger.api.v1.admin.UserManagementService>` for how to list, create, update and delete users and their rights.
See the :ref:`UserManagementFeature descriptor <com.daml.ledger.api.v1.UserManagementFeature>` to learn about limits of the user management service, e.g., the maximum number of rights per user.
The feature descriptor can be retrieved using the :ref:`Version service <version-service>`.

With user management enabled you can use both new user-based and old custom Daml authorization tokens.
Read the :doc:`Authorization documentation </app-dev/authorization>` to understand how Ledger API requests are authorized, and how to use user management to dynamically change an application's rights.

User management is available in Canton-enabled drivers and not yet available in the Daml for VMware Blockchain driver.

.. _package-service:

Package Service
===============

Use the **package service** to obtain information about Daml packages available on the ledger.

This is useful for obtaining type and metadata information that allow you to interpret event data in a more useful way.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.PackageService>`.

.. _ledger-identity-service:

Ledger Identity Service (DEPRECATED)
=====================================

Use the **ledger identity service** to get the identity string of the ledger that your application is connected to.

Including identity string is optional for all Ledger API requests.
If you include it, commands with an incorrect identity string will be rejected.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.LedgerIdentityService>`.

.. _ledger-configuration-service:

Ledger Configuration Service
============================

Use the **ledger configuration service** to subscribe to changes in ledger configuration.

This configuration includes the maximum command deduplication period (see `Command Deduplication <#command-submission-service-deduplication>`__ for details).

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.LedgerConfigurationService>`.

.. _version-service:

Version Service
===============

Use the **version service** to retrieve information about the Ledger API version and what optional features are supported by the ledger server.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.VersionService>`.

.. _pruning-service:

Pruning Service
===============

Use the **pruning service** to prune archived contracts and transactions before or at a given offset.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.admin.ParticipantPruningService>`.

.. _metering-report-service:

Metering Report Service
=======================

Use the **metering report service** to retrieve a participant metering report.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.admin.MeteringReportService>`.

.. _ledger-api-testing-services:

Testing Services
****************

**These are only for use for testing with the Sandbox, not for on production ledgers.**

.. _time-service:

Time Service
============

Use the **time service** to obtain the time as known by the ledger server.

For full details, see :ref:`the proto documentation for the service <com.daml.ledger.api.v1.testing.TimeService>`.
