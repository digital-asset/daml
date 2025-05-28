.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _ledger-api-services:

The gRPC Ledger API Services
############################

.. note::
    MOVE ME INTO THE CANTON REPO

The Ledger API is structured as a set of services. The core services are implemented using `gRPC <https://grpc.io/>`__ and `Protobuf <https://developers.google.com/protocol-buffers/>`__, but most applications access this API through the mediation of the language bindings.

This page gives more detail about each of the services in the API, and will be relevant whichever way you're accessing it.

If you want to read low-level detail about each service, see the protobuf documentation of the API </sdk/reference/grpc/proto-docs> (TODO FIX BUILD SCRIPT AS THE DOCUMENTATION IS NOT GENERATED).

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

For more help understanding these issues so you can build correct, performant and maintainable applications, read the :brokenref:`application architecture guide </app-dev/app-arch>`.

Glossary
========

- The ledger is a list of ``transactions``. The Update Service returns these.
- A ``transaction`` is a tree of ``actions``, also called ``events``, which are of type ``create``, ``exercise`` or ``archive``. The Update Service can return the whole tree, or a flattened list.
- A ``submission`` is a proposed transaction, consisting of a list of ``commands``, which correspond to the top-level ``actions`` in that transaction.
- A ``completion`` indicates the success or failure of a ``submission``.

.. _ledger-api-submission-services:

Submit Commands to the Ledger
*****************************

.. _command-submission-service:

Command Submission Service
==========================

Use the **Command Submission Service** to submit commands to the ledger. Commands either create a new contract, or exercise a choice on an existing contract.

A call to the Command Submission Service will return as soon as the ledger server has parsed the command, and has either accepted or rejected it. This does not mean the command has been executed, only that the server has looked at the command and decided that its format is acceptable, or has rejected it for syntactic or content reasons.

The on-ledger effect of the command execution will be reported via the `Update Service <#update-service>`__, described below. The completion status of the command is reported via the `Command Completion Service <#command-completion-service>`__. Your application should receive completions, correlate them with command submission, and handle errors and failed commands. Alternatively, you can use the `Command Service <#command-service>`__, which conveniently wraps the command submission and completion services.

.. _change-id:

Change ID
---------

Each intended ledger change is identified by its **change ID**, consisting of the following three components:

- The submitting parties, i.e., the union of :brokenref:`party <com.daml.ledger.api.v1.Commands.party>` and :brokenref:`act_as <com.daml.ledger.api.v1.Commands.act_as>`
- the :brokenref:`application ID <com.daml.ledger.api.v1.Commands.application_id>`
- The :subsiteref:`command ID <com.daml.ledger.api.v2.Commands.command_id>`

Application-specific IDs
------------------------

The following application-specific IDs, all of which are included in completion events, can be set in commands:

- A :subsiteref:`submission ID <com.daml.ledger.api.v2.Commands.submission_id>`, returned to the submitting application only. It may be used to correlate specific submissions to specific completions.
- A :subsiteref:`command ID <com.daml.ledger.api.v2.Commands.command_id>`, returned to the submitting application only; it can be used to correlate commands to completions.
- A :subsiteref:`workflow ID <com.daml.ledger.api.v2.Commands.workflow_id>`, returned as part of the resulting transaction to all applications receiving it. It can be used to track workflows between parties, consisting of several transactions.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.CommandSubmissionService>`.

.. _command-submission-service-deduplication:

Command Deduplication
---------------------

The Command Submission Service deduplicates submitted commands based on their :ref:`change ID <change-id>`.

- Applications can provide a deduplication period for each command. If this parameter is not set, the default maximum deduplication duration is used.
- A command submission is considered a duplicate submission if the Ledger API server is aware of another command within the deduplication period and with the same :ref:`change ID <change-id>`.
- A command resubmission will generate a rejection until the original submission was rejected (i.e. the command failed and resulted in a rejected transaction) or until the effective deduplication period has elapsed since the completion of the original command, whichever comes first.
- Command deduplication is only *guaranteed* to work if all commands are submitted to the same participant. Ledgers are free to perform additional command deduplication across participants. Consult the respective ledger's manual for more details.

For details on how to use command deduplication, see the :ref:`Command Deduplication Guide <command-deduplication>`.

.. _command-explicit-contract-disclosure:

Explicit contract disclosure (experimental)
-------------------------------------------

Starting with Canton 2.7, Ledger API clients can use explicit contract disclosure to submit commands with attached
disclosed contracts received from third parties. For more details,
see :ref:`Explicit contract disclosure <explicit-contract-disclosure>`.

.. _command-completion-service:

Command Completion Service
==========================

Use the **Command Completion Service** to find out the completion status of commands you have submitted.

Completions contain the :subsiteref:`command ID <com.daml.ledger.api.v2.Commands.command_id>` of the completed command, and the completion status of the command. This status indicates failure or success, and your application should use it to update what it knows about commands in flight, and implement any application-specific error recovery.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.CommandCompletionService>`.

.. _command-service:

Command Service
===============

Use the **Command Service** when you want to submit a command and wait for it to be executed. This service is similar to the Command Submission Service, but also receives completions and waits until it knows whether or not the submitted command has completed. It returns the completion status of the command execution.

You can use either the command or command submission services to submit commands to effect a ledger change. The Command Service is useful for simple applications, as it handles a basic form of coordination between command submission and completion, correlating submissions with completions, and returning a success or failure status. This allow simple applications to be completely stateless, and alleviates the need for them to track command submissions.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.CommandService>`.

Read From the Ledger
********************

.. _update-service:

Update Service
==============

Use the **Update Service** to listen to changes in the ledger state, reported via a stream of transactions.

Transactions detail the changes on the ledger, and contains all the events (create, exercise, archive of contracts) that had an effect in that transaction.

Transactions contain a :brokenref:`transaction ID <com.daml.ledger.api.v1.Transaction.transaction_id>` (assigned by the server), the :brokenref:`workflow ID <com.daml.ledger.api.v1.Commands.workflow_id>`, the :brokenref:`command ID <com.daml.ledger.api.v1.Commands.command_id>`, and the events in the transaction.

Subscribe to the Update Service to read events from an arbitrary point on the ledger. This arbitrary point is specified by the ledger offset. This is important when starting or restarting and application, and to work in conjunction with the `State Service <#state-service>`__.

For full details, see :brokenref:`the proto documentation for the service <com.daml.ledger.api.v2.UpdateService>`.

Transactions and transaction Trees
----------------------------------

``UpdateService`` offers several different subscriptions. The most commonly used is ``GetUpdates``. If you need more details, you can use ``GetUpdateTrees`` instead, which returns transactions as flattened trees, represented as a map of event IDs to events and a list of root event IDs.

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
``UpdateService`` offers transaction subscriptions filtered by templates and interfaces using ``GetUpdates`` calls. A :subsiteref:`transaction filter <com.daml.ledger.api.v2.TransactionFilter>` in ``GetUpdatesRequest`` allows:

- filtering by a party, when the :brokenref:`inclusive <com.daml.ledger.api.v1.Filters.inclusive>` field is left empty
- filtering by a party and :brokenref:`template ID <com.daml.ledger.api.v1.InclusiveFilters.template_filters>`
- filtering by a party and :brokenref:`interface ID <com.daml.ledger.api.v1.InclusiveFilters.interface_filters>`
- exposing an interface view, when the :subsiteref:`include_interface_view <com.daml.ledger.api.v2.InterfaceFilter.include_interface_view>` is set to ``true``
- exposing a created event blob to be used for a disclosed contract in command submission when ``include_created_event_blob`` is set to ``true`` in either :subsiteref:`TemplateFilter <com.daml.ledger.api.v2.TemplateFilter>` or :subsiteref:`InterfaceFilter <com.daml.ledger.api.v2.InterfaceFilter>`

.. note::

  The :brokenref:`template_ids <com.daml.ledger.api.v1.InclusiveFilters.template_ids>` field is deprecated as of Canton 2.8.0 and will be removed in future releases. Use :brokenref:`template_filter <com.daml.ledger.api.v1.InclusiveFilters.template_filters>` instead.

.. _active-contract-service:

State Service
=============

Use the **State Service** to obtain a party-specific view of all contracts that are active on the ledger at the time of the request.

The State Service returns its response as a stream of batches of the created events that would re-create the state being reported (the size of these batches is left to the ledger implementation). As part of the last message, the offset at which the reported active contract set was valid is included. This offset can be used to subscribe to the "flat transactions" stream to keep a consistent view of the active contract set without querying the State Service further.

This is most important at application start, if the application needs to synchronize its initial state with a known view of the ledger. Without this service, the only way to do this would be to read the Transaction Stream from the beginning of the ledger, which can be prohibitively expensive with a large ledger.

For full details, see :brokenref:`the proto documentation for the service <com.daml.ledger.api.v2.StateService>`.

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
  The state of a ledger (i.e. the set of active contracts) as exposed by the Ledger API is valid at a specific offset, which is why the last message your application receives when calling the ``StateService`` is precisely that offset.
  In this way, the client can keep track of the relevant state without needing to invoke the ``StateService`` again, by starting to read transactions from the given offset.

  Offsets are also useful to perform crash recovery and failover as documented more in depth in the :brokenref:`application architecture <dealing-with-failures>` page.

  You can read more about offsets in the `protobuf documentation of the API <../app-dev/grpc/proto-docs.html#ledgeroffset>`__.

.. event-query-service:

Event Query Service
===================

Use the **event query service** to obtain a party-specific view of contract events.

The gRPC API provides ledger streams to off-ledger components that maintain a queryable state. This service allows you to make simple event queries without off-ledger components like the JSON Ledger API.

Using the Event Query Service, you can create, retrieve, and archive events associated with a contract ID or contract key. The API returns only those events where at least one of the requesting parties is a stakeholder of the contract. If the contract is still active, the ``archive_event`` is unset.

Contract keys can be used by multiple contracts over time. The latest contract events are returned first. To access earlier contract key events, use the ``continuation_token`` returned in the ``GetEventsByContractKeyResponse`` in a subsequent ``GetEventsByContractKeyRequest``.

If no events match the request criteria or the requested events are not visible to the requesting parties, an empty structure is returned. Events associated with consumed contracts are returned until they are pruned.

.. note::

  When querying by contract key, the key value must be structured in the same way as the key returned in the create event.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.EventQueryService>`.

.. _ledger-api-utility-services:

Utility Services
****************

.. _party-service:

Party Management Service
========================

Use the **Party Management Service** to allocate parties on the ledger, update party properties local to the participant and retrieve information about allocated parties.

Parties govern on-ledger access control as per :externalref:`Daml's privacy model <da-model-privacy>`
and :externalref:`authorization rules <da-ledgers-authorization-rules>`.
Applications and their operators are expected to allocate and use parties to manage on-ledger access control as per their business requirements.

For more information, refer to the pages on :brokenref:`Identity Management</concepts/identity-and-package-management>` and :subsiteref:`the API reference documentation <com.daml.ledger.api.v2.admin.PartyManagementService>`.

.. _user-management-service:

User Management Service
=======================

Use the **User Management Service** to manage the set of users on a participant node and
their :ref:`access rights <authorization-claims>` to that node's Ledger API services
and as the integration point for your organization's IAM (Identity and Access Management) framework.

Daml 2.0 introduced the concept of the user in Daml. While a party represents a single individual with a single set of rights and is universal across participant nodes, a user is local to a specific participant node. Each user is typically associated with a primary party and is given the right to act as or read as other parties. Every participant node will maintain its own mapping from its user ids to the parties that they can act and/or read as. Also, when used, the user's ids will serve as application ids. Thus, participant users can be used to manage the permissions of Daml applications (i.e. to authorize applications to read as or act as certain parties). Unlike a JWT token-based system, the user management system does not limit the number of parties that the user can act or read as.

The relation between a participant node's users and Daml parties is best understood by analogy to classical databases: a participant node's users are analogous to database users while Daml parties are analogous to database roles. Further, the rights granted to a user are analogous to the user's assigned database roles.

For more information, consult the :subsiteref:`the API reference documentation <com.daml.ledger.api.v2.admin.UserManagementService>` for how to list, create, update, and delete users and their rights.
See the :subsiteref:`UserManagementFeature descriptor <com.daml.ledger.api.v2.UserManagementFeature>` to learn about the limits of the User Management Service, e.g., the maximum number of rights per user.
The feature descriptor can be retrieved using the :ref:`Version service <version-service>`.

With user management enabled you can use both new user-based and old custom Daml authorization tokens.
Consult the :ref:`Authorization documentation <authorization>` to understand how Ledger API requests are authorized, and how to use user management to dynamically change an application's rights.

User management is available in Canton-enabled drivers and not yet available in the Daml for VMware Blockchain driver.


.. _identity-provider-config-service:

Identity Provider Config Service
================================

Use **identity provider config service** to define and manage the parameters of an external IDP systems configured to issue tokens for a participant node.

The **identity provider config service** makes it possible for participant node administrators to set up and manage additional identity providers at runtime. This allows using access tokens from identity providers unknown at deployment time. When an identity provider is configured, independent IDP administrators can manage their own set of parties and users.

Such parties and users have a matching identity_provider_id defined and are inaccessible to administrators from other identity providers. A user will only be authenticated if the corresponding JWT token is issued by the appropriate identity provider. Users and parties without identity_provider_id defined are assumed to be using the default identity provider, which is configured statically when the participant node is deployed.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.admin.IdentityProviderConfigService>`.

.. _package-service:

Package Service
===============

Use the **Package Service** to obtain information about Daml packages available on the ledger.

This is useful for obtaining type and metadata information that allow you to interpret event data in a more useful way.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.PackageService>`.

.. _ledger-identity-service:

Version Service
===============

Use the **Version Service** to retrieve information about the Ledger API version and what optional features are supported by the ledger server.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.VersionService>`.

.. _pruning-service:

Pruning Service
===============

Use the **pruning service** to prune archived contracts and transactions before or at a given offset.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.admin.ParticipantPruningService>`.

.. _metering-report-service:

Testing Services
****************

**These are only for use for testing with the Sandbox, not for on production ledgers.**

.. _time-service:

Time Service
============

Use the **Time Service** to obtain the time as known by the ledger server.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.testing.TimeService>`.
