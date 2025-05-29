.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _ledger-api-services:

The gRPC Ledger API Services
############################

.. note::
    MOVE ME INTO THE CANTON REPO

The Ledger API is structured as a set of services. This page gives more detail about each of the services in the API, and will be relevant whichever way you're accessing it.

If you want to read low-level detail about each service, see :subsiteref:`the protobuf documentation of the API <build-reference-lapi-proto-docs>`.

Overview
********

The API is structured as two separate data streams:

-  A stream of **commands** TO the ledger that allow an application to submit transactions and change state.
-  A stream of **updates** and corresponding **events** FROM the ledger that indicate all state changes that have taken place on the ledger.

Commands are the only way an application can cause the state of the ledger to change, and events are the only mechanism to read those changes.

For an application, the most important consequence of these architectural decisions and implementation is that the Ledger API is asynchronous. This means:

-  The outcome of commands is only known some time after they are submitted.
-  The application must deal with successful and erroneous command completions separately from command submission.
-  Ledger state changes are indicated by events received asynchronously from the command submissions that cause them.

The need to handle these issues is a major determinant of application architecture. Understanding the consequences of the API characteristics is important for a successful application design.

For more help understanding these issues so you can build correct, performant and maintainable applications, read the :brokenref:`application architecture guide <build_sdlc_howtos_daml_app_arch_design>`.

Services
********

The gRPC Ledger API exposes the following services:

- Submitting commands to the ledger

  - Use the :ref:`Command Submission Service <command-submission-service>` to submit commands (create a contract or exercise a choice) to the ledger.
  - Use the :ref:`Command Completion Service <command-completion-service>` to track the status of submitted commands.
  - Use the :ref:`Command Service <command-service>` for a convenient service that combines the command submission and completion.
  - Use the :ref:`Interactive Submission Service <interactive-submission-service>` to prepare and submit an externally signed transaction.

- Reading from the ledger

  - Use the :ref:`Update Service <update-service>` to stream committed transactions and the resulting events (choices exercised, and contracts created or archived), and to look up transactions.
  - Use the :ref:`State Service <state-service>` to quickly bootstrap an application with the currently active contracts. It saves you the work to process the ledger from the beginning to obtain its current state.
  - Use the :ref:`Event Query Service <event-query-service>` to obtain a party-specific view of contract events.

- Utility services

  - Use the :ref:`Party Management Service <party-service>` to allocate and find information about parties on the Daml ledger.
  - Use the :ref:`User Management Service <user-management-service>` to manage users and their rights.
  - Use the :ref:`Identity Provider Config Service <identity-provider-config-service>` to define and manage external IDP systems configured to issue tokens for a Participant Node.
  - Use the :ref:`Package Management Service <package-management-service>` to upload packages the Daml ledger.
  - Use the :ref:`Package Service <package-service>` to query the Daml packages deployed to the ledger.
  - Use the :ref:`Version Service <version-service>` to retrieve information about the Ledger API version.
  - Use the :ref:`Pruning Service <pruning-service>` to prune archived contracts and transactions before or at a given offset.

- Testing services (configured for testing only, *not* for production ledgers)

  - Use the :ref:`Time Service <time-service>` to obtain the time as known by the ledger.

Glossary
========

- The ledger is a list of ``updates``.
- An ``update`` can be a Daml ``transaction``, a ``reassignment``, or ``topology transaction`` such as change of permission of Participant to a party.
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

A call to the Command Submission Service returns as soon as the ledger server has parsed the command, and has either accepted or rejected it. This does not mean the command has been executed, only that the server has looked at the command and decided that its format is acceptable, or has rejected it for syntactic or content reasons.

The on-ledger effect of the command execution is reported via the :ref:`Update Service <update-service>`, described below. The completion status of the command is reported via the :ref:`Command Completion Service <command-completion-service>`. Your application should receive completions, correlate them with command submission, and handle errors and failed commands. Alternatively, you can use the :ref:`Command Service <command-service>`, which conveniently wraps the Command Submission and Command Completion Services.

.. _change-ID:

Change ID
---------

Each intended ledger change is identified by its **change ID**, consisting of the following three components:

- The submitting parties, that is :subsiteref:`act_as <com.daml.ledger.api.v2.Commands.act_as>`
- the :subsiteref:`user ID <com.daml.ledger.api.v2.Commands.user_id>`
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

The Command Submission Service deduplicates submitted commands based on their :ref:`change ID <change-ID>`.

- Applications can provide a deduplication period for each command. If this parameter is not set, the default maximum deduplication duration is used.
- A command submission is considered a duplicate submission if the Ledger API server is aware of another command within the deduplication period and with the same :ref:`change ID <change-ID>`.
- A command resubmission will generate a rejection until the original submission was rejected (i.e. the command failed and resulted in a rejected transaction) or until the effective deduplication period has elapsed since the completion of the original command, whichever comes first.
- Command deduplication is only *guaranteed* to work if all commands are submitted to the same Participant Node.

For details on how to use command deduplication, see the :ref:`Command Deduplication Guide <command-deduplication>`.

.. _command-explicit-contract-disclosure:

Explicit contract disclosure
----------------------------

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

.. _interactive-submission-service:

Interactive Submission Service
==============================

Use **Interactive Submission Service** to prepare and submit daml transactions signed with your own signing keys.

Service allows interactive construction of command submissions It contains two endpoints allowing a two-step command submission:
The prepare and execute endpoints allow to submit commands in steps:

- prepare transaction from the supplied list of commands
- inspect proposed transaction and sign it using own key
- submit the prepared transaction

For more information, refer to the :subsiteref:`the API reference documentation <com.daml.ledger.api.v2.interactive.InteractiveSubmissionService>`.

.. _reading-from-the-ledger:

Read From the Ledger
********************

.. _update-service:

Update Service
==============

Use the **Update Service** to listen to changes in the ledger state, reported via a stream of updates.

Updates can contain transactions, reassignments and topology transactions. A transaction in turn can contain all the events (create, exercise, archive of contracts) that had an effect in that transaction.

Transactions contain an :subsiteref:`update ID <com.daml.ledger.api.v2.Transaction.update_id>` (assigned by the server), a :subsiteref:`workflow ID <com.daml.ledger.api.v2.Commands.workflow_id>`, a :subsiteref:`command ID <com.daml.ledger.api.v2.Commands.command_id>`, and the events in the transaction.

Subscribe to the Update Service to read events from an arbitrary point on the ledger. This arbitrary point is specified by the :ref:`offset<ledger-api-offset>`. This is important when starting or restarting an application, and to work in conjunction with the :ref:`State Service <state-service>`.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.UpdateService>`.

Transactions and transaction Trees
----------------------------------

``UpdateService`` offers several different subscriptions. The most commonly used is ``GetUpdates`` configured to deliver ``TRANSACTION_SHAPE_ACS_DELTA``. It returns a stream of changes to the active contract set: contracts created and archived. If you need the entire transactions visible to a set of parties, you can use ``TRANSACTION_SHAPE_LEDGER_EFFECTS`` instead, which returns transactions as trees, represented list of events with descendant information.

.. _event-format:

Event format
------------
``UpdateService`` offers transaction subscriptions filtered by templates and interfaces using ``GetUpdates`` calls. An :subsiteref:`event format <com.daml.ledger.api.v2.EventFormat>` embedded in ``GetUpdatesRequest.update_format.include_transactions`` allows:

- filtering by a party
- filtering by a party and template ID
- filtering by a party and interface ID
- exposing an interface view
- exposing a created event blob to be used for a disclosed contract in command submission

To learn more see :subsiteref:`Ledger API reference <build_reference_ledger_api>`.

.. _verbosity:

Verbosity
---------

The service works in a non-verbose mode by default, which means that some identifiers are omitted:

- Record IDs
- Record field labels
- Variant IDs

You can get these included in requests related to Transactions by setting the ``verbose`` field in the :subsiteref:`event format <com.daml.ledger.api.v2.EventFormat>` message to ``true``.

.. _state-service:

State Service
=============

Use the **State Service** to obtain a party-specific view of all contracts that are active on the ledger at the time of the request.

The State Service returns a stream of the created events that re-creates the state being reported. The state is always requested as of a certain offset. This offset can be used to subscribe to the ``updates`` stream to keep a consistent view of the active contract set without querying the State Service further.

This is most important at application start, if the application needs to synchronize its initial state with a known view of the ledger. Without this service, the only way to do this would be to read the Update Stream from the beginning of the ledger, which can be prohibitively expensive with a large ledger.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.StateService>`.

The :ref:`verbosity` and  :ref:`event-format` are defined in the same manner as for the Update Service.

.. _ledger-api-offset:

Offset
======

The methods exposed as part of the Update and State Services make use of offsets.

An offset describes a specific point in the stream of updates observed by the Participant Node. An offset is meaningful
only in the context of its original Participant Node. Different Participant Nodes associate different offsets to the same
change synchronized over a Synchronizer. Conversely, the same literal participant offset may refer to different changes on
different Participant Nodes.

An offset is also a unique index of the changes which happened on the virtual shared ledger. The order of offsets is
reflected in the order the updates that are visible when subscribing to the Update Service. This ordering is also fully
causal for any specific Synchronizer: for two updates synchronized by the same Synchronizer, the one with a bigger offset
happened after than the one with a smaller offset. This is not true for updates synchronized by different Synchronizers.
Accordingly, the offset order may deviate from the order of the changes on the virtual shared ledger.

.. _event-query-service:

Event Query Service
===================

Use the **event query service** to obtain a party-specific view of contract events.

The gRPC API provides ledger streams to off-ledger components that maintain a queryable state. This service allows you to make simple event queries without off-ledger components like the Participant Query Store.

Using the Event Query Service, you can create, retrieve, and archive events associated with a contract ID. The API returns only those events where at least one of the requesting parties is a stakeholder of the contract. If the contract is still active, the ``archive_event`` is unset.

If no events match the request criteria or the requested events are not visible to the requesting parties, an empty structure is returned. Events associated with consumed contracts are returned until they are pruned.

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

For more information, refer to the :subsiteref:`the API reference documentation <com.daml.ledger.api.v2.admin.PartyManagementService>`.

.. _user-management-service:

User Management Service
=======================

Use the **User Management Service** to manage the set of users on a Participant Node and
their :ref:`access rights <authorization-claims>` to that node's Ledger API services
and as the integration point for your organization's IAM (Identity and Access Management) framework.

While a party represents a single individual with a single set of rights and is universal across Participant Nodes, a user is local to a specific Participant Node. Each user is typically associated with a primary party and is given the right to act as or read as other parties. Every Participant Node maintains its own mapping from its user IDs to the parties that they can act and/or read as. The user IDs are referenced as subjects in the JWT tokens allowing authorization of ledger clients to read as or act as certain parties. The user management system does not limit the number of parties that the user can act or read as.

The relation between a Participant Node's users and Daml parties is best understood by analogy to classical databases: a Participant Node's users are analogous to database users while Daml parties are analogous to database roles. Further, the rights granted to a user are analogous to the user's assigned database roles.

For more information, consult the :subsiteref:`the API reference documentation <com.daml.ledger.api.v2.admin.UserManagementService>` for how to list, create, update, and delete users and their rights.
See the :subsiteref:`UserManagementFeature descriptor <com.daml.ledger.api.v2.UserManagementFeature>` to learn about the limits of the User Management Service, e.g., the maximum number of rights per user.
The feature descriptor can be retrieved using the :ref:`Version Service <version-service>`.

Consult the :ref:`Authorization documentation <authorization>` to understand how Ledger API requests are authorized, and how to use User Management to dynamically change an application's rights.

.. _identity-provider-config-service:

Identity Provider Config Service
================================

Use **Identity Provider Config Service** to define and manage the parameters of an external IDP systems configured to issue tokens for a Participant Node.

The **Identity Provider Config Service** makes it possible for Participant Node administrators to set up and manage additional identity providers at runtime. This allows using access tokens from identity providers unknown at deployment time. When an identity provider is configured, independent IDP administrators can manage their own set of parties and users.

Such parties and users have a matching ``identity_provider_id`` defined and are inaccessible to administrators from other identity providers. A user is only be authenticated if the corresponding JWT token is issued by the appropriate identity provider. Users and parties without ``identity_provider_id`` defined are assumed to be using the default identity provider, which is configured statically when the Participant Node is deployed.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.admin.IdentityProviderConfigService>`.

.. _package-management-service:

Package Management Service
==========================

Use the **Package Management Service** to query the Daml-LF packages supported by the Participant Node and to upload and validate .dar files.

.. _package-service:

Package Service
===============

Use the **Package Service** to obtain information about Daml packages available on the ledger.

This is useful for obtaining type and metadata information that allow you to interpret event data in a more useful way.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.PackageService>`.

.. _version-service:

Version Service
===============

Use the **Version Service** to retrieve information about the Ledger API version and what optional features are supported by the ledger server.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.VersionService>`.

.. _pruning-service:

Pruning Service
===============

Use the **Pruning Service** to prune archived contracts and transactions before or at a given offset.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.admin.ParticipantPruningService>`.

.. _metering-report-service:

Testing Services
****************

**These are only for use for testing, not for on production ledgers.**

.. _time-service:

Time Service
============

Use the **Time Service** to get and set the ledger time. This service is only available if the Canton has been set up to to work in the static time mode which only makes sense in testing.

For full details, see :subsiteref:`the proto documentation for the service <com.daml.ledger.api.v2.testing.TimeService>`.
