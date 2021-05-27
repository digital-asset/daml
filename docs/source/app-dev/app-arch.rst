.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _recommended-architecture:

Application architecture
########################

This section describes our recommended design of a full-stack Daml application.

.. image:: ./recommended_architecture.svg

The above image shows the recommended architecture. Of course there are many ways how you can change
the architecture and technology stack to fit your needs, which we'll mention in the corresponding
sections. Note that the Participant Node is integrated into the Daml drivers in some cases rather
than being part of the Application Backend. See :doc:`/support/overview` for more details.

To get started quickly with the recommended application architecture, generate a new project using the ``create-daml-app`` template:

.. code-block:: bash

  daml new --template=create-daml-app my-project-name

``create-daml-app`` is a small, but fully functional demo application implementing the recommended
architecture, providing you with an excellent starting point for your own application. It showcases

- using Daml React libraries
- quick iteration against the :ref:`Daml Sandbox <sandbox-manual>`.
- authorization
- deploying your application in the cloud as a Docker container

Backend
~~~~~~~
The backend for your application can be any Daml ledger implementation running your DAR
(:ref:`Daml Archive <dar-file-dalf-file>`) file.

We recommend using the :ref:`Daml JSON API <json-api>` as an interface to your frontend. It is
served by the HTTP JSON API server connected to the ledger API server. It provides simple HTTP
endpoints to interact with the ledger via GET/POST requests. However, if you prefer, you can also
use the :ref:`gRPC Ledger API <grpc>` directly.

When you use the ``create-daml-app`` template application, you can start a Daml Sandbox together
with a JSON API server by running

.. code-block:: bash

  daml start --start-navigator=no

in the root of the project. Daml Sandbox exposes the same Daml Ledger API a Participant Node would
expose without requiring a fully-fledged Daml network to back the application. Once your
application matures and becomes ready for production, the ``daml deploy`` command helps you deploy
your frontend and Daml artifacts of your project to a production Daml network. See
:ref:`Deploying to Daml Ledgers <deploy-ref_overview>` for an in depth manual for specific ledgers.

Frontend
~~~~~~~~

We recommended building your frontend with the `React <https://reactjs.org>`_ framework. However,
you can choose virtually any language for your frontend and interact with the ledger via
:ref:`HTTP JSON <json-api>` endpoints. In addition, we provide support libraries for
:ref:`Java <java-bindings>` and :ref:`Scala <scala-bindings>` and you can also interact with the
:ref:`gRPC Ledger API <grpc>` directly.


We provide two libraries to build your React frontend for a Daml application.

+--------------------------------------------------------------+--------------------------------------------------------------------------+
| Name                                                         | Summary                                                                  |
+==============================================================+==========================================================================+
| `@daml/react <https://www.npmjs.com/package/@daml/react>`_   | React hooks to query/create/exercise Daml contracts                      |
+--------------------------------------------------------------+--------------------------------------------------------------------------+
| `@daml/ledger <https://www.npmjs.com/package/@daml/ledger>`_ | Daml ledger object to connect and directly submit commands to the ledger |
+--------------------------------------------------------------+--------------------------------------------------------------------------+

You can install any of these libraries by running ``npm install <library>`` in the ``ui`` directory of
your project, e.g. ``npm install @daml/react``. Please explore the ``create-daml-app`` example project
to see the usage of these libraries.

To make your life easy when interacting with the ledger, the Daml assistant can generate JavaScript
libraries with TypeScript typings from the data types declared in the deployed DAR.

.. code-block:: bash

  daml codegen js .daml/dist/<your-project-name.dar> -o ui/daml.js

This command will generate a JavaScript library for each DALF in you DAR, containing metadata about
types and templates in the DALF and TypeScript typings them. In ``create-daml-app``, ``ui/package.json`` refers to these
libraries via the ``"create-daml-app": "file:../daml.js/create-daml-app-0.1.0"`` entry in the
``dependencies`` field.

If you choose a different JavaScript based frontend framework, the packages ``@daml/ledger``,
``@daml/types`` and the generated ``daml.js`` libraries provide you with the necessary code to
connect and issue commands against your ledger.

Authorization
~~~~~~~~~~~~~

When you deploy your application to a production ledger, you need to authenticate the identities of
your users.

Daml ledgers support a unified interface for authorization of commands. Some Daml ledgers, like for
example https://projectdabl.com, offer integrated authentication and authorization, but you can also
use an external service provider like https://auth0.com. The Daml react libraries support interfacing
with a Daml ledger that validates authorization of incoming requests. Simply initialize your
``DamlLedger`` object with the token obtained by the respective token issuer. How authorization works and the
form of the required tokens is described in the :ref:`Authorization <authorization>` section.

Developer workflow
~~~~~~~~~~~~~~~~~~

The SDK enables a local development environment with fast iteration cycles:

1. The integrated VSCode IDE (``daml studio``) runs your Scripts on any change to your Daml models. See :ref:`Daml Script <testing-using-script>`.
#. ``daml start`` will build all of your Daml code, generate the JavaScript bindings, and start the required "backend" processes (sandbox and HTTP JSON API). It will also allow you to press ``r`` (followed by Enter on Windows) to rebuild your code, regenerate the JavaScript bindings and upload the new code to the running ledger.
#. ``npm start`` will watch your JavaScript source files for change and recompile them immediately when they are saved.

Together, these features can provide you with very tight feedback loops while developing your Daml application, all the way from your Daml contracts up to your web UI. A typical Daml developer workflow is to

  1. Make a small change to your Daml data model
  #. Optionally test your Daml code with :ref:`Daml Script <testing-using-script>`
  #. Edit your React components to be aligned with changes made in Daml code
  #. Extend the UI to make use of the newly introduced feature
  #. Make further changes either to your Daml and/or React code until you're happy with what you've developed

.. image:: ./developer_workflow.svg

See :doc:`Your First Feature </getting-started/first-feature>` for a more detailed walkthrough of these steps.

.. _command-deduplication:

Command deduplication
*********************

The interaction of a Daml application with the ledger is inherently asynchronous: applications send commands to the ledger, and some time later they see the effect of that command on the ledger.

There are several things that can fail during this time window: the application can crash, the participant node can crash, messages can be lost on the network, messages can overtake each other, or the ledger may be just slow to respond due to a high load.

If you want to make sure that a command is applied to the ledger at most once, your application needs to robustly handle all the failure scenarios.
Daml ledgers provide a mechanism for :ref:`command deduplication and submission ranking <command-submission-service-deduplication>` to help deal with this problem.

An application can provide for each command the following parameters:

- A :ref:`command ID <com.daml.ledger.api.v1.Commands.command_id>`.
  The command ID, the application ID, and the set of submitting parties together form the change ID,
  which uniquely identifies the intended ledger change.
- An optional :ref:`submission ID <com.daml.ledger.api.v1.Commands.submission_id>` to uniquely identify the submission among all (re)submissions with the same change ID.
- An optional :ref:`submission rank <com.daml.ledger.api.v1.Commands.submission_rank>` to rank the submissions with the same change ID.
- An optional :ref:`deduplication period <com.daml.ledger.api.v1.Commands.deduplication>` during which command submissions with the same change ID are deduplicated.

The change ID, submission ID, and the submission rank are included in the completion event to correlate the event to the command submission.
Their guarantees are described in the :ref:`Ledger API Services <command-submission-service-deduplication>` section.
We now look at how applications should set these parameters in different scenarios.


No deduplication needed
=======================

Some applications do not need command deduplication, say because the Daml models already prevent duplication.

* Set a fresh command ID for each submission, e.g., a random UUID.
* Omit the submission ID, submission rank, and deduplication period.
  The participant or ledger sets them at their discretion.
  Under unlucky circumstances, such a submission may result in a ``NOT_FOUND`` or ``OUT_OF_RANGE`` error due to inappropriate choices for the submission rank and deduplication period.
  If so, the application should resubmit the command.

If the application receives a ``DEADLINE_EXCEEDED`` response or no response at all, then it must not make any assumptions about the command being applied or not until a completion event for the change ID appears on the completion stream.
Yet, if the submission is actually lost, no such completion event will ever appear.
To obtain finality, the application may resubmit the command and rely on its own mechanisms (e.g. in the Daml model) to prevent duplication.


Bounded network delays and clock skews
======================================

In this scenario, the application wants to have its intended ledger changes applied at most once and it wants finality about whether they were applied.
We assume bounds on the following:

* The clock skews between application, participant and ledger.
  
* The message delay on the network from the application to the participant and the latency for processing a submission.

* The downtime or failover time of the application system, the particpant, and the ledger.

Commands may be duplicated and finality violated if the actual skews and delays exceed the bounds.
Finality may not be achieveable if unhealthy conditions persist for longer than the :ref:`max deduplication time <com.daml.ledger.api.v1.LedgerConfiguration.max_deduplication_time>`.

We further assume that the application persists its intended ledger changes and whether it has decided to not retry a change.
It also maintains a watermark of the changes that have either been applied or rejected and will not be retried and the time when it has advanced the watermark.
The application keeps track of the in-flight command submissions via their submission IDs, but need not persist them.

#. After starting up, the application submits a dummy command with a fresh command ID and submission ID (e.g., a random UUID), but no submission rank or deduplication period.
   This should generate a definite-answer completion with a completion offset ``off_start``.
   The application can use the command service for the submission and extract the completion offset from the successful response or the error details of a rejection.   
   If the submission fails without generating a completion offset, the application should repeat this step until it receives such a fresh completion offset.

#. All subsequent command submission should use a submission rank that is between ``off_start`` and the current completion end.
   To that end, we assume that the application maintains a running completion offset ``off_running``.
   It is initialized to ``off_start`` and need not be persisted.
   Whenever the application observes a completion offset on the completion stream or as part of an RPC response from the command service,
   it advances ``off_running`` to the observed offset.

#. Starting above the watermark, the application (re)submits the persisted changes and new incoming changes.
   
   - The command ID is derived deterministically from the intended change, e.g., using a hash function.
   - A fresh submission ID is set (e.g., a random UUID).
     Each resubmission should use a fresh submission ID.
   - The submission rank is set to ``off_running``.
   - The deduplication period should start before the first time that the application might have submitted the command.
     This can be approximated by the watermark's time minus the processing latency, clock skews, and message delays.
   
   The following errors around command deduplication should be dealt with (see ``error.proto`` for details on the errors):

   - ``NOT_FOUND`` for the deduplication start:
     The specified deduplication period is too long.
     The application should find a shorter deduplication time that is still safe.
     Alternatively, the application can resubmit the change ID with a dummy command instead of the intended ledger change;
     when it receives a definite answer for the dummy and there are no other submissions with a higher submission rank,
     it can conclude that the actual ledger change will not be applied.

   - ``OUT_OF_RANGE`` for the deduplication start:
     The deduplication start lies in the future.
     Resubmit with an earlier deduplication start.

   - ``OUT_OF_RANGE`` for the submission rank:
     This should happen only when the application uses a completion offset obtained from one participant for a submission on another participant.
     The application should not mix completion offsets from different participants.

   - ``ALREADY_EXISTS`` with reason ``DEDUPLICATION``:
     The error details contain the offset and submission id of the earlier completion.
     There is no need to retry as the reaction to the earlier submission should have dealt with the intended ledger change.

   - ``ABORTED`` with reason ``ALREADY_IN_FLIGHT``:
     There is already another submission with the same change ID in flight, whose submission ID can be found in the error details.
     If no completion event appears for the other submission in a while, the application should retry the current submission,
     as the in-flight submission might have led to a rejection RPC response that got lost.

   - ``FAILED_PRECONDITION`` with reason ``RANK_TOO_LOW``:
     There is another command submission for the same change ID with a higher submission rank, whose submission ID and rank can be found in the error details.
     Resubmit with a fresh submission ID and a higher submission rank.
   
   When the submission results in a rejected completion that is not marked as a definite answer,
   the application should analyse the cause of the error. To obtain finality, it should adjust the command submission if necessary and resubmit
   until it receives a definite answer.

   When the application receives a definite-answer rejection, and there are no in-flight submissions with a higher submission rank for the change ID,
   it can decide whether it wants to resubmit the change. If not, the application can conclude that the change will not be applied to the ledger and
   check whether the watermark can be increased.

   When the application receives an accepting completion, it should check whether the watermark can be increased.


The asynchronous case
=====================

This scenario differs from the previous one in that no bound on message delays, processing latencies, and clock skews need to be known.
So the choice of the deduplication start changes; the rest stays the same.

We assume that the application has a local clock.
When it observes a completion offset for the first time, it persistently associates the offset with the clock's time.
So observation times increase with the offsets.
The application also persists with every intended ledger changes when it has persisted them.
Then, the deduplication offset is chosen such that the time associated with the offset is before the time associated with the intended ledger change.


.. _failing-over-between-ledger-api-endpoints:

Failing over between Ledger API endpoints
*****************************************

Some Daml Ledgers support exposing multiple eventually consistent Ledger API
endpoints where command deduplication works across these Ledger API endpoints.
For example, these endpoints might be hosted by separate Ledger API servers
that replicate the same data and host the same parties. Contact your ledger
operator to find out whether this applies to your ledger.

Below we describe how you can build your application such that it can switch
between such eventually consistent Ledger API endpoints to tolerate server
failures. You can do this using the following two steps.

First, your application must keep track of the last ledger offset received
from the :ref:`transaction service <transaction-service>` or the
:ref:`command completion service <command-completion-service>`.  When switching to a new
Ledger API endpoint, it must resume consumption of the transaction (tree)
and/or the command completion streams starting from this last received
offset.

Second, your application must retry on ``OUT_OF_RANGE`` errors (see
`gRPC status codes <https://grpc.github.io/grpc/core/md_doc_statuscodes.html>`_)
received from a stream subscription -- using an appropriate backoff strategy
to avoid overloading the server. Such errors can be raised because of eventual
consistency. The Ledger API endpoint that the application is newly subscribing
to might be behind the endpoint that it subscribed to before the switch, and
needs time to catch up. Thanks to eventual consistency this is guaranteed to
happen at some point in the future.

Once the application successfully subscribes to its required streams on the
new endpoint, it will resume normal operation.


.. _dealing-with-time:

Dealing with time
*****************

The Daml language contains a function :ref:`getTime <daml-ref-gettime>` which returns a rough estimate of “current time” called *Ledger Time*. The notion of time comes with a lot of problems in a distributed setting: different participants might run different clocks, there may be latencies due to calculation and network, clocks may drift against each other over time, etc.

In order to provide a useful notion of time in Daml without incurring severe performance or liveness penalties, Daml has two notions of time: *Ledger Time* and *Record Time*:

- As part of command interpretation, each transaction is automatically assigned a *Ledger Time* by the participant server.
- All calls to ``getTime`` within a transaction return the *Ledger Time* assigned to that transaction.
- *Ledger Time* is chosen (and validated) to respect Causal Monotonicity: The Create action on a contract *c* always precedes all other actions on *c* in Ledger Time.
- As part of the commit/synchronization protocol of the underlying infrastructure, every transaction is assigned a *Record Time*, which can be thought of as the infrastructures "system time". It's the best available notion of "real time", but the only guarantees on it are the guarantees the underlying infrastructure can give. It is also not known at interpretation time.
- *Ledger Time* is kept close to "real time" by bounding it against *Record Time*. Transactions where *Ledger* and *Record Time* are too far apart are rejected.

Some commands might take a long time to process, and by the time the resulting transaction is about to be committed to the ledger, it might violate the condition that *Ledger Time* should  be reasonably close to *Record Time* (even when considering the ledger's tolerance interval). To avoid such problems, applications can set the optional parameters :ref:`min_ledger_time_abs <com.daml.ledger.api.v1.Commands.min_ledger_time_abs>` or :ref:`min_ledger_time_rel <com.daml.ledger.api.v1.Commands.min_ledger_time_rel>` that specify (in absolute or relative terms) the minimal *Ledger Time* for the transaction. The ledger will then process the command, but wait with committing the resulting transaction until *Ledger Time* fits within the ledger's tolerance interval.

How is this used in practice?

- Be aware that ``getTime`` is only reasonably close to real time, and not completely monotonic. Avoid Daml workflows that rely on very accurate time measurements or high frequency time changes.
- Set ``min_ledger_time_abs`` or ``min_ledger_time_rel`` if the duration of command interpretation and transmission is likely to take a long time relative to the tolerance interval set by the ledger.
- In some corner cases, the participant node may be unable to determine a suitable Ledger Time by itself. If you get an error that no Ledger Time could be found, check whether you have contention on any contract referenced by your command or whether the referenced contracts are sensitive to small changes of ``getTime``.

For more details, see :ref:`Background concepts - time <time>`.
