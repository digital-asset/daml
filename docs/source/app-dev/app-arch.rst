.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _recommended-architecture:

Application architecture
########################

This section describes our recommended design of a full-stack DAML application.

.. image:: ./recommended_architecture.svg

The above image shows the recommended architecture. Of course there are many ways how you can change
the architecture and technology stack to fit your needs, which we'll mention in the corresponding
sections.

To get started quickly with the recommended application architecture clone the
``create-daml-app`` application template:

.. code-block:: bash

  git clone https://github.com/digital-asset/create-daml-app

``create-daml-app`` is a small, but fully functional demo application implementing the recommended
architecture, providing you with an excellent starting point for your own application. It showcases

- using DAML React libraries
- quick iteration against the :ref:`DAML Ledger Sandbox <sandbox-manual>`.
- authorization
- deploying your application in the cloud as a Docker container

Backend
~~~~~~~

The backend for your application can be any DAML ledger implementation running your DAR (:ref:`DAML
Archive <dar-file-dalf-file>`) file.

We recommend using the :ref:`DAML JSON API <json-api>` as an interface to your frontend. It is
served by the HTTP JSON API server connected to the ledger API server. It provides simple HTTP
endpoints to interact with the ledger via GET/POST requests. However, if you prefer, you can also
use the :ref:`gRPC API <grpc>` directly.

When you use the ``create-daml-app`` template application, you can start a local sandbox together
with a JSON API server by running

.. code-block:: bash

  daml start --start-navigator=no

in the root of the project. This is the most simple DAML ledger implementation. Once your
application matures and becomes ready for production, the ``daml deploy`` command helps you deploy
your frontend and DAML artifacts of your project to a production ledger. See :ref:`Deploying to DAML
Ledgers <deploy-ref_overview>` for an in depth manual for specific ledgers.

Frontend
~~~~~~~~

We recommended building your frontend with the `React <https://reactjs.org>`_ framework. However,
you can choose virtually any language for your frontend and interact with the ledger via :ref:`HTTP
JSON <json-api>` endpoints. In addition, we provide support libraries for :ref:`Java
<java-bindings>` and :ref:`Scala <scala-bindings>` and you can also interact with the :ref:`gRPC API
<grpc>` directly.


We provide two libraries to build your React frontend for a DAML application.

+--------------------------------------------------------------+--------------------------------------------------------------------------+
| Name                                                         | Summary                                                                  |
+==============================================================+==========================================================================+
| `@daml/react <https://www.npmjs.com/package/@daml/react>`_   | React hooks to query/create/exercise DAML contracts                      |
+--------------------------------------------------------------+--------------------------------------------------------------------------+
| `@daml/ledger <https://www.npmjs.com/package/@daml/ledger>`_ | DAML ledger object to connect and directly submit commands to the ledger |
+--------------------------------------------------------------+--------------------------------------------------------------------------+

You can install any of these libraries by running ``npm install <library>`` in the ``ui`` directory of
your project, e.g. ``npm install @daml/react``. Please explore the ``create-daml-app`` example project
to see the usage of these libraries.

To make your life easy when interacting with the ledger, the DAML assistant can generate JavaScript
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

DAML ledgers support a unified interface for authorization of commands. Some DAML ledgers, like for
example https://projectdabl.com, offer integrated authentication and authorization, but you can also
use an external service provider like https://auth0.com. The DAML react libraries support interfacing
with a DAML ledger that validates authorization of incoming requests. Simply initialize your
``DamlLedger`` object with the token obtained by the respective token issuer. How authorization works and the
form of the required tokens is described in the :ref:`Authorization <authorization>` section.

Developer workflow
~~~~~~~~~~~~~~~~~~

The DAML SDK enables a local development environment with fast iteration cycles. If you run
``daml-reload-on-change.sh`` of the ``create-daml-app``, a local DAML sandbox ledger is started that
is updated with your most recent DAML code on any change. Next, you can start your frontend in
development mode by changing to your ``ui`` directory and run ``npm start``. This will reload your
frontend whenever you make changes to it. You can add unit tests for your DAML models by writing
:ref:`DAML scenarios <testing-using-scenarios>`. These will also be reevaluated on change.  A
typical DAML developer workflow is to

  #. Make a small change to your DAML data model
  #. Optionally test your DAML code and with :ref:`scenarios <testing-using-scenarios>`
  #. Edit your React components to be aligned with changes made in DAML code
  #. Extend the UI to make use of the newly introduced feature
  #. Make further changes either to your DAML and/or React code until you're happy with what you've developed

.. image:: ./developer_workflow.svg

.. _command-deduplication:

Command deduplication
*********************

The interaction of a DAML application with the ledger is inherently asynchronous: applications send commands to the ledger, and some time later they see the effect of that command on the ledger.

There are several things that can fail during this time window: the application can crash, the participant node can crash, messages can be lost on the network, or the ledger may be just slow to respond due to a high load.

If you want to make sure that a command is not executed twice, your application needs to robustly handle all the various failure scenarios.
DAML ledgers provide a mechanism for :ref:`command deduplication <command-submission-service-deduplication>` to help deal this problem.

For each command applications provide a command ID and an optional parameter that specifies the deduplication time. If the latter parameter is not specified in the command submission itself, the ledger will fall back to using the configured maximum deduplication time.
The ledger will then guarantee that commands for the same submitting party and command ID will be ignored within the deduplication time window.

To use command deduplication, you should:

- Use generous values for the deduplication time. It should be large enough such that you can assume the command was permanently lost if the deduplication time has passed and you still don’t observe any effect of the command on the ledger (i.e. you don't see a transaction with the command ID via the :ref:`transaction service <transaction-service>`).
- Make sure you set command IDs deterministically, that is to say: the "same" command must use the same command ID. This is useful for the recovery procedure after an application crash/restart, in which the application inspects the state of the ledger (e.g. via the :ref:`Active contracts service <active-contract-service>`) and sends commands to the ledger. When using deterministic command IDs, any commands that had been sent before the application restart will be discarded by the ledger to avoid duplicate submissions.
- If you are not sure whether a command was submitted successfully, just resubmit it. If the new command was submitted within the deduplication time window, the duplicate submission will safely be ignored. If the deduplication time window has passed, you can assume the command was lost or rejected and a new submission is justified.


For more details on command deduplication, see the :ref:`Ledger API Services <command-submission-service-deduplication>` documentation.


.. _failing-over-between-ledger-api-endpoints:

Failing over between Ledger API endpoints
*****************************************

Some DAML Ledgers support exposing multiple eventually consistent Ledger API
endpoints where command deduplication works across these Ledger API endpoints.
For example, these endpoints might be hosted by separate Ledger API servers
that replicate the same data and host the same parties. Contact your ledger
operator to find out whether this applies to your ledger.

Below we describe how you can build your application such that it can switch
between such eventually consistent Ledger API endpoints to tolerate server
failures. You can do this using the following two steps.

First, your application must keep track of the last ledger offset received
from the :ref:`transaction service <transaction-service>` or the :ref:`command
completion service <command-completion-service>`.  When switching to a new
Ledger API endpoint, it must resume consumption of the transaction (tree)
and/or the command completion streams starting from this last received
offset.

Second, your application must retry on ``OUT_OF_RANGE`` errors (see `gRPC
status codes <https://grpc.github.io/grpc/core/md_doc_statuscodes.html>`_)
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

The DAML language contains a function :ref:`getTime <daml-ref-gettime>` which returns the “current time”. The notion of time comes with a lot of problems in a distributed setting: different participants might run slightly different clocks, transactions would not be allowed to “overtake” each other during DAML interpretation, i.e., a long-running command could block all other commands, and many more.

To avoid such problems, DAML provides the following concept of *ledger time*:

- As part of command interpretation, each transaction is automatically assigned a *ledger time* by the participant server.
- All calls to ``getTime`` within a transaction return the *ledger time* assigned to that transaction.
- *Ledger time* is reasonably close to real time. To avoid transactions being rejected because the assigned *ledger time* does not match the ledger's system time exactly, DAML Ledgers define a tolerance interval around its system time. The system time is part of the ledger synchronization/consensus protocol, but is not known by the participant node at interpretation time. Transactions with a *ledger time* outside this tolerance interval will be rejected.
- *Ledger time* respects causal monotonicity: if a transaction ``x`` uses a contract created in another transaction ``y``, transaction ``x``\ s ledger time will be greater than or equal to the ledger time of the referenced transaction ``y``.

Some commands might take a long time to process, and by the time the resulting transaction is about to be committed to the ledger, it might violate the condition that *ledger time* should  be reasonably close to real time (even when considering the ledger's tolerance interval). To avoid such problems, applications can set the optional parameters :ref:`min_ledger_time_abs <com.daml.ledger.api.v1.Commands.min_ledger_time_abs>` or :ref:`min_ledger_time_rel <com.daml.ledger.api.v1.Commands.min_ledger_time_rel>` command parameters that specify (in absolute or relative terms) the minimal *ledger time* for the transaction. The ledger will then process the command, but wait with committing the resulting transaction until *ledger time* fits within the ledger's tolerance interval.

How is this used in practice?

- Be aware that ``getTime`` is only reasonably close to real time. Avoid DAML workflows that rely on very accurate time measurements or high frequency time changes.
- Set ``min_ledger_time_abs`` or ``min_ledger_time_rel`` if the duration of command interpretation and transmission is likely to take a long time relative to the tolerance interval set by the ledger.
- In some corner cases, the participant node may be unable to determine a suitable ledger time by itself. If you get an error that no ledger time could be found, check whether you have contention on any contract referenced by your command or whether the referenced contracts are sensitive to small changes of ``getTime``.

For details, see :ref:`Background concepts - time <time>`.
