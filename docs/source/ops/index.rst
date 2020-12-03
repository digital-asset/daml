.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. ops-ref_index:

DAML Participant pruning
========================

.. HINT::
   DAML Participant pruning is currently an :doc:`Early Access Feature in Labs status </support/status-definitions>`

The DAML Ledger API exposes an append-only ledger model; on the other hand, DAML Participants must be able to operate continuously for an indefinite amount of time on a limited amount of hot storage.

In addition, privacy demands [1]_ may require removing Personally Identifiable Information (PII) upon request.

To satisfy these requirements, the :ref:`Pruning Service <com.daml.ledger.api.v1.admin.ParticipantPruningService>` Ledger API endpoint [2]_ allows DAML Participants to support pruning of DAML contracts and transactions that were respectively archived and submitted before or at a given ledger offset.

Please refer to the specific DAML Driver information for details about its pruning support.

.. [1] For example, as enabled by provisions about the "right to be forgotten" of legislation such as
       `EU's GDPR <https://gdpr-info.eu/>`_.
.. [2] Invoking the Pruning Service requires administrative privileges.

Impacts on DAML applications
----------------------------

When supported, pruning can be invoked by an operator with administrative privileges at any time on a healthy DAML participant; furthermore, it doesn't require stopping nor suspending normal operation.

Still, DAML applications may be affected in the following ways:

- Pruning is potentially a long-running operation and demanding one in terms of system resources; as such, it may significantly reduce DAML Ledger API throughput and increase latency while it is being performed. It is thus recommended to plan pruning invocations when low system utilization is expected.
- Pruning may degrade the behavior of and/or crash the system if the pruning offset is too recent. In particular, the system might misbehave if command completions are pruned before the command trackers are able to process the completions.
- Pruning may affect the behavior of Ledger API calls that allow to read data from the ledger: see the next sub-section for more information about API impacts.

How the DAML Ledger API is affected
-----------------------------------

- Active data streams from the DAML Participant may abort and need to be re-established by the DAML application from a later offset than pruned, even if they are already streaming past it.
- Requesting information at offsets that predate pruning, including from the ledger's start, will result in a ``NOT_FOUND`` gRPC error.
  - As a consequence, after pruning, a DAML application must bootstrap from the Active Contract Service and a recent offset [3]_.

Submission validation and DAML Ledger API endpoints that write to the ledger are generally not affected by pruning; an exception is that in-progress calls could abort while awaiting completion.

Please refer to the :doc:`protobuf documentation of the API </app-dev/grpc/proto-docs>` for details about the ``prune`` operation itself and the behavior of other DAML Ledger API endpoints when pruning is being or has been performed.

Other limitations
-----------------

- If the number of divulged contracts continually grows, the storage used by the participant node may still keep growing, even with regular pruning.
- Pruning may be rejected even if the node is running correctly (for example, to preserve non-repudiation properties); in this case, the application might not be able to archive contracts containing PII or pruning of these contracts may not be possible; thus, actually deleting this PII may also be technically unfeasible.
- Pruning may leave parties, packages, and configuration data on the participant node, even if they are no longer needed for transaction processing, and even if they contain PII [3]_.
- Pruning does not move pruned information to cold storage but simply deletes pruned data; for this reason, it is advisable to back up the Participant Index DB before invoking pruning. See the next sub-section for more Participant Index DB-related advice before and after invoking `prune`.

.. [3] This might be improved in future versions.

Pruning and the DAML Participant Index DB
-----------------------------------------

Activities to be carried out *before* invoking a pruning operation include:

- Backing up the Participant Index DB, as pruning will not move information to cold storage but rather it will delete archived contracts and transactions earlier than or at the pruning offset.

Activities to be carried out *after* invoking a pruning operation include:

- On a PostreSQL Index DB, a manual `VACUUM FULL` command is needed for the DB to give back disk space to the OS.

Determining a suitable pruning offset
-------------------------------------

The :ref:`Active Contract Service <active-contract-service>` and the :ref:`Transaction Service <transaction-service>` provide offset information for transactions and for Active Contracts snapshots respectively: such offset can be used unchanged with `prune` calls.

Scheduled jobs, applications and/or operator tools can be built on top of the DAML Ledger API to implement pruning automatically, for example at regular intervals, or on-demand, for example according to a user-initiated process.

For instance, pruning at regular intervals could be performed by a cron job that:

1. If a pruning interval has been saved to a well-known location:
   a. Backs up the DAML Participant Index DB.
   b. Performs pruning.
   c. (If using PostgreSQL) Performs a `VACUUM FULL` command on the DAML Participant Index DB.

2. Acquires a fresh Active Contract Set and saves the offset.

Pruning could also be initiated on-demand at the offset of a specific transaction [4]_, for example as provided by a user application based on a search.

.. [4] Note that not only a specific transaction but also earlier transactions and archived contracts will be pruned.