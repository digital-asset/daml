.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Participant Pruning
===================

The Daml Ledger API exposes an append-only ledger model; on the other hand, Daml Participants must be able to operate continuously for an indefinite amount of time on a limited amount of hot storage.

In addition, privacy demands [1]_ may require removing Personally Identifiable Information (PII) upon request.

To satisfy these requirements, the :ref:`Pruning Service <com.daml.ledger.api.v1.admin.ParticipantPruningService>` Ledger API endpoint [2]_ allows Daml Participants to support pruning of Daml contracts and transactions that were respectively archived and submitted before or at a given ledger offset.

Please refer to the specific Daml driver information for details about its pruning support.

.. [1] For example, as enabled by provisions about the "right to be forgotten" of legislation such as
       `EU's GDPR <https://gdpr-info.eu/>`_.
.. [2] Invoking the Pruning Service requires administrative privileges.

Impacts on Daml Applications
----------------------------

When supported, pruning can be invoked by an operator with administrative privileges at any time on a healthy Daml participant; furthermore, it doesn't require stopping nor suspending normal operation.

Still, Daml applications may be affected in the following ways:

- Pruning is potentially a long-running operation and demanding one in terms of system resources; as such, it may significantly reduce Daml Ledger API throughput and increase latency while it is being performed. It is thus strongly recommended to plan pruning invocations, preferably, when the system is offline or at least when very low system utilization is expected.
- Pruning may degrade the behavior of or abort in-progress requests if the pruning offset is too recent. In particular, the system might misbehave if command completions are pruned before the command trackers are able to process the completions.
- Command deduplication and command tracker retention should always configured in such a way, that the associated windows don't overlap with the pruning window, so that their operation is unaffected by pruning.
- Pruning may affect the behavior of Ledger API calls that allow to read data from the ledger: see the next sub-section for more information about API impacts.
- Pruning of all divulged contracts (see :ref:`Prune Request <com.daml.ledger.api.v1.admin.PruneRequest>`) does not preserve application visibility over contracts divulged up to the pruning offset, hence applications making use of pruned divulged contracts might start experiencing failed command submissions: see the section below for determining a suitable pruning offset.

.. warning::
  Participants may know of contracts for which they don't know the current activeness status. This happens through :ref:`divulgence <da-model-divulgence>` where a party learns of the existence of a contract without being guaranteed to ever see its archival. Such contracts are pruned by the feature described on this page as not doing so could easily lead to an ever growing participant state.
  
During command submission, parties can fetch divulged contracts. This is incompatible with the pruning behaviour described above which allows participant operators to reclaim storage space by pruning divulged contracts. Daml code running on pruned participants should therefore never rely on existence of divulged contracts prior to or at the pruning offset. Instead, such applications MUST ensure re-divulgence of the used contracts.

How the Daml Ledger API is Affected
-----------------------------------

- Active data streams from the Daml Participant may abort and need to be re-established by the Daml application from a later offset than pruned, even if they are already streaming past it.
- Requesting information at offsets that predate pruning, including from the ledger's start, will result in a ``FAILED_PRECONDITION`` gRPC error.
  - As a consequence, after pruning, a Daml application must bootstrap from the Active Contract Service and a recent offset [3]_.

Submission validation and Daml Ledger API endpoints that write to the ledger are generally not affected by pruning; an exception is that in-progress calls could abort while awaiting completion.

Please refer to the :doc:`protobuf documentation of the API </app-dev/grpc/proto-docs>` for details about the ``prune`` operation itself and the behavior of other Daml Ledger API endpoints when pruning is being or has been performed.

Other Limitations
-----------------

- Pruning may be rejected even if the node is running correctly (for example, to preserve non-repudiation properties); in this case, the application might not be able to archive contracts containing PII or pruning of these contracts may not be possible; thus, actually deleting this PII may also be technically unfeasible.
- Pruning may leave parties, packages, and configuration data on the participant node, even if they are no longer needed for transaction processing, and even if they contain PII [3]_.
- Pruning does not move pruned information to cold storage but simply deletes pruned data; for this reason, it is advisable to back up the Participant Index DB before invoking pruning. See the next sub-section for more Participant Index DB-related advice before and after invoking `prune`.
- Pruning is not selective but rather effectively truncates the ledger, removing events on behalf of archived contracts and command completions at the pruning offset and all previous offsets.

.. [3] This might be improved in future versions.

How Pruning Affects Index DB Administration
-------------------------------------------

Pruning deletes data from the participant's database and therefore frees up space within it, which can and will be reused during the continued operation of the Index DB. Whether this freed up space is handed back to the OS depends on the database in use. For example, in PostgreSQL the deleted data frees up space in the table storage itself, but does not shrink the size of the files backing the tables of the IndexDB. Please refer to the PostgreSQL documentation on `VACUUM` and `VACUUM FULL` for more information.

Activities to be carried out *before* invoking a pruning operation should thus include backing up the Participant Index DB, as pruning will not move information to cold storage but rather it will delete events on behalf of archived contracts and command completions before or at the pruning offset.

In addition, activities to be carried out *after* invoking a pruning operation might include:

- On a PostgreSQL Index DB, especially if auto-vacuum tuning has not been performed, issuing `VACUUM` commands at appropriate times may improve performance and storage usage by letting the database reuse freed space. Note that `VACUUM FULL` commands are still needed for the OS to reclaim disk space previously used by the database.

Backing up and vacuuming, in addition to pruning itself, are also long-running and resource-hungry operations that might negatively affect the performance of regular workloads and even the availability of the system: this is true in particular for `VACUUM FULL` in PostgreSQL and equivalent commands in other DBMSs. These operations should thus be planned and taken carefully into account when sizing system resources. They should also be scheduled sensibly in relation to the desired sustained performance levels of regular workloads and to the hot storage usage goals.

Professional advice on database administration is strongly recommended that would take into account the DB specifics as well as all of the above aspects.

Determine a Suitable Pruning Offset
-----------------------------------

The :ref:`Transaction Service <transaction-service>` and the :ref:`Active Contract Service <active-contract-service>` provide offsets of the ledger end of the Transactions, and of Active Contracts snapshots respectively. Such offsets can be passed unchanged to `prune` calls, as long as they are lexicographically lower than the current ledger end.

When pruning all divulged contracts, the participant operator can choose the pruning offset as follows:

- Just before the ledger end, if no application hosted on the participant makes use of divulgence OR

- An offset old enough (e.g. older than an arbitrary multi-day grace period) that it ensures that pruning does not affect any recently-divulged contract needed by the applications hosted on the participant.

Scheduled jobs, applications and/or operator tools can be built on top of the Daml Ledger API to implement pruning automatically, for example at regular intervals, or on-demand, for example according to a user-initiated process.

For instance, pruning at regular intervals could be performed by a cron job that:

1. If a pruning interval has been saved to a well-known location:

   a. Backs up the Daml Participant Index DB.

   b. Performs pruning.

   c. (If using PostgreSQL) Performs a `VACUUM FULL` command on the Daml Participant Index DB.

2. Queries the current ledger end and saves its offset.

The interval between 2 (i.e. saving a recent ledger end offset) and the next cron job run determines the data retention window, that should be long enough not to affect deduplication and commands completion. For example, pruning at a recent ledger end offset could be problematic and should be avoided.

Pruning could also be initiated on-demand at the offset of a specific transaction [4]_, for example as provided by a user application based on search.

.. [4] Note that all the events on behalf of archived contracts and command completions found at earlier offsets will also be pruned.
