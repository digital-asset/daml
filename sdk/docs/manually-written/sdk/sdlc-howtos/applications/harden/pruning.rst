Pruning
==========

.. note::
    What is pruning and how to deal with it on an app.

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
- Command deduplication and command tracker retention should always be configured so that the associated windows don't overlap with the pruning window to ensure that their operation is unaffected by pruning.
- Pruning may affect the behavior of Ledger API calls that allow to read data from the ledger: see the next sub-section for more information about API impacts.
- Pruning of all divulged contracts (see :ref:`Prune Request <com.daml.ledger.api.v1.admin.PruneRequest>`) does not preserve application visibility over contracts divulged up to the pruning offset, hence applications making use of pruned divulged contracts might start experiencing failed command submissions: see the section below for determining a suitable pruning offset.

.. warning::
  Participants may know of contracts for which they don't know the current activeness status. This happens through :ref:`divulgence <da-model-divulgence>` where a party learns of the existence of a contract without being guaranteed to ever see its archival. Such contracts are pruned by the feature described on this page as not doing so could easily lead to an ever growing participant state.

During command submission, parties can fetch divulged contracts. This is incompatible with the pruning behaviour described above which allows participant operators to reclaim storage space by pruning divulged contracts. Daml code running on pruned participants should therefore never rely on existence of divulged contracts prior to or at the pruning offset. Instead, such applications MUST ensure re-divulgence of the used contracts.


How the Daml Ledger API is Affected
-----------------------------------

Pruning of old data is not noticed by applications that are up to date. However pruning data in active use by applications can result in the following errors:

- Active data streams from the Daml Participant may abort and need to be re-established by the Daml application from a later offset than pruned, even if they are already streaming past it.
- Requesting information at offsets that predate pruning, including from the ledger's start, will result in a ``FAILED_PRECONDITION`` gRPC error.
  - As a consequence, after pruning, a Daml application must bootstrap from the Active Contract Service and a recent offset (TODO FIX REFERENCE).

Submission validation and Daml Ledger API endpoints that write to the ledger are generally not affected by pruning; an exception is that in-progress calls could abort while awaiting completion.

Please refer to the :doc:`protobuf documentation of the API </app-dev/grpc/proto-docs>` for details about the ``prune`` operation itself and the behavior of other Daml Ledger API endpoints when pruning is being or has been performed.
