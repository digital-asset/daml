.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. ops-ref_index:

Ledger API Server pruning
=========================

DAML Ledger API servers may support pruning DAML contracts archived and transactions submitted
before or at a given ledger offset, for example in order to reduce hot storage usage or to comply
with privacy demands [1]_.

The DAML Ledger API server pruning functionality is provided by the Pruning Service,
it requires administrative privileges and it is currently considered EXPERIMENTAL.

Please refer to the specific DAML Driver information for detailed information about whether it supports
pruning and its impacts.

.. [1] For example, as enabled by provisions about the "right to be forgotten" of legislation such as EU's GDPR.

Impacts on DAML applications
----------------------------

When supported, pruning can be invoked by an operator with administrative privileges at any time on an healthy
DAML Ledger API server; furthermore, it doesn't require stopping nor suspending normal operation.

Still, DAML applications may be affected in the following ways:

- Pruning is potentially a long-running operation and demanding one in terms of system resources; as such, it may
  significantly reduce DAML Ledger API throughput and increase latency while it is being performed.
  It is thus recommended to plan pruning invocations when low system utilization is expected.
- Pruning may degrade the behavior of and/or crash the system if the pruning offset is too recent. In particular,
  the system might misbehave if command completions are pruned before the command trackers are able to process
  the completions.
- Pruning may affect some Ledger API behavior: see the next sub-section for more information about API impacts.

How the DAML Ledger API is affected
-----------------------------------

- Active data streams from the Ledger API server may abort and need to be re-established by the DAML application
  from a later offset than pruned, even if they are already streaming past it.
- Requesting information at offsets that predate pruning, including from the ledger's start, will result
  in a ``NOT_FOUND`` gRPC error.
  - As a consequence, after pruning, a DAML application must bootstrap from the Active Contract Service and a
  recent offset [2]_.

Please refer to the DAML Ledger API documentation for details about the ``prune`` operation itself and
the behavior of other DAML Ledger API endpoints when pruning is being or has been performed.

Other limitations
-----------------

- If the number of divulged contracts continually grows, the storage used by the participant node may still
  keep growing, even with regular pruning.
- Pruning may be rejected even if the node is running correctly (for example, to preserve non-repudiation properties);
  in this case, the application might not be able to archive contracts containing PII or pruning of these contracts
  may not be possible; thus, actually deleting this PII may also not be technically feasible.
- Pruning may leave parties, packages, and configuration data on the participant node, even if they are no longer
  needed for transaction processing, and even if they contain PII [2]_.

.. [2] This might be improved in future versions.