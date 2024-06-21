// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.data.Offset

import java.util.concurrent.CompletionStage

/** An interface to prune participant ledger updates to manage participant ledger space and enable GDPR-style
  * right-to-be-forgotten support.
  */
trait WriteParticipantPruningService {

  /** Prune the participant ledger specifying the offset up to which participant ledger events can be removed.
    *
    * As this interface applies only to the local participant unlike other administrator services, returns a
    * (completion stage of a) PruningResult rather than a SubmissionResult.
    *
    * Ledgers that do not elect to support participant pruning, return NotPruned(Status.UNIMPLEMENTED). Returning an
    * error also keeps the ledger api server from pruning its index.
    *
    * Ledgers whose participants hold no participant-local state, but want the ledger api server to prune, return
    * ParticipantPruned.
    *
    * For pruning implementations to be fault tolerant, the following aspects are important:
    * - Consider failing a prune request before embarking on destructive operations for example if certain safety
    * conditions are not met (such as being low on resources). This helps minimize the chances of partially performed
    * prune operations. If the system cannot prune up to the specified offset, the call should not alter the system
    * and return NotPruned rather than prune partially.
    * - Implement pruning either atomically (performing all operations or none), or break down pruning steps into
    * idempotent pieces that pick up after retries or system recovery in case of a mid-pruning crash.
    * - To the last point, be aware that pruning of the ledger api server index happens in such an idempotent follow-up
    * step upon successful completion of each prune call. To reach eventual consistency upon failures, be sure
    * to return ParticipantPruned even if the specified offset has already been pruned to allow ledger api server
    * index pruning to proceed in case of an earlier failure.
    *
    * @param pruneUpToInclusive The offset up to which contracts should be pruned.
    * @param submissionId The submission id.
    * @param pruneAllDivulgedContracts If set, instruct the ledger to prune all immediately divulged contracts
    *                                  created before `pruneUpToInclusive` independent of whether they were archived before
    *                                  `pruneUpToInclusive`.
    * @return The pruning result.
    */
  def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult]

}
