// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.util.concurrent.CompletionStage

/** An interface to prune participant ledger updates to manage participant ledger space and enable GDPR-style
  * right-to-be-forgotten support. */
trait WriteParticipantPruningService {

  /** Prune the participant ledger specifying the offset up to which participant ledger events can be removed.
    *
    * As this interface applies only to the local participant unlike other administrator services, returns a
    * (completion stage of a) PruningResult rather than a SubmissionResult.
    *
    * Ledgers that do not elect to support participant pruning, return NotPruned(Status.UNIMPLEMENTED). Returning an
    * error also keeps the ledger api server from pruning its index.
    *
    * Ledgers whose participants hold no participant-local state, but want the ledger api server to prune, return the
    * input offset as the output.
    */
  def prune(pruneUpToInclusive: Offset, submissionId: SubmissionId): CompletionStage[PruningResult]

}
