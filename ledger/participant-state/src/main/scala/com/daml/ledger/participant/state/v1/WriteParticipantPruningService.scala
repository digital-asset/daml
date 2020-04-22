// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.util.concurrent.CompletionStage

import com.daml.lf.data.Time.Timestamp

/** An interface to prune participant ledger updates to manage participant ledger space and GDPR-style right to be
  * forgotten. */
trait WriteParticipantPruningService {

  /** Prune the ledger specifying the point in time up to which participant ledger transactions can be removed.
    *
    * As this interface applies only to the local participant, returns a (completion stage of) Option[ParticipantPruned]
    * rather than SubmissionResult.
    *
    * Participants that don't hold participant-local state or only elect to support [[pruneByOffset]], return None.
    * Returning None when pruning by time keeps the ledger api server from attempting to prune.
    */
  def pruneByTime(pruneUpTo: Timestamp): CompletionStage[Option[ParticipantPruned]]

  /** Prune the ledger specifying the point in time up to which participant ledger transactions can be removed.
    *
    * As this interface applies only to the local participant, returns a (completion stage of) Option[ParticipantPruned]
    * rather than SubmissionResult.
    *
    * Participants that don't hold participant-local state or only elect to support [[pruneByTime]], return None.
    * Note that returning None still results in the ledger api server attempting to prune at the specified offset.
    */
  def pruneByOffset(pruneUpTo: Offset): CompletionStage[Option[ParticipantPruned]]

}
