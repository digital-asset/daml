// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref

import java.util.concurrent.CompletionStage

/** An interface for on-boarding parties via a participant. */
trait PartySyncService {

  /** Adds a new party to the set managed by the ledger.
    *
    * Caller specifies a party identifier suggestion, the actual identifier
    * allocated might be different and is implementation specific.
    *
    * In particular, a ledger may:
    * - Disregard the given hint and choose a completely new party identifier
    * - Construct a new unique identifier from the given hint, e.g., by appending a UUID
    * - Use the given hint as is, and reject the call if such a party already exists
    *
    * Successful party allocations will result in a [[com.digitalasset.canton.ledger.participant.state.Update.PartyAddedToParticipant]]
    * message. See the comments on [[com.digitalasset.canton.ledger.participant.state.Update]] for
    * further details.
    *
    * @param hint             A party identifier suggestion
    * @param submissionId     Client picked submission identifier for matching the responses with the request.
    *
    * @return an async result of a SubmissionResult
    */
  def allocateParty(
      hint: Ref.Party,
      submissionId: Ref.SubmissionId,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult]
}
