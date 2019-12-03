// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.util.concurrent.CompletionStage

/** An interface for on-boarding parties via a participant. */
trait WritePartyService {

  /**
    * Adds a new party to the set managed by the ledger.
    *
    * Caller specifies a party identifier suggestion, the actual identifier
    * allocated might be different and is implementation specific.
    *
    * In particular, a ledger may:
    * - Disregard the given hint and choose a completely new party identifier
    * - Construct a new unique identifier from the given hint, e.g., by appending a UUID
    * - Use the given hint as is, and reject the call if such a party already exists
    *
    * The result of the party allocation is communicated synchronously.
    * TODO: consider also providing an asynchronous response in a similar
    * manner as it is done for transaction submission. It is possible that
    * in some implementations, party allocation will fail due to authorization etc.
    *
    * Successful party allocations will result in a [[Update.PartyAddedToParticipant]]
    * message. See the comments on [[ReadService.stateUpdates]] and [[Update]] for
    * further details.
    *
    * @param hint         : A party identifier suggestion
    *
    * @param displayName  : A human readable name of the new party
    *
    * @param tracingInfo  : The information allowing tracing of the request across the distributed stack.
    *
    * @return an async result of a PartyAllocationResult
    */
  def allocateParty(
      hint: Option[String],
      displayName: Option[String],
      tracingInfo: TracingInfo
  ): CompletionStage[PartyAllocationResult]
}
