// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

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
    * @param hint A party identifier suggestion
    * @param displayName A human readable name of the new party
    *
    * @return an async result of a PartyAllocationResult
    */
  def allocateParty(
    hint: Option[String],
    displayName: Option[String]
  ): CompletionStage[PartyAllocationResult]
}
