// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.digitalasset.ledger.api.domain.PartyDetails

sealed abstract class PartyAllocationResult extends Product with Serializable

object PartyAllocationResult {

  /** The party was successfully allocated */
  final case class Ok(result: PartyDetails) extends PartyAllocationResult

  /** Party allocation was rejected with the given reason */
  final case class Rejected(reason: PartyAllocationRejectionReason) extends PartyAllocationResult

}
