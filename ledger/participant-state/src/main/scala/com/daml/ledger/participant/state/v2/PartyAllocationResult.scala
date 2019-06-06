// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.digitalasset.ledger.api.domain.PartyDetails

sealed abstract class PartyAllocationResult extends Product with Serializable

object PartyAllocationResult {

  /** The party was successfully allocated */
  final case class Ok(result: PartyDetails) extends PartyAllocationResult

  /** Synchronous party allocation is not supported */
  final case object NotSupported extends PartyAllocationResult

  /** The requested party name already exists */
  final case object AlreadyExists extends PartyAllocationResult

  /** The requested party name is not valid */
  final case object InvalidName extends PartyAllocationResult
}
