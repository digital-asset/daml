// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfPartyId

final case class UsedAndCreatedContracts(
    witnessed: Map[LfContractId, GenContractInstance],
    checkActivenessTxInputs: Set[LfContractId],
    consumedInputsOfHostedStakeholders: Map[LfContractId, Set[LfPartyId]],
    used: Map[LfContractId, GenContractInstance],
    maybeCreated: Map[LfContractId, Option[NewContractInstance]],
    transient: Map[LfContractId, Set[LfPartyId]],
    maybeUnknown: Set[LfContractId],
) {
  def created: Map[LfContractId, NewContractInstance] =
    maybeCreated.collect { case (cid, Some(sc)) => cid -> sc }
}

object UsedAndCreatedContracts {
  val empty: UsedAndCreatedContracts = UsedAndCreatedContracts(
    witnessed = Map.empty,
    checkActivenessTxInputs = Set.empty,
    consumedInputsOfHostedStakeholders = Map.empty,
    used = Map.empty,
    maybeCreated = Map.empty,
    transient = Map.empty,
    maybeUnknown = Set.empty,
  )
}
