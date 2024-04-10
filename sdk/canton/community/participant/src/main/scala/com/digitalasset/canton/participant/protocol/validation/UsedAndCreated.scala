// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessSet,
}
import com.digitalasset.canton.protocol.*

final case class UsedAndCreated(
    contracts: UsedAndCreatedContracts,
    hostedWitnesses: Set[LfPartyId],
) {
  def activenessSet: ActivenessSet =
    ActivenessSet(
      contracts = contracts.activenessCheck,
      transferIds = Set.empty,
    )
}

final case class UsedAndCreatedContracts(
    witnessedAndDivulged: Map[LfContractId, SerializableContract],
    checkActivenessTxInputs: Set[LfContractId],
    consumedInputsOfHostedStakeholders: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    used: Map[LfContractId, SerializableContract],
    maybeCreated: Map[LfContractId, Option[SerializableContract]],
    transient: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
) {
  def activenessCheck: ActivenessCheck[LfContractId] =
    ActivenessCheck.tryCreate(
      checkFresh = maybeCreated.keySet,
      checkFree = Set.empty,
      checkActive = checkActivenessTxInputs,
      lock = consumedInputsOfHostedStakeholders.keySet ++ created.keySet,
      needPriorState = Set.empty,
    )

  def created: Map[LfContractId, SerializableContract] = maybeCreated.collect {
    case (cid, Some(sc)) => cid -> sc
  }
}
