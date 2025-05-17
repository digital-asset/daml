// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessSet,
}
import com.digitalasset.canton.protocol.*

private[protocol] final case class UsedAndCreated(
    contracts: UsedAndCreatedContracts,
    hostedWitnesses: Set[LfPartyId],
) {
  def activenessSet: ActivenessSet =
    ActivenessSet(
      contracts = contracts.activenessCheck,
      reassignmentIds = Set.empty,
    )
}

private[protocol] final case class UsedAndCreatedContracts(
    witnessed: Map[LfContractId, SerializableContract],
    checkActivenessTxInputs: Set[LfContractId],
    consumedInputsOfHostedStakeholders: Map[LfContractId, Set[LfPartyId]],
    used: Map[LfContractId, SerializableContract],
    maybeCreated: Map[LfContractId, Option[SerializableContract]],
    transient: Map[LfContractId, Set[LfPartyId]],
    maybeUnknown: Set[LfContractId],
) {
  def activenessCheck: ActivenessCheck[LfContractId] =
    ActivenessCheck.tryCreate(
      checkFresh = maybeCreated.keySet,
      checkFree = Set.empty,
      // Don't check legitimately unknown contracts for activeness.
      checkActive = checkActivenessTxInputs -- maybeUnknown,
      // Let key locking know not to flag legitimately unknown contracts (such as those associated
      // with party onboarding) as activeness errors.
      // Note that created contracts are always locked, even if they are transient.
      lock = consumedInputsOfHostedStakeholders.keySet -- maybeUnknown ++ created.keySet,
      lockMaybeUnknown =
        (consumedInputsOfHostedStakeholders.keySet intersect maybeUnknown) -- created.keySet,
      needPriorState = Set.empty,
    )

  def created: Map[LfContractId, SerializableContract] = maybeCreated.collect {
    case (cid, Some(sc)) => cid -> sc
  }
}
