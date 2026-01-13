// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.protocol.LocalRejectError.AssignmentRejects.AlreadyCompleted
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.{
  InactiveContracts,
  LockedContracts,
}
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.{
  BadRootHashMessages,
  CreatesExistingContracts,
  MalformedRequest,
  ModelConformance,
  Payloads,
}
import com.digitalasset.canton.protocol.LocalRejectError.ReassignmentRejects
import com.digitalasset.canton.protocol.LocalRejectError.TimeRejects.{
  LedgerTime,
  LocalTimeout,
  PreparationTime,
}
import com.digitalasset.canton.protocol.LocalRejectError.UnassignmentRejects.ActivenessCheckFailed
import com.digitalasset.canton.protocol.{LocalAbstainError, LocalRejectErrorImpl, Malformed}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{GeneratorsLf, LfPartyId}
import org.scalacheck.{Arbitrary, Gen}

final case class GeneratorsLocalVerdict(
    protocolVersion: ProtocolVersion,
    generatorsLf: GeneratorsLf,
) {

  import com.digitalasset.canton.Generators.*
  import generatorsLf.*

  private def localVerdictRejectGen: Gen[LocalReject] = {
    val resources = List("resource1", "resource2")
    val details = "details"

    val builders = Seq[LocalRejectErrorImpl](
      LockedContracts.Reject(resources),
      InactiveContracts.Reject(resources),
      LedgerTime.Reject(details),
      PreparationTime.Reject(details),
      LocalTimeout.Reject(),
      ActivenessCheckFailed.Reject(details),
      ReassignmentRejects.ValidationFailed.Reject(details),
      AlreadyCompleted.Reject(details),
    )

    Gen
      .oneOf(builders)
      .map(_.toLocalReject(protocolVersion))
  }

  private def localVerdictMalformedGen: Gen[LocalReject] = {
    val resources = List("resource1", "resource2")
    val details = "details"

    val builders = Seq[Malformed](
      MalformedRequest.Reject(details),
      Payloads.Reject(details),
      ModelConformance.Reject(details),
      BadRootHashMessages.Reject(details),
      CreatesExistingContracts.Reject(resources),
    )

    Gen
      .oneOf(builders)
      .map(_.toLocalReject(protocolVersion))
  }

  private def localRejectGen: Gen[LocalReject] =
    Gen.oneOf(localVerdictRejectGen, localVerdictMalformedGen)

  private def localApproveGen: Gen[LocalApprove] =
    Gen.const(LocalApprove(protocolVersion))

  private def localAbstainGen: Gen[LocalAbstain] =
    Gen
      .const(LocalAbstainError.CannotPerformAllValidations.Abstain("Unassignment data not found."))
      .map(_.toLocalAbstain(protocolVersion))

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: LocalVerdict) match {
      case _: LocalApprove => ()
      case _: LocalAbstain => ()
      case _: LocalReject => ()
    }).discard
  }

  implicit val localVerdictArb: Arbitrary[LocalVerdict] = Arbitrary(
    Gen.oneOf(localApproveGen, localRejectGen, localAbstainGen)
  )

  implicit val participantRejectReasonArb: Arbitrary[(Set[LfPartyId], ParticipantId, LocalReject)] =
    Arbitrary(
      for {
        parties <- boundedSetGen[LfPartyId]
        reject <- localRejectGen
        participantId <- generatorsLf.generatorsTopology.participantIdArb.arbitrary
      } yield (parties, participantId, reject)
    )
}
