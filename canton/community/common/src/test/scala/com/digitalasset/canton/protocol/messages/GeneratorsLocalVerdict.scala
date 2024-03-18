// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
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
import com.digitalasset.canton.protocol.LocalRejectError.TimeRejects.{
  LedgerTime,
  LocalTimeout,
  SubmissionTime,
}
import com.digitalasset.canton.protocol.LocalRejectError.TransferInRejects.{
  AlreadyCompleted,
  ContractAlreadyActive,
  ContractAlreadyArchived,
  ContractIsLocked,
}
import com.digitalasset.canton.protocol.LocalRejectError.TransferOutRejects.ActivenessCheckFailed
import com.digitalasset.canton.protocol.{LocalRejectErrorImpl, Malformed}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalacheck.{Arbitrary, Gen}

final case class GeneratorsLocalVerdict(protocolVersion: ProtocolVersion) {

  import com.digitalasset.canton.GeneratorsLf.lfPartyIdArb

  // TODO(#14515) Check that the generator is exhaustive
  private def localVerdictRejectGen: Gen[LocalReject] = {
    val resources = List("resource1", "resource2")
    val details = "details"

    val builders = Seq[LocalRejectErrorImpl](
      LockedContracts.Reject(resources),
      InactiveContracts.Reject(resources),
      LedgerTime.Reject(details),
      SubmissionTime.Reject(details),
      LocalTimeout.Reject(),
      ActivenessCheckFailed.Reject(details),
      ContractAlreadyArchived.Reject(details),
      ContractAlreadyActive.Reject(details),
      ContractIsLocked.Reject(details),
      AlreadyCompleted.Reject(details),
    )

    Gen
      .oneOf(builders)
      .map(_.toLocalReject(protocolVersion))
  }

  // TODO(#14515) Check that the generator is exhaustive
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

  // TODO(#14515) Check that the generator is exhaustive
  private def localRejectGen: Gen[LocalReject] =
    Gen.oneOf(localVerdictRejectGen, localVerdictMalformedGen)

  private def localApproveGen: Gen[LocalApprove] =
    Gen.const(LocalApprove(protocolVersion))

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: LocalVerdict) match {
      case _: LocalApprove => ()
      case _: LocalReject => ()
    }).discard
  }

  implicit val localVerdictArb: Arbitrary[LocalVerdict] = Arbitrary(
    Gen.oneOf(localApproveGen, localRejectGen)
  )

  implicit val participantRejectReasonArb: Arbitrary[(Set[LfPartyId], LocalReject)] =
    Arbitrary(
      for {
        parties <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
        reject <- localRejectGen
      } yield (parties, reject)
    )
}
