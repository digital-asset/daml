// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.messages.LocalReject.ConsistencyRejections.{
  DuplicateKey,
  InactiveContracts,
  InconsistentKey,
  LockedContracts,
  LockedKeys,
}
import com.digitalasset.canton.protocol.messages.LocalReject.MalformedRejects.{
  BadRootHashMessages,
  CreatesExistingContracts,
  ModelConformance,
  Payloads,
}
import com.digitalasset.canton.protocol.messages.LocalReject.TimeRejects.{
  LedgerTime,
  LocalTimeout,
  SubmissionTime,
}
import com.digitalasset.canton.protocol.messages.LocalReject.TransferInRejects.{
  AlreadyCompleted,
  ContractAlreadyActive,
  ContractAlreadyArchived,
  ContractIsLocked,
}
import com.digitalasset.canton.protocol.messages.LocalReject.TransferOutRejects.ActivenessCheckFailed
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import org.scalacheck.{Arbitrary, Gen}

final case class GeneratorsLocalVerdict(protocolVersion: ProtocolVersion) {

  import com.digitalasset.canton.GeneratorsLf.lfPartyIdArb

  // TODO(#14515) Check that the generator is exhaustive
  private def localRejectImplGen: Gen[LocalRejectImpl] = {
    val resources = List("resource1", "resource2")
    val details = "details"

    val builders: Seq[RepresentativeProtocolVersion[LocalVerdict.type] => LocalRejectImpl] = Seq(
      LockedContracts.Reject(resources),
      LockedKeys.Reject(resources),
      InactiveContracts.Reject(resources),
      DuplicateKey.Reject(resources),
      InconsistentKey.Reject(resources),
      LedgerTime.Reject(details),
      SubmissionTime.Reject(details),
      LocalTimeout.Reject(),
      ActivenessCheckFailed.Reject(details),
      ContractAlreadyArchived.Reject(details),
      ContractAlreadyActive.Reject(details),
      ContractIsLocked.Reject(details),
      AlreadyCompleted.Reject(details),
      /*
       GenericReject is intentionally excluded
       Reason: it should not be serialized.
       */
      // GenericReject("cause", details, resources, "SOME_ID", ErrorCategory.TransientServerFailure),
    )

    Gen.oneOf(builders).map(_(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion)))
  }

  // TODO(#14515) Check that the generator is exhaustive
  private def localVerdictMalformedGen: Gen[Malformed] = {
    val resources = List("resource1", "resource2")
    val details = "details"

    val builders: Seq[RepresentativeProtocolVersion[LocalVerdict.type] => Malformed] = Seq(
      /*
        MalformedRequest.Reject is intentionally excluded
        The reason is for backward compatibility reason, its `v0.LocalReject.Code` does not correspond to the id
        (`v0.LocalReject.Code.MalformedPayloads` vs "LOCAL_VERDICT_MALFORMED_REQUEST")
       */
      // MalformedRequest.Reject(details),
      Payloads.Reject(details),
      ModelConformance.Reject(details),
      BadRootHashMessages.Reject(details),
      CreatesExistingContracts.Reject(resources),
    )

    Gen.oneOf(builders).map(_(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion)))
  }

  // TODO(#14515) Check that the generator is exhaustive
  private def localRejectGen: Gen[LocalReject] =
    Gen.oneOf(localRejectImplGen, localVerdictMalformedGen)

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
