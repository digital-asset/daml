// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.Generators.nonEmptyListGen
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.error.GeneratorsError
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.Ordered.orderingToOrdered

final case class GeneratorsVerdict(protocolVersion: ProtocolVersion) {

  private val generatorsLocalVerdict = GeneratorsLocalVerdict(protocolVersion)

  // TODO(#14515): move elsewhere?
  implicit val protoMediatorRejectionCodeArb
      : Arbitrary[com.digitalasset.canton.protocol.v0.MediatorRejection.Code] = genArbitrary

  private implicit val mediatorRejectV0Gen: Gen[Verdict.MediatorRejectV0] = {
    import com.digitalasset.canton.protocol.v0.MediatorRejection.Code
    for {
      code <- protoMediatorRejectionCodeArb.arbitrary
      if {
        code match {
          case Code.MissingCode | Code.Unrecognized(_) => false
          case _ => true
        }
      }
      reason <- Gen.alphaNumStr
    } yield Verdict.MediatorRejectV0.tryCreate(code, reason)
  }

  private def mediatorRejectV1Gen(
      rpv: RepresentativeProtocolVersion[Verdict.type]
  ): Gen[Verdict.MediatorRejectV1] = for {
    cause <- Gen.alphaNumStr
    id <- Gen.alphaNumStr
    damlError <- GeneratorsError.damlErrorCategoryArb.arbitrary
  } yield Verdict.MediatorRejectV1.tryCreate(cause, id, damlError.asInt, rpv)

  private def mediatorRejectV2Gen(
      rpv: RepresentativeProtocolVersion[Verdict.type]
  ): Gen[Verdict.MediatorRejectV2] =
    // TODO(#14515): do we want randomness here?
    Gen.const {
      val status = com.google.rpc.status.Status(com.google.rpc.Code.CANCELLED_VALUE)
      Verdict.MediatorRejectV2.tryCreate(status, rpv)
    }

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: Verdict.MediatorReject) match {
      case _: Verdict.MediatorRejectV0 => ()
      case _: Verdict.MediatorRejectV1 => ()
      case _: Verdict.MediatorRejectV2 => ()
    }).discard
  }

  private[messages] def mediatorRejectGen(
      rpv: RepresentativeProtocolVersion[Verdict.type]
  ): Gen[Verdict.MediatorReject] = {
    if (
      rpv >= Verdict.protocolVersionRepresentativeFor(
        Verdict.MediatorRejectV2.firstApplicableProtocolVersion
      )
    ) mediatorRejectV2Gen(rpv)
    else if (
      rpv >= Verdict.protocolVersionRepresentativeFor(
        Verdict.MediatorRejectV1.firstApplicableProtocolVersion
      )
    ) mediatorRejectV1Gen(rpv)
    else mediatorRejectV0Gen
  }

  // TODO(#14515) Check that the generator is exhaustive
  implicit val mediatorRejectArb: Arbitrary[Verdict.MediatorReject] =
    Arbitrary(
      Gen
        .const(Verdict.protocolVersionRepresentativeFor(protocolVersion))
        .flatMap(mediatorRejectGen)
    )

  private val verdictApproveArb: Arbitrary[Verdict.Approve] = Arbitrary(
    Gen.const(Verdict.protocolVersionRepresentativeFor(protocolVersion)).map(Verdict.Approve())
  )

  private implicit val participantRejectArb: Arbitrary[Verdict.ParticipantReject] = Arbitrary(
    nonEmptyListGen[(Set[LfPartyId], LocalReject)](
      generatorsLocalVerdict.participantRejectReasonArb
    ).map { reasons =>
      Verdict.ParticipantReject(reasons)(Verdict.protocolVersionRepresentativeFor(protocolVersion))
    }
  )

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: Verdict) match {
      case _: Verdict.Approve => ()
      case _: Verdict.MediatorReject => ()
      case _: Verdict.ParticipantReject => ()
    }).discard
  }
  private[messages] def verdictGen: Gen[Verdict] = {
    Gen.oneOf(
      verdictApproveArb.arbitrary,
      mediatorRejectGen(Verdict.protocolVersionRepresentativeFor(protocolVersion)),
      participantRejectArb.arbitrary,
    )
  }

  implicit val verdictArb: Arbitrary[Verdict] = Arbitrary(
    verdictGen
  )
}
