// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.Generators.nonEmptyListGen
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.version.ProtocolVersion
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

final case class GeneratorsVerdict(
    protocolVersion: ProtocolVersion,
    generatorsLocalVerdict: GeneratorsLocalVerdict,
) {
  import generatorsLocalVerdict.*

  // TODO(#14515): move elsewhere?
  implicit val protoMediatorRejectionCodeArb
      : Arbitrary[com.digitalasset.canton.protocol.v30.MediatorRejection.Code] = genArbitrary

  // TODO(#14515) Check that the generator is exhaustive
  implicit val mediatorRejectArb: Arbitrary[Verdict.MediatorReject] =
    Arbitrary(
      // TODO(#14515): do we want randomness here?
      Gen.const {
        val status = com.google.rpc.status.Status(com.google.rpc.Code.CANCELLED_VALUE)
        Verdict.MediatorReject.tryCreate(status, protocolVersion)
      }
    )

  private val verdictApproveArb: Arbitrary[Verdict.Approve] = Arbitrary(
    Gen.const(Verdict.protocolVersionRepresentativeFor(protocolVersion)).map(Verdict.Approve())
  )

  private implicit val participantRejectArb: Arbitrary[Verdict.ParticipantReject] = Arbitrary(
    nonEmptyListGen[(Set[LfPartyId], LocalReject)](
      participantRejectReasonArb
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
  implicit val verdictArb: Arbitrary[Verdict] = Arbitrary(
    Gen.oneOf(
      verdictApproveArb.arbitrary,
      mediatorRejectArb.arbitrary,
      participantRejectArb.arbitrary,
    )
  )
}
