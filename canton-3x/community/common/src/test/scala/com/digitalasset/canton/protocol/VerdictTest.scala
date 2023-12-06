// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.protocol.messages.Verdict.{
  Approve,
  MediatorReject,
  ParticipantReject,
}
import com.digitalasset.canton.protocol.messages.{LocalReject, LocalVerdict, Verdict}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Serializable",
  )
)
class VerdictTest extends AnyWordSpec with BaseTest {
  private def party(name: String): LfPartyId = LfPartyId.assertFromString(name)
  private lazy val representativeProtocolVersion =
    LocalVerdict.protocolVersionRepresentativeFor(testedProtocolVersion)

  "Verdict" can {
    "converting to and from proto" should {
      "result in an equal object" in {
        val exampleResults = Table(
          ("type", "value"),
          ("approve", Approve(testedProtocolVersion)),
          (
            "reject",
            ParticipantReject(
              NonEmpty(
                List,
                (
                  Set(party("p1"), party("p2")),
                  LocalReject.MalformedRejects.Payloads
                    .Reject("some error")(representativeProtocolVersion),
                ),
                (
                  Set(party("p3")),
                  LocalReject.ConsistencyRejections.LockedContracts
                    .Reject(Seq())(representativeProtocolVersion),
                ),
              ),
              testedProtocolVersion,
            ),
          ),
          ("timeout", VerdictTest.timeoutVerdict(testedProtocolVersion)),
        )
        forAll(exampleResults) { (resultType: String, original: Verdict) =>
          val cycled =
            Verdict.fromProtoVersioned(
              original.toProtoVersioned
            ) match {
              case Left(err) => fail(err.toString)
              case Right(verdict) => verdict
            }

          assertResult(original, resultType)(cycled)
        }
      }
    }
  }
}

object VerdictTest {
  def timeoutVerdict(protocolVersion: ProtocolVersion): Verdict.MediatorReject =
    MediatorReject.tryCreate(
      MediatorError.MalformedMessage.Reject("").rpcStatusWithoutLoggingContext(),
      protocolVersion,
    )

  def malformedVerdict(protocolVersion: ProtocolVersion): Verdict.MediatorReject =
    MediatorReject.tryCreate(
      MediatorError.MalformedMessage.Reject("").rpcStatusWithoutLoggingContext(),
      protocolVersion,
    )
}
