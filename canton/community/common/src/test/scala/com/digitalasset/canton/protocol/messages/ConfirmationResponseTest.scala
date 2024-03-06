// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.protocol.{LocalRejectError, RequestId, RootHash}
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, LfPartyId, topology}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class ConfirmationResponseTest extends AnyWordSpec with BaseTest with HasCryptographicEvidenceTest {
  private lazy val localVerdictProtocolVersion =
    LocalVerdict.protocolVersionRepresentativeFor(testedProtocolVersion)

  private lazy val response1: ConfirmationResponse = ConfirmationResponse.tryCreate(
    RequestId(CantonTimestamp.now()),
    topology.ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::p1")),
    Some(ViewPosition.root),
    LocalApprove(testedProtocolVersion),
    Some(RootHash(TestHash.digest("txid1"))),
    Set(LfPartyId.assertFromString("p1"), LfPartyId.assertFromString("p2")),
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("da::default")),
    testedProtocolVersion,
  )
  private lazy val response2: ConfirmationResponse = ConfirmationResponse.tryCreate(
    RequestId(CantonTimestamp.now()),
    topology.ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::p1")),
    None,
    LocalRejectError.MalformedRejects.Payloads
      .Reject("test message")
      .toLocalReject(testedProtocolVersion),
    Some(RootHash(TestHash.digest("txid3"))),
    Set.empty,
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("da::default")),
    testedProtocolVersion,
  )

  def fromByteString(bytes: ByteString): ConfirmationResponse = {
    ConfirmationResponse
      .fromByteString(testedProtocolVersion)(bytes)
      .valueOr(err => fail(err.toString))
  }

  "ConfirmationResponse" should {
    behave like hasCryptographicEvidenceSerialization(response1, response2)
    behave like hasCryptographicEvidenceDeserialization(
      response1,
      response1.getCryptographicEvidence,
      "response1",
    )(fromByteString)
    behave like hasCryptographicEvidenceDeserialization(
      response2,
      response2.getCryptographicEvidence,
      "response2",
    )(fromByteString)
  }
}
