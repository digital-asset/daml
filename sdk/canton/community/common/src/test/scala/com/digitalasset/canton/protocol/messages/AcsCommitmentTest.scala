// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.LtHash16
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class AcsCommitmentTest extends AnyWordSpec with BaseTest with HasCryptographicEvidenceTest {
  val cryptoApi = new SymbolicPureCrypto
  val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::da"))
  val sender = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("participant::da"))
  val counterParticipant = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("participant2::da"))
  val interval = PositiveSeconds.tryOfSeconds(1)
  val period1 = CommitmentPeriod
    .create(
      CantonTimestamp.Epoch,
      CantonTimestamp.Epoch.plusSeconds(2),
      interval,
    )
    .value
  val period2 = CommitmentPeriod
    .create(
      CantonTimestamp.Epoch.plusSeconds(2),
      CantonTimestamp.Epoch.plusSeconds(4),
      interval,
    )
    .value

  val h = LtHash16()
  h.add("abc".getBytes())
  val cmt = h.getByteString()

  val commitment1 = AcsCommitment
    .create(
      domainId,
      sender,
      counterParticipant,
      period1,
      cmt,
      testedProtocolVersion,
    )

  val commitment2 = AcsCommitment
    .create(
      domainId,
      sender,
      counterParticipant,
      period2,
      cmt,
      testedProtocolVersion,
    )

  def fromByteString(bytes: ByteString): AcsCommitment = {
    AcsCommitment.fromByteString(testedProtocolVersion)(bytes) match {
      case Left(x) => fail(x.toString)
      case Right(x) => x
    }
  }

  "AcsCommitment" should {
    behave like hasCryptographicEvidenceSerialization(commitment1, commitment2)
    behave like hasCryptographicEvidenceDeserialization(
      commitment1,
      commitment1.getCryptographicEvidence,
      "commitment1",
    )(fromByteString)
    behave like hasCryptographicEvidenceDeserialization(
      commitment2,
      commitment2.getCryptographicEvidence,
      "commitment2",
    )(fromByteString)
  }
}
