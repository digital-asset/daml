// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.LtHash16
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId, UniqueIdentifier}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class AcsCommitmentTest extends AnyWordSpec with BaseTest with HasCryptographicEvidenceTest {
  private val synchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::da")
  )
  private val sender = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("participant::da"))
  private val counterParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("participant2::da")
  )
  private val interval = PositiveSeconds.tryOfSeconds(1)
  private val period1 = CommitmentPeriod
    .create(
      CantonTimestamp.Epoch,
      CantonTimestamp.Epoch.plusSeconds(2),
      interval,
    )
    .value
  private val period2 = CommitmentPeriod
    .create(
      CantonTimestamp.Epoch.plusSeconds(2),
      CantonTimestamp.Epoch.plusSeconds(4),
      interval,
    )
    .value

  private val h = LtHash16()
  h.add("abc".getBytes())
  private val cmt = h.getByteString()

  private val commitment1 = AcsCommitment
    .create(
      synchronizerId,
      sender,
      counterParticipant,
      period1,
      cmt,
      testedProtocolVersion,
    )

  private val commitment2 = AcsCommitment
    .create(
      synchronizerId,
      sender,
      counterParticipant,
      period2,
      cmt,
      testedProtocolVersion,
    )

  private def fromByteString(bytes: ByteString): AcsCommitment =
    AcsCommitment.fromByteString(testedProtocolVersion, bytes) match {
      case Left(x) => fail(x.toString)
      case Right(x) => x
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
