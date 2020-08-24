// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.time.Instant

import com.daml.ledger.participant.state.v1
import com.google.protobuf.ByteString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class FileBasedLedgerDataExportSpec extends WordSpec with Matchers with MockitoSugar {
  "writing" should {
    "mark the submission as in progress" in {
      val instance = new FileBasedLedgerDataExporter(mock[DataOutputStream])

      val _ = instance.addSubmission(
        ByteString.copyFromUtf8("an envelope"),
        "parent",
        Instant.now(),
        v1.ParticipantId.assertFromString("id"),
      )

      instance.inProgressSubmissions.keys should contain("parent")
    }
  }

  "finishedProcessing" should {
    "remove all data such as submission info, write-set and child correlation IDs" in {
      val dataOutputStream = new DataOutputStream(new ByteArrayOutputStream())
      val instance = new FileBasedLedgerDataExporter(dataOutputStream)

      val submission = instance.addSubmission(
        ByteString.copyFromUtf8("another envelope"),
        "some other parent",
        Instant.now(),
        v1.ParticipantId.assertFromString("id"),
      )
      val writeSet = submission.addChild()
      writeSet += keyValuePairOf("a", "b")

      instance.finishedProcessing("some other parent")

      instance.inProgressSubmissions shouldBe empty
    }
  }

  "serialized submission" should {
    "be readable back" in {
      val baos = new ByteArrayOutputStream()
      val dataOutputStream = new DataOutputStream(baos)
      val instance = new FileBasedLedgerDataExporter(dataOutputStream)
      val expectedRecordTimeInstant = Instant.ofEpochSecond(123456, 123456789)
      val expectedParticipantId = v1.ParticipantId.assertFromString("id")

      val submission = instance.addSubmission(
        ByteString.copyFromUtf8("an envelope"),
        "parent",
        expectedRecordTimeInstant,
        v1.ParticipantId.assertFromString("id"),
      )
      val writeSetA1 = submission.addChild()
      writeSetA1 ++= Seq(keyValuePairOf("a", "b"), keyValuePairOf("c", "d"))
      val writeSetA2 = submission.addChild()
      writeSetA2 ++= Seq(keyValuePairOf("e", "f"), keyValuePairOf("g", "h"))

      instance.finishedProcessing("parent")

      val dataInputStream = new DataInputStream(new ByteArrayInputStream(baos.toByteArray))
      val (actualSubmissionInfo, actualWriteSet) = Serialization.readEntry(dataInputStream)
      actualSubmissionInfo.submissionEnvelope should be(ByteString.copyFromUtf8("an envelope"))
      actualSubmissionInfo.correlationId should be("parent")
      actualSubmissionInfo.recordTimeInstant should be(expectedRecordTimeInstant)
      actualSubmissionInfo.participantId should be(expectedParticipantId)
      actualWriteSet should be(
        Seq(
          keyValuePairOf("a", "b"),
          keyValuePairOf("c", "d"),
          keyValuePairOf("e", "f"),
          keyValuePairOf("g", "h"),
        ))
    }
  }

  private def keyValuePairOf(key: String, value: String): (ByteString, ByteString) =
    ByteString.copyFromUtf8(key) -> ByteString.copyFromUtf8(value)
}
