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
  // XXX SC remove in Scala 2.13; see notes in ConfSpec
  import scala.collection.GenTraversable, org.scalatest.enablers.Containing
  private[this] implicit def `fixed sig containingNatureOfGenTraversable`[
      E: org.scalactic.Equality,
      TRAV]: Containing[TRAV with GenTraversable[E]] =
    Containing.containingNatureOfGenTraversable[E, GenTraversable]

  "addParentChild" should {
    "add entry to correlation ID mapping" in {
      val instance = new FileBasedLedgerDataExporter(mock[DataOutputStream])
      instance.addSubmission(
        ByteString.copyFromUtf8("an envelope"),
        "parent",
        Instant.now(),
        v1.ParticipantId.assertFromString("id"),
      )
      instance.addParentChild("parent", "child")

      instance.correlationIdMapping should contain("child" -> "parent")
    }
  }

  "addToWriteSet" should {
    "append to existing data" in {
      val instance = new FileBasedLedgerDataExporter(mock[DataOutputStream])
      instance.addSubmission(
        ByteString.copyFromUtf8("an envelope"),
        "parent",
        Instant.now(),
        v1.ParticipantId.assertFromString("id"),
      )
      instance.addParentChild("parent", "child")
      instance.addToWriteSet("child", Seq(keyValuePairOf("a", "b")))
      instance.addToWriteSet("child", Seq(keyValuePairOf("c", "d")))

      instance.bufferedKeyValueDataPerCorrelationId should contain(
        "parent" ->
          Seq(keyValuePairOf("a", "b"), keyValuePairOf("c", "d")))
    }
  }

  "finishedProcessing" should {
    "remove all data such as submission info, write-set and child correlation IDs" in {
      val dataOutputStream = new DataOutputStream(new ByteArrayOutputStream())
      val instance = new FileBasedLedgerDataExporter(dataOutputStream)
      instance.addSubmission(
        ByteString.copyFromUtf8("an envelope"),
        "parent",
        Instant.now(),
        v1.ParticipantId.assertFromString("id"),
      )
      instance.addParentChild("parent", "parent")
      instance.addToWriteSet("parent", Seq(keyValuePairOf("a", "b")))

      instance.finishedProcessing("parent")

      instance.inProgressSubmissions shouldBe empty
      instance.bufferedKeyValueDataPerCorrelationId shouldBe empty
      instance.correlationIdMapping shouldBe empty
    }
  }

  "serialized submission" should {
    "be readable back" in {
      val baos = new ByteArrayOutputStream()
      val dataOutputStream = new DataOutputStream(baos)
      val instance = new FileBasedLedgerDataExporter(dataOutputStream)
      val expectedRecordTimeInstant = Instant.ofEpochSecond(123456, 123456789)
      val expectedParticipantId = v1.ParticipantId.assertFromString("id")
      instance.addSubmission(
        ByteString.copyFromUtf8("an envelope"),
        "parent",
        expectedRecordTimeInstant,
        v1.ParticipantId.assertFromString("id"))
      instance.addParentChild("parent", "parent")
      instance.addToWriteSet("parent", Seq(keyValuePairOf("a", "b")))

      instance.finishedProcessing("parent")

      val dataInputStream = new DataInputStream(new ByteArrayInputStream(baos.toByteArray))
      val (actualSubmissionInfo, actualWriteSet) = Serialization.readEntry(dataInputStream)
      actualSubmissionInfo.submissionEnvelope should be(ByteString.copyFromUtf8("an envelope"))
      actualSubmissionInfo.correlationId should be("parent")
      actualSubmissionInfo.recordTimeInstant should be(expectedRecordTimeInstant)
      actualSubmissionInfo.participantId should be(expectedParticipantId)
      actualWriteSet should be(Seq(keyValuePairOf("a", "b")))
    }
  }

  private def keyValuePairOf(key: String, value: String): (ByteString, ByteString) =
    ByteString.copyFromUtf8(key) -> ByteString.copyFromUtf8(value)
}
