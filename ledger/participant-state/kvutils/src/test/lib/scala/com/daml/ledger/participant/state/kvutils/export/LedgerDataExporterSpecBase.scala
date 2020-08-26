// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, OutputStream}
import java.time.Instant

import com.daml.ledger.participant.state.v1
import com.google.protobuf.ByteString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.ClassTag

abstract class LedgerDataExporterSpecBase[T <: LedgerDataExporter: ClassTag]
    extends WordSpec
    with Matchers
    with MockitoSugar {
  protected def implementation(outputStream: OutputStream): LedgerDataExporter

  implicitly[ClassTag[T]].runtimeClass.getSimpleName should {
    "serialize a submission to something deserializable" in {
      val baos = new ByteArrayOutputStream()
      val instance = implementation(baos)
      val expectedRecordTimeInstant = Instant.ofEpochSecond(123456, 123456789)
      val expectedParticipantId = v1.ParticipantId.assertFromString("id")

      val submission = instance.addSubmission(
        v1.ParticipantId.assertFromString("id"),
        "parent",
        ByteString.copyFromUtf8("an envelope"),
        expectedRecordTimeInstant,
      )
      val writeSetA1 = submission.addChild()
      writeSetA1 ++= Seq(keyValuePairOf("a", "b"), keyValuePairOf("c", "d"))
      val writeSetA2 = submission.addChild()
      writeSetA2 ++= Seq(keyValuePairOf("e", "f"), keyValuePairOf("g", "h"))

      submission.finish()

      val dataInputStream = new DataInputStream(new ByteArrayInputStream(baos.toByteArray))
      val (actualSubmissionInfo, actualWriteSet) = Deserialization.deserializeEntry(dataInputStream)
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
