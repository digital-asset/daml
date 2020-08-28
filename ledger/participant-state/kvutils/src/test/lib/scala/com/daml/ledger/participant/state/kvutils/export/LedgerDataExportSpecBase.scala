// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{InputStream, OutputStream, PipedInputStream, PipedOutputStream}
import java.time.Instant

import com.daml.ledger.participant.state.v1
import com.google.protobuf.ByteString
import org.scalatest.{Matchers, WordSpec}

abstract class LedgerDataExportSpecBase(name: String) extends WordSpec with Matchers {
  protected def newExporter(outputStream: OutputStream): LedgerDataExporter

  protected def newImporter(inputStream: InputStream): LedgerDataImporter

  name should {
    "serialize a submission to something deserializable" in {
      val inputStream = new PipedInputStream()
      val outputStream = new PipedOutputStream(inputStream)
      val exporter = newExporter(outputStream)
      val expectedRecordTimeInstant = Instant.ofEpochSecond(123456, 123456789)
      val expectedParticipantId = v1.ParticipantId.assertFromString("id")

      val submission = exporter.addSubmission(
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

      val importer = newImporter(inputStream)

      val (actualSubmissionInfo, actualWriteSet) = importer.read().head
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
