// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{
  BufferedOutputStream,
  InputStream,
  OutputStream,
  PipedInputStream,
  PipedOutputStream
}
import java.time.Instant

import com.daml.ledger.participant.state.kvutils.export.LedgerDataExportSpecBase._
import com.daml.ledger.participant.state.v1
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

abstract class LedgerDataExportSpecBase(name: String) extends AnyWordSpec with Matchers {
  protected def newExporter(outputStream: OutputStream): LedgerDataExporter

  protected def newImporter(inputStream: InputStream): LedgerDataImporter

  name should {
    "serialize a submission to something deserializable" in {
      val inputStream = new PipedInputStream()
      val outputStream = new PipedOutputStream(inputStream)
      val exporter = newExporter(outputStream)
      val importer = newImporter(inputStream)

      val submissionInfo = someSubmissionInfo()
      val submission = exporter.addSubmission(submissionInfo)
      val writeSetA1 = submission.addChild()
      writeSetA1 ++= Seq(keyValuePairOf("a", "b"), keyValuePairOf("g", "h"))
      val writeSetA2 = submission.addChild()
      writeSetA2 ++= Seq(keyValuePairOf("i", "j"), keyValuePairOf("e", "f"))
      writeSetA2 += keyValuePairOf("c", "d")
      submission.finish()

      outputStream.close()

      val (actualSubmissionInfo, actualWriteSet) = importer.read().head
      actualSubmissionInfo should be(submissionInfo)
      actualWriteSet should be(
        Seq(
          keyValuePairOf("a", "b"),
          keyValuePairOf("g", "h"),
          keyValuePairOf("i", "j"),
          keyValuePairOf("e", "f"),
          keyValuePairOf("c", "d"),
        ))
    }

    "flush between writes" in {
      val inputStream = new PipedInputStream()
      val outputStream = new BufferedOutputStream(new PipedOutputStream(inputStream))
      val exporter = newExporter(outputStream)
      val importer = newImporter(inputStream)

      val submissionInfo = someSubmissionInfo()
      val submission = exporter.addSubmission(submissionInfo)
      val writeSet = submission.addChild()
      writeSet += keyValuePairOf("a", "b")
      submission.finish()

      val (actualSubmissionInfo, actualWriteSet) = importer.read().head
      actualSubmissionInfo should be(submissionInfo)
      actualWriteSet should be(Seq(keyValuePairOf("a", "b")))

      outputStream.close()
    }
  }
}

object LedgerDataExportSpecBase {
  private def someSubmissionInfo(): SubmissionInfo = SubmissionInfo(
    participantId = v1.ParticipantId.assertFromString("id"),
    correlationId = "parent",
    submissionEnvelope = ByteString.copyFromUtf8("an envelope"),
    recordTimeInstant = Instant.ofEpochSecond(123456, 123456789),
  )

  private def keyValuePairOf(key: String, value: String): (ByteString, ByteString) =
    ByteString.copyFromUtf8(key) -> ByteString.copyFromUtf8(value)
}
