// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{BufferedOutputStream, Closeable, OutputStream}
import java.nio.file.{Files, Path}

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.export.LedgerExport.LedgerExportEntry

import scala.jdk.CollectionConverters._

final class ProtobufBasedLedgerDataExporter private (output: OutputStream)
    extends LedgerDataExporter
    with Closeable {

  override def addSubmission(submissionInfo: SubmissionInfo): SubmissionAggregator =
    new InMemorySubmissionAggregator(submissionInfo, Writer)

  override def close(): Unit = output.close()

  private object Writer extends LedgerDataWriter {
    override def write(submissionInfo: SubmissionInfo, writeSet: WriteSet): Unit = {
      val entry = LedgerExportEntry.newBuilder
        .setSubmissionInfo(buildSubmissionInfo(submissionInfo))
        .addAllWriteSet(buildWriteSet(writeSet).asJava)
        .build
      output.synchronized {
        entry.writeDelimitedTo(output)
        output.flush()
      }
    }

    private def buildSubmissionInfo(
        submissionInfo: SubmissionInfo
    ): LedgerExportEntry.SubmissionInfo =
      LedgerExportEntry.SubmissionInfo.newBuilder
        .setParticipantId(submissionInfo.participantId: String)
        .setCorrelationId(submissionInfo.correlationId)
        .setSubmissionEnvelope(submissionInfo.submissionEnvelope.bytes)
        .setRecordTime(Conversions.buildTimestamp(submissionInfo.recordTimeInstant))
        .build()

    private def buildWriteSet(writeSet: WriteSet): Iterable[LedgerExportEntry.WriteEntry] =
      writeSet.map { case (key, value) =>
        LedgerExportEntry.WriteEntry.newBuilder
          .setKey(key.bytes)
          .setValue(value.bytes)
          .build()
      }
  }

}

object ProtobufBasedLedgerDataExporter {
  def start(output: OutputStream): ProtobufBasedLedgerDataExporter = {
    header.write(output)
    new ProtobufBasedLedgerDataExporter(output)
  }

  def start(path: Path): ProtobufBasedLedgerDataExporter =
    start(new BufferedOutputStream(Files.newOutputStream(path)))
}
