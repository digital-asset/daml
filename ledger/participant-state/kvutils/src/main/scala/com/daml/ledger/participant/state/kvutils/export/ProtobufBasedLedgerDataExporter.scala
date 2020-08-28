// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.OutputStream

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{LedgerExportEntry, LedgerExportHeader}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

final class ProtobufBasedLedgerDataExporter private (output: OutputStream)
    extends LedgerDataExporter {
  override def addSubmission(submissionInfo: SubmissionInfo): SubmissionAggregator =
    new InMemorySubmissionAggregator(submissionInfo, Writer)

  private object Writer extends LedgerDataWriter {
    override def write(submissionInfo: SubmissionInfo, writeSet: WriteSet): Unit =
      output.synchronized {
        val builder = LedgerExportEntry.newBuilder
        builder.setSubmissionInfo(buildSubmissionInfo(submissionInfo))
        writeSet.foreach {
          case (key, value) =>
            builder.addWriteSet(buildWriteEntry(key, value))
        }
        val entry = builder.build()
        entry.writeDelimitedTo(output)
      }

    private def buildSubmissionInfo(
        submissionInfo: SubmissionInfo,
    ): LedgerExportEntry.SubmissionInfo =
      LedgerExportEntry.SubmissionInfo.newBuilder
        .setParticipantId(submissionInfo.participantId: String)
        .setCorrelationId(submissionInfo.correlationId)
        .setSubmissionEnvelope(submissionInfo.submissionEnvelope)
        .setRecordTime(Conversions.buildTimestamp(submissionInfo.recordTimeInstant))
        .build()

    private def buildWriteEntry(
        key: Key,
        value: Value,
    ): LedgerExportEntry.WriteEntry = {
      LedgerExportEntry.WriteEntry.newBuilder.setKey(key).setValue(value).build()
    }
  }

}

object ProtobufBasedLedgerDataExporter {
  val version = "v3"

  def start(output: OutputStream): LedgerDataExporter = {
    writeHeader(output)
    new ProtobufBasedLedgerDataExporter(output)
  }

  private def writeHeader(output: OutputStream): Unit = {
    val header = LedgerExportHeader.newBuilder.setVersion(version).build()
    header.writeDelimitedTo(output)
  }
}
