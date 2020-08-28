// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export.v2

import java.io.{BufferedOutputStream, Closeable, DataOutputStream}
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.concurrent.locks.StampedLock

import com.daml.ledger.participant.state.kvutils.export.{
  InMemorySubmissionAggregator,
  LedgerDataExporter,
  LedgerDataWriter,
  SubmissionAggregator,
  SubmissionInfo,
  WriteSet
}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

/**
  * Enables exporting ledger data to an output stream.
  * This class is thread-safe.
  */
final class SerializationBasedLedgerDataExporter(output: DataOutputStream)
    extends LedgerDataExporter
    with Closeable {

  private val outputLock = new StampedLock

  override def addSubmission(submissionInfo: SubmissionInfo): SubmissionAggregator = {
    new InMemorySubmissionAggregator(submissionInfo, SerializationBasedLedgerDataWriter)
  }

  override def close(): Unit = output.close()

  object SerializationBasedLedgerDataWriter extends LedgerDataWriter {
    override def write(submissionInfo: SubmissionInfo, writeSet: Seq[(Key, Value)]): Unit = {
      val stamp = outputLock.writeLock()
      try {
        serializeEntry(submissionInfo, writeSet)
        output.flush()
      } finally {
        outputLock.unlock(stamp)
      }
    }

    private def serializeEntry(
        submissionInfo: SubmissionInfo,
        writeSet: Seq[(Key, Value)],
    ): Unit = {
      serializeSubmissionInfo(submissionInfo)
      serializeWriteSet(writeSet)
    }

    private def serializeSubmissionInfo(submissionInfo: SubmissionInfo): Unit = {
      output.writeUTF(submissionInfo.correlationId)
      writeBytes(submissionInfo.submissionEnvelope)
      writeInstant(submissionInfo.recordTimeInstant)
      output.writeUTF(submissionInfo.participantId)
    }

    private def serializeWriteSet(writeSet: WriteSet): Unit = {
      output.writeInt(writeSet.size)
      for ((key, value) <- writeSet.sortBy(_._1.asReadOnlyByteBuffer())) {
        writeBytes(key)
        writeBytes(value)
      }
    }

    private def writeBytes(bytes: Key): Unit = {
      output.writeInt(bytes.size())
      bytes.writeTo(output)
    }

    private def writeInstant(instant: Instant): Unit = {
      output.writeLong(instant.getEpochSecond)
      output.writeInt(instant.getNano)
    }
  }

}

object SerializationBasedLedgerDataExporter {
  def apply(path: Path): SerializationBasedLedgerDataExporter =
    new SerializationBasedLedgerDataExporter(
      new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path))))
}
