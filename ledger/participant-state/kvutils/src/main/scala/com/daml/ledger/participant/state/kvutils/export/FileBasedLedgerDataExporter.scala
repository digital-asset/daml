// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.DataOutputStream
import java.time.Instant
import java.util.concurrent.locks.StampedLock

import com.daml.ledger.participant.state.kvutils.CorrelationId
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

/**
  * Enables exporting ledger data to an output stream.
  * This class is thread-safe.
  */
class FileBasedLedgerDataExporter(output: DataOutputStream) extends LedgerDataExporter {

  private val outputLock = new StampedLock

  override def addSubmission(
      participantId: ParticipantId,
      correlationId: CorrelationId,
      submissionEnvelope: Key,
      recordTimeInstant: Instant,
  ): SubmissionAggregator =
    this.synchronized {
      val submissionInfo =
        SubmissionInfo(participantId, correlationId, submissionEnvelope, recordTimeInstant)
      new InMemorySubmissionAggregator(submissionInfo, FileBasedLedgerDataWriter)
    }

  object FileBasedLedgerDataWriter extends LedgerDataWriter {
    override def write(submissionInfo: SubmissionInfo, writeSet: Seq[(Key, Value)]): Unit = {
      val stamp = outputLock.writeLock()
      try {
        Serialization.serializeEntry(submissionInfo, writeSet, output)
        output.flush()
      } finally {
        outputLock.unlock(stamp)
      }
    }
  }

}
