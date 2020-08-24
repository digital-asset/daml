// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.DataOutputStream
import java.time.Instant
import java.util.concurrent.locks.StampedLock

import com.daml.ledger.participant.state.kvutils.CorrelationId
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString

import scala.collection.mutable

/**
  * Enables exporting ledger data to an output stream.
  * This class is thread-safe.
  */
class FileBasedLedgerDataExporter(output: DataOutputStream) extends LedgerDataExporter {

  import FileBasedLedgerDataExporter._

  private val outputLock = new StampedLock

  private var submissionCorrelationId: Option[CorrelationId] = None
  private[export] val inProgressSubmissions =
    mutable.Map.empty[CorrelationId, (SubmissionInfo, SubmissionAggregator)]

  override def addSubmission(
      submissionEnvelope: ByteString,
      correlationId: CorrelationId,
      recordTimeInstant: Instant,
      participantId: ParticipantId,
  ): SubmissionAggregator =
    this.synchronized {
      if (submissionCorrelationId.isDefined) {
        throw new RuntimeException("Cannot add a submission without finishing the previous one.")
      }
      submissionCorrelationId = Some(correlationId)
      val aggregator = new InMemorySubmissionAggregator
      inProgressSubmissions.put(
        correlationId,
        SubmissionInfo(submissionEnvelope, correlationId, recordTimeInstant, participantId) -> aggregator,
      )
      aggregator
    }

  override def finishedProcessing(correlationId: CorrelationId): Unit = {
    this.synchronized {
      if (!submissionCorrelationId.contains(correlationId)) {
        throw new RuntimeException(
          s"Attempted to finish processing, but couldn't. Expected: $submissionCorrelationId, actual: $correlationId.")
      }
      val (submissionInfo, aggregator) = inProgressSubmissions.getOrElse(
        correlationId,
        throw new RuntimeException(
          s"Missing correlation ID during ledger data export (submissionInfo): $correlationId"),
      )
      writeSubmissionData(submissionInfo, aggregator.finish())
      submissionCorrelationId = None
      inProgressSubmissions.remove(correlationId)
      ()
    }
  }

  private def writeSubmissionData(
      submissionInfo: SubmissionInfo,
      writeSet: Seq[(Key, Value)],
  ): Unit = {
    val stamp = outputLock.writeLock()
    try {
      Serialization.serializeEntry(submissionInfo, writeSet, output)
      output.flush()
    } finally {
      outputLock.unlock(stamp)
    }
  }
}

object FileBasedLedgerDataExporter {
  case class SubmissionInfo(
      submissionEnvelope: ByteString,
      correlationId: CorrelationId,
      recordTimeInstant: Instant,
      participantId: ParticipantId,
  )

  type WriteSet = Seq[(Key, Value)]
}
