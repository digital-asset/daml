// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.DataOutputStream
import java.time.Instant
import java.util.concurrent.locks.StampedLock

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Enables exporting ledger data to an output stream.
  * This class is thread-safe.
  */
class FileBasedLedgerDataExporter(output: DataOutputStream) extends LedgerDataExporter {

  import FileBasedLedgerDataExporter._

  private val outputLock = new StampedLock

  private[export] val correlationIdMapping = mutable.Map.empty[String, String]
  private[export] val inProgressSubmissions = mutable.Map.empty[String, SubmissionInfo]
  private[export] val bufferedKeyValueDataPerCorrelationId =
    mutable.Map.empty[String, mutable.ListBuffer[(Key, Value)]]

  private var submissionCorrelationId: Option[String] = None

  override def addSubmission(
      submissionEnvelope: ByteString,
      correlationId: String,
      recordTimeInstant: Instant,
      participantId: ParticipantId,
  ): Unit =
    this.synchronized {
      if (submissionCorrelationId.isDefined) {
        throw new RuntimeException("Cannot add a submission without finishing the previous one.")
      }
      submissionCorrelationId = Some(correlationId)
      inProgressSubmissions.put(
        correlationId,
        SubmissionInfo(submissionEnvelope, correlationId, recordTimeInstant, participantId))
      ()
    }

  override def addParentChild(parentCorrelationId: String, childCorrelationId: String): Unit =
    this.synchronized {
      if (submissionCorrelationId.isEmpty) {
        throw new RuntimeException(
          s"Cannot add a parent/child when the submission correlation ID is empty.")
      }
      if (!submissionCorrelationId.contains(parentCorrelationId)) {
        throw new RuntimeException(
          s"Invalid parent correlation ID: $parentCorrelationId; expected $submissionCorrelationId")
      }
      correlationIdMapping.put(childCorrelationId, parentCorrelationId)
      ()
    }

  override def addToWriteSet(correlationId: String, data: Iterable[(Key, Value)]): Unit =
    this.synchronized {
      val parentCorrelationId = correlationIdMapping.getOrElse(
        correlationId,
        throw new RuntimeException(
          s"Missing correlation ID during ledger data export (addToWriteSet): $correlationId"))
      val keyValuePairs = bufferedKeyValueDataPerCorrelationId
        .getOrElseUpdate(parentCorrelationId, ListBuffer.empty)
      keyValuePairs.appendAll(data)
      bufferedKeyValueDataPerCorrelationId.put(parentCorrelationId, keyValuePairs)
      ()
    }

  override def finishedProcessing(correlationId: String): Unit = {
    this.synchronized {
      if (!submissionCorrelationId.contains(correlationId)) {
        throw new RuntimeException(
          s"Attempted to finish processing, but couldn't. Expected: $submissionCorrelationId, actual: $correlationId.")
      }
      val submissionInfo = inProgressSubmissions.getOrElse(
        correlationId,
        throw new RuntimeException(
          s"Missing correlation ID during ledger data export (submissionInfo): $correlationId"),
      )
      val bufferedData = bufferedKeyValueDataPerCorrelationId.getOrElse(
        correlationId,
        throw new RuntimeException(
          s"Missing correlation ID during ledger data export (bufferedData): $correlationId"),
      )
      writeSubmissionData(submissionInfo, bufferedData)
      submissionCorrelationId = None
      inProgressSubmissions.remove(correlationId)
      bufferedKeyValueDataPerCorrelationId.remove(correlationId)
      correlationIdMapping
        .collect {
          case (key, value) if value == correlationId => key
        }
        .foreach(correlationIdMapping.remove)
    }
  }

  private def writeSubmissionData(
      submissionInfo: SubmissionInfo,
      writeSet: ListBuffer[(Key, Value)]): Unit = {
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
      correlationId: String,
      recordTimeInstant: Instant,
      participantId: ParticipantId)

  type WriteSet = Seq[(Key, Value)]
}
