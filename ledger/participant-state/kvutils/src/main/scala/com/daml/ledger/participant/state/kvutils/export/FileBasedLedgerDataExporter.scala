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
  * Enables exporting ledger data to a file.
  * This class is thread-safe.
  */
class FileBasedLedgerDataExporter(output: DataOutputStream) extends LedgerDataExporter {
  import FileBasedLedgerDataExporter._

  private val fileLock = new StampedLock

  private val correlationIdMapping = mutable.Map.empty[String, String]
  private val inProgressSubmissions = mutable.Map.empty[String, SubmissionInfo]
  private val bufferedKeyValueDataPerCorrelationId =
    mutable.Map.empty[String, mutable.ListBuffer[(Key, Value)]]

  def addSubmission(
      submissionEnvelope: ByteString,
      correlationId: String,
      recordTimeInstant: Instant,
      participantId: ParticipantId): Unit =
    this.synchronized {
      inProgressSubmissions.put(
        correlationId,
        SubmissionInfo(submissionEnvelope, correlationId, recordTimeInstant, participantId))
      ()
    }

  def addChildTo(parentCorrelationId: String, childCorrelationId: String): Unit =
    this.synchronized {
      correlationIdMapping.put(childCorrelationId, parentCorrelationId)
      ()
    }

  def addKeyValuePairs(correlationId: String, data: Iterable[(Key, Value)]): Unit =
    this.synchronized {
      correlationIdMapping
        .get(correlationId)
        .foreach { parentCorrelationId =>
          val keyValuePairs = bufferedKeyValueDataPerCorrelationId
            .getOrElseUpdate(parentCorrelationId, ListBuffer.empty)
          keyValuePairs.appendAll(data)
          bufferedKeyValueDataPerCorrelationId.put(parentCorrelationId, keyValuePairs)
        }
    }

  def finishedEntry(correlationId: String): Unit = {
    val (submissionInfo, bufferedData) = this.synchronized {
      (
        inProgressSubmissions.get(correlationId),
        bufferedKeyValueDataPerCorrelationId.get(correlationId))
    }
    submissionInfo.foreach { submission =>
      bufferedData.foreach(writeSubmissionData(submission, _))
      this.synchronized {
        inProgressSubmissions.remove(correlationId)
        bufferedKeyValueDataPerCorrelationId.remove(correlationId)
        correlationIdMapping
          .collect {
            case (key, value) if value == correlationId => key
          }
          .foreach(correlationIdMapping.remove)
      }
    }
  }

  private def writeSubmissionData(
      submissionInfo: SubmissionInfo,
      writeSet: ListBuffer[(Key, Value)]): Unit = {
    val lock = fileLock.writeLock()
    Serialization.serializeSubmissionInfo(submissionInfo, output)
    Serialization.serializeWriteSet(writeSet, output)
    fileLock.unlock(lock)
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
