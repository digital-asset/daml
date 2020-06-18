package com.daml.ledger.participant.state.kvutils.export

import java.io.{DataOutputStream, FileOutputStream}
import java.time.Instant
import java.util.concurrent.locks.StampedLock

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * If ledger dumps are not enabled doesn't do anything.
  * This class is thread-safe.
  */
object FileBasedLedgerDataExporter extends LedgerDataExporter {
  case class SubmissionInfo(
      submissionEnvelope: ByteString,
      correlationId: String,
      recordTimeInstant: Instant,
      participantId: ParticipantId)

  type WriteSet = Seq[(Key, Value)]

  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val optLedgerDumpStream: Option[DataOutputStream] = {
    //Option(System.getenv("KVUTILS_LEDGER_DUMP"))
    Option("/tmp/ledger_dump_new3")
      .map { filename =>
        logger.info(s"Enabled writing ledger entries to $filename")
        new DataOutputStream(new FileOutputStream(filename))
      }
  }

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
    optLedgerDumpStream.foreach { _ =>
      this.synchronized {
        inProgressSubmissions.put(
          correlationId,
          SubmissionInfo(submissionEnvelope, correlationId, recordTimeInstant, participantId))
      }
    }

  def addChildTo(parentCorrelationId: String, childCorrelationId: String): Unit =
    optLedgerDumpStream.foreach { _ =>
      this.synchronized {
        correlationIdMapping.put(childCorrelationId, parentCorrelationId)
      }
    }

  //TODO(miklos): Add support for storing ACLs too.
  def addKeyValuePairs(correlationId: String, data: Iterable[(Key, Value)]): Unit =
    optLedgerDumpStream.foreach { _ =>
      this.synchronized {
        correlationIdMapping
          .get(correlationId)
          .map { parentCorrelationId =>
            val keyValuePairs = bufferedKeyValueDataPerCorrelationId
              .getOrElseUpdate(parentCorrelationId, ListBuffer.empty)
            keyValuePairs.appendAll(data)
            bufferedKeyValueDataPerCorrelationId.put(parentCorrelationId, keyValuePairs)
          }
      }
    }

  def finishedEntry(correlationId: String): Unit = optLedgerDumpStream.foreach { _ =>
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
      writeSet: ListBuffer[(Key, Value)]): Unit =
    optLedgerDumpStream.foreach { out =>
      val lock = fileLock.writeLock()
      Serialization.serializeSubmissionInfo(submissionInfo, out)
      Serialization.serializeWriteSet(writeSet, out)
      fileLock.unlock(lock)
    }
}
