// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Clock

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.{KeyValueCommitting, SequentialLogEntryId}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator.ValidationResult.{
  MissingInputState,
  SubmissionValidated,
  ValidationError
}
import com.daml.ledger.validator._
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import com.digitalasset.resources.ResourceOwner
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

private[memory] class LogEntry(val entryId: DamlLogEntryId, val payload: Array[Byte])

private[memory] object LogEntry {
  def apply(entryId: DamlLogEntryId, payload: Array[Byte]): LogEntry =
    new LogEntry(entryId, payload)
}

private[memory] class InMemoryState(
    val log: mutable.Buffer[LogEntry] = ArrayBuffer[LogEntry](),
    val state: mutable.Map[ByteString, Array[Byte]] = mutable.Map.empty,
)

final class InMemoryLedgerReaderWriter(
    ledgerId: LedgerId,
    override val participantId: ParticipantId,
    dispatcher: Dispatcher[Index],
)(implicit executionContext: ExecutionContext)
    extends LedgerWriter
    with LedgerReader {

  private val currentState = new InMemoryState()

  private class InMemoryLedgerStateAccess(theParticipantId: ParticipantId)
      extends LedgerStateAccess {
    override def inTransaction[T](body: LedgerStateOperations => Future[T]): Future[T] =
      Future {
        currentState.state.synchronized {
          body(new InMemoryLedgerStateOperations)
        }
      }.flatten

    override def participantId: String = theParticipantId
  }

  private class InMemoryLedgerStateOperations extends LedgerStateOperations {
    override def readState(key: Array[Byte]): Future[Option[Array[Byte]]] = Future.successful {
      currentState.state.get(ByteString.copyFrom(key))
    }

    override def writeState(keyValuePairs: Seq[(Array[Byte], Array[Byte])]): Future[Unit] =
      Future.successful {
        currentState.state ++= keyValuePairs.map {
          case (keyBytes, valueBytes) => ByteString.copyFrom(keyBytes) -> valueBytes
        }
      }

    override def appendToLog(key: Array[Byte], value: Array[Byte]): Future[Unit] =
      Future.successful {
        val damlLogEntryId = KeyValueCommitting.unpackDamlLogEntryId(key)
        val logEntry = LogEntry(damlLogEntryId, value)
        val newHead = currentState.log.synchronized {
          currentState.log += logEntry
          currentState.log.size
        }
        dispatcher.signalNewHead(newHead)
      }
  }

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] = {
    val validator = SubmissionValidator.create(
      new InMemoryLedgerStateAccess(participantId),
      () => sequentialLogEntryId.next())
    validator
      .validateAndCommit(envelope, correlationId, currentRecordTime())
      .map {
        case SubmissionValidated => SubmissionResult.Acknowledged
        case MissingInputState(_) => SubmissionResult.InternalError("Missing input state")
        case ValidationError(reason) => SubmissionResult.InternalError(reason)
      }
  }

  override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        offset
          .map(_.components.head.toInt)
          .getOrElse(StartIndex),
        OneAfterAnother[Int, List[LedgerRecord]](
          (index: Int, _) => index + 1,
          (index: Int) => Future.successful(List(retrieveLogEntry(index))),
        ),
      )
      .mapConcat { case (_, updates) => updates }

  override def retrieveLedgerId(): LedgerId = ledgerId

  override def currentHealth(): HealthStatus = Healthy

  private def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  private def retrieveLogEntry(index: Int): LedgerRecord = {
    val logEntry = currentState.log.synchronized {
      currentState.log(index)
    }
    LedgerRecord(Offset(Array(index.toLong)), logEntry.entryId, logEntry.payload)
  }
}

object InMemoryLedgerReaderWriter {
  type Index = Int

  private val StartIndex: Index = 0

  private val NamespaceLogEntries = "L"

  private val sequentialLogEntryId = new SequentialLogEntryId(NamespaceLogEntries)

  def owner(
      ledgerId: LedgerId,
      participantId: ParticipantId,
  )(implicit executionContext: ExecutionContext): ResourceOwner[InMemoryLedgerReaderWriter] =
    for {
      dispatcher <- ResourceOwner.forCloseable(
        () =>
          Dispatcher(
            "in-memory-key-value-participant-state",
            zeroIndex = StartIndex,
            headAtInitialization = StartIndex,
        ))
    } yield new InMemoryLedgerReaderWriter(ledgerId, participantId, dispatcher)
}
