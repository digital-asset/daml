// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Clock

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.{
  Envelope,
  KeyValueCommitting,
  SequentialLogEntryId,
}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import com.digitalasset.resources.ResourceOwner
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{breakOut, mutable}
import scala.concurrent.{ExecutionContext, Future}

private[memory] class LogEntry(val entryId: DamlLogEntryId, val payload: Array[Byte])

private[memory] object LogEntry {
  def apply(entryId: DamlLogEntryId, payload: Array[Byte]): LogEntry =
    new LogEntry(entryId, payload)
}

private[memory] class InMemoryState(
    val log: mutable.Buffer[LogEntry] = ArrayBuffer[LogEntry](),
    val state: mutable.Map[ByteString, DamlStateValue] = mutable.Map.empty,
)

final class InMemoryLedgerReaderWriter(
    ledgerId: LedgerId,
    override val participantId: ParticipantId,
    dispatcher: Dispatcher[Index],
)(implicit executionContext: ExecutionContext)
    extends LedgerWriter
    with LedgerReader {

  private val engine = Engine()

  private val currentState = new InMemoryState()

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] =
    Future {
      val submission = Envelope
        .openSubmission(envelope)
        .getOrElse(throw new IllegalArgumentException("Not a valid submission in envelope"))
      currentState.synchronized {
        val stateInputs: Map[DamlStateKey, Option[DamlStateValue]] =
          submission.getInputDamlStateList.asScala
            .map(key => key -> currentState.state.get(key.toByteString))(breakOut)
        val entryId = sequentialLogEntryId.next()
        val (logEntry, damlStateUpdates) =
          KeyValueCommitting.processSubmission(
            engine,
            entryId,
            currentRecordTime(),
            LedgerReader.DefaultConfiguration,
            submission,
            participantId,
            stateInputs,
          )
        val stateUpdates = damlStateUpdates.map {
          case (damlStateKey, value) => damlStateKey.toByteString -> value
        }
        currentState.log += LogEntry(entryId, Envelope.enclose(logEntry).toByteArray)
        currentState.state ++= stateUpdates
        dispatcher.signalNewHead(currentState.log.size)
      }
      SubmissionResult.Acknowledged
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
    val logEntry = currentState.log(index)
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
      dispatcher <- ResourceOwner.forCloseable(() =>
        Dispatcher(
          "in-memory-key-value-participant-state",
          zeroIndex = StartIndex,
          headAtInitialization = StartIndex,
        ),
      )
    } yield new InMemoryLedgerReaderWriter(ledgerId, participantId, dispatcher)
}
