// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api.impl

import java.time.Clock
import java.util.UUID

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
  DamlSubmission
}
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother

import scala.collection.JavaConverters._
import scala.collection.{breakOut, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.ByteString

import scala.util.Random

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
private[impl] case class LogEntry(entryId: DamlLogEntryId, payload: Array[Byte])

private[impl] case class InMemoryState(
    log: mutable.Buffer[LogEntry] = ArrayBuffer[LogEntry](),
    state: mutable.Map[ByteString, DamlStateValue] = mutable.Map.empty)

class InMemoryLedgerReaderWriter(
    ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    val participantId: ParticipantId)(implicit executionContext: ExecutionContext)
    extends LedgerWriter
    with LedgerReader {

  private val engine = Engine()

  private val currentState = InMemoryState()

  private val StartOffset: Int = 0

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] =
    Future {
      val submission = Envelope
        .openSubmission(envelope)
        .getOrElse(throw new IllegalArgumentException("Not a valid submission in envelope"))
      currentState.synchronized {
        val stateInputs: Map[DamlStateKey, Option[DamlStateValue]] =
          submission.getInputDamlStateList.asScala
            .map(key => key -> currentState.state.get(key.toByteString))(breakOut)
        val entryId = allocateEntryId()
        val (logEntry, damlStateUpdates) =
          KeyValueCommitting.processSubmission(
            engine,
            entryId,
            currentRecordTime(),
            LedgerReader.DefaultTimeModel,
            submission,
            participantId,
            stateInputs
          )
        verifyStateUpdatesAgainstPreDeclaredOutputs(damlStateUpdates, entryId, submission)
        val stateUpdates = damlStateUpdates.toSeq.map {
          case (damlStateKey, value) => damlStateKey.toByteString -> value
        }
        currentState.log += LogEntry(entryId, Envelope.enclose(logEntry).toByteArray)
        currentState.state ++= stateUpdates
        dispatcher.signalNewHead(currentState.log.size)
      }
      SubmissionResult.Acknowledged
    }

  private def verifyStateUpdatesAgainstPreDeclaredOutputs(
      actualStateUpdates: Map[DamlStateKey, DamlStateValue],
      entryId: DamlLogEntryId,
      submission: DamlSubmission): Unit = {
    val expectedStateUpdates = KeyValueCommitting.submissionOutputs(entryId, submission)
    if (!(actualStateUpdates.keySet subsetOf expectedStateUpdates)) {
      val unaccountedKeys = actualStateUpdates.keySet diff expectedStateUpdates
      sys.error(
        s"State updates not a subset of expected updates! Keys [$unaccountedKeys] are unaccounted for!")
    }
  }

  override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        offset
          .map(_.components.head.toInt)
          .getOrElse(StartOffset),
        OneAfterAnother[Int, List[LedgerRecord]](
          (index: Int, _) => index + 1,
          (index: Int) => Future.successful(List(retrieveLogEntry(index)))
        )
      )
      .mapConcat { case (_, updates) => updates }

  override def retrieveLedgerId(): LedgerId = ledgerId

  override def currentHealth(): HealthStatus = Healthy

  private val dispatcher: Dispatcher[Int] =
    Dispatcher("in-memory-key-value-participant-state", zeroIndex = 0, headAtInitialization = 0)

  private val randomNumberGenerator = new Random()

  private val NamespaceLogEntries = ByteString.copyFromUtf8("L")

  private def allocateEntryId(): DamlLogEntryId = {
    val nonce: Array[Byte] = Array.ofDim(8)
    randomNumberGenerator.nextBytes(nonce)
    DamlLogEntryId.newBuilder
      .setEntryId(NamespaceLogEntries.concat(ByteString.copyFrom(nonce)))
      .build
  }

  private def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  private def retrieveLogEntry(index: Int): LedgerRecord = {
    val logEntry = currentState.log(index)
    LedgerRecord(Offset(Array(index.toLong)), logEntry.entryId, logEntry.payload)
  }
}
