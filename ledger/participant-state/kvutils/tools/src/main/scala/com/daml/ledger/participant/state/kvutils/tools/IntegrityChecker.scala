// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools

import java.io.DataInputStream
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.daml.ledger.on.memory.{InMemoryLedgerStateOperations, Index}
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.kvutils.export.FileBasedLedgerDataExporter.{
  SubmissionInfo,
  WriteSet
}
import com.daml.ledger.participant.state.kvutils.export.{NoopLedgerDataExporter, Serialization}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorFactory,
  BatchedSubmissionValidatorParameters,
  ConflictDetection
}
import com.daml.ledger.validator.{
  CommitStrategy,
  DamlLedgerStateReader,
  StateKeySerializationStrategy
}
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.io.AnsiColor

class IntegrityChecker {
  private implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(
    Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors()))
  private implicit val actorSystem: ActorSystem = ActorSystem("integrity-checker")
  private implicit val materializer: Materializer = Materializer(actorSystem)

  private val stateKeySerializationStrategy = StateKeySerializationStrategy.createDefault()

  def run(input: DataInputStream): Unit = {
    val engine = Engine()
    val metricRegistry = new MetricRegistry
    val metrics = new Metrics(metricRegistry)
    val submissionValidator = BatchedSubmissionValidator[Index](
      BatchedSubmissionValidatorParameters.default,
      new KeyValueCommitting(engine, metrics),
      new ConflictDetection(metrics),
      metrics,
      engine,
      NoopLedgerDataExporter
    )
    val inMemoryState = mutable.Map.empty[Key, Value]
    val inMemoryLog = mutable.ArrayBuffer[LedgerRecord]()
    val inMemoryLedgerStateOperations =
      new InMemoryLedgerStateOperations(inMemoryLog, inMemoryState)
    val writeRecordingLedgerStateOperations =
      new WriteRecordingLedgerStateOperations[Index](inMemoryLedgerStateOperations)
    val (reader, commitStrategy) =
      BatchedSubmissionValidatorFactory.readerAndCommitStrategyFrom(
        writeRecordingLedgerStateOperations,
        stateKeySerializationStrategy,
        NoopLedgerDataExporter)
    processSubmissions(
      input,
      submissionValidator,
      reader,
      commitStrategy,
      writeRecordingLedgerStateOperations)
    reportDetailedMetrics(metricRegistry)
  }

  private def processSubmissions(
      input: DataInputStream,
      submissionValidator: BatchedSubmissionValidator[Index],
      reader: DamlLedgerStateReader,
      commitStrategy: CommitStrategy[Index],
      writeRecordingLedgerStateOperations: WriteRecordingLedgerStateOperations[Index]): Unit = {
    var counter = 0
    while (input.available() > 0) {
      val (submissionInfo, expectedWriteSet) = readSubmissionAndOutputs(input)
      val validationFuture = submissionValidator.validateAndCommit(
        submissionInfo.submissionEnvelope,
        submissionInfo.correlationId,
        submissionInfo.recordTimeInstant,
        submissionInfo.participantId,
        reader,
        commitStrategy
      )
      Await.ready(validationFuture, Duration(10, TimeUnit.SECONDS))
      counter += 1
      val actualWriteSet = writeRecordingLedgerStateOperations.getAndClearRecordedWriteSet()
      val sortedActualWriteSet = actualWriteSet.sortBy(_._1.asReadOnlyByteBuffer())
      compareWriteSets(expectedWriteSet, sortedActualWriteSet)
    }
    println(s"Processed $counter submissions")
  }

  private def compareWriteSets(expectedWriteSet: WriteSet, actualWriteSet: WriteSet): Unit = {
    if (expectedWriteSet == actualWriteSet) {
      println(s"${AnsiColor.GREEN}OK${AnsiColor.WHITE}")
    } else {
      println(s"${AnsiColor.RED}failed")
      if (expectedWriteSet.size == actualWriteSet.size) {
        for (((expectedKey, expectedValue), (actualKey, actualValue)) <- expectedWriteSet.zip(
            actualWriteSet)) {
          if (expectedKey == actualKey && expectedValue != actualValue) {
            println(
              s"expected value: ${bytesAsHexString(expectedValue)} vs. actual value: ${bytesAsHexString(actualValue)}")
            println(detailDifference(expectedKey, expectedValue, actualValue))
          } else if (expectedKey != actualKey) {
            println(
              s"expected key: ${bytesAsHexString(expectedKey)} vs. actual key: ${bytesAsHexString(actualKey)}")
          }
        }
      } else {
        println(s"Expected write-set of size ${expectedWriteSet.size} vs. ${actualWriteSet.size}")
      }
      println(AnsiColor.WHITE)
      sys.exit(1)
    }
  }

  private def detailDifference(key: Key, expectedValue: Value, actualValue: Value): String =
    kvutils.Envelope
      .openStateValue(expectedValue)
      .toOption
      .map { expectedStateValue =>
        val stateKey = stateKeySerializationStrategy.deserializeStateKey(key)
        val actualStateValue = kvutils.Envelope.openStateValue(actualValue)
        s"State key: ${stateKey.toString}\nExpected: ${expectedStateValue.toString}\nvs. actual: ${actualStateValue}"
      }
      .getOrElse {
        // FIXME(miklos): This is dependent on the commit strategy.
        val logEntryId = key
        val expectedLogEntry = kvutils.Envelope.openLogEntry(expectedValue)
        val actualLogEntry = kvutils.Envelope.openLogEntry(actualValue)
        s"Log entry ID: ${bytesAsHexString(logEntryId)}\nExpected: ${expectedLogEntry.toString}\nvs. actual: ${actualLogEntry}"
      }

  private def bytesAsHexString(bytes: ByteString): String =
    bytes.toByteArray.map(byte => "%02x".format(byte)).mkString

  private def readSubmissionAndOutputs(input: DataInputStream): (SubmissionInfo, WriteSet) = {
    val submissionInfo = Serialization.readSubmissionInfo(input)
    val writeSet = Serialization.readWriteSet(input)
    println(
      s"Read submission correlationId=${submissionInfo.correlationId} submissionEnvelopeSize=${submissionInfo.submissionEnvelope
        .size()} writeSetSize=${writeSet.size}")
    (submissionInfo, writeSet)
  }

  private def reportDetailedMetrics(metricRegistry: MetricRegistry): Unit = {
    val reporter = ConsoleReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build
    reporter.report()
  }
}
