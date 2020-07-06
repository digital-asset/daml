// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools

import java.io.DataInputStream
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting
import com.daml.ledger.participant.state.kvutils.export.FileBasedLedgerDataExporter.{
  SubmissionInfo,
  WriteSet
}
import com.daml.ledger.participant.state.kvutils.export.{NoopLedgerDataExporter, Serialization}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorParameters,
  ConflictDetection
}
import com.daml.ledger.validator.{CommitStrategy, DamlLedgerStateReader}
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.io.AnsiColor

class IntegrityChecker[LogResult](commitStrategySupport: CommitStrategySupport[LogResult]) {
  import IntegrityChecker._

  def run(input: DataInputStream)(implicit executionContext: ExecutionContext): Future[Unit] = {
    val actorSystem: ActorSystem = ActorSystem("integrity-checker")
    implicit val materializer: Materializer = Materializer(actorSystem)

    val engine = new Engine(Engine.DevConfig)
    val metricRegistry = new MetricRegistry
    val metrics = new Metrics(metricRegistry)
    val submissionValidator = BatchedSubmissionValidator[LogResult](
      BatchedSubmissionValidatorParameters.default,
      new KeyValueCommitting(engine, metrics),
      new ConflictDetection(metrics),
      metrics,
      engine,
      NoopLedgerDataExporter
    )
    val (reader, commitStrategy, queryableWriteSet) = commitStrategySupport.createComponents()
    processSubmissions(input, submissionValidator, reader, commitStrategy, queryableWriteSet)
      .map { _ =>
        reportDetailedMetrics(metricRegistry)
      }
      .andThen {
        case _ =>
          materializer.shutdown()
          actorSystem.terminate()
      }
  }

  private def processSubmissions(
      input: DataInputStream,
      submissionValidator: BatchedSubmissionValidator[LogResult],
      reader: DamlLedgerStateReader,
      commitStrategy: CommitStrategy[LogResult],
      queryableWriteSet: QueryableWriteSet,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] = {
    def go(counter: Int): Future[Unit] =
      if (input.available() == 0) {
        println(s"Processed $counter submissions")
        Future.unit
      } else {
        val (submissionInfo, expectedWriteSet) = readSubmissionAndOutputs(input)
        for {
          _ <- submissionValidator.validateAndCommit(
            submissionInfo.submissionEnvelope,
            submissionInfo.correlationId,
            submissionInfo.recordTimeInstant,
            submissionInfo.participantId,
            reader,
            commitStrategy,
          )
          actualWriteSet = queryableWriteSet.getAndClearRecordedWriteSet()
          sortedActualWriteSet = actualWriteSet.sortBy(_._1.asReadOnlyByteBuffer())
          _ = if (!compareWriteSets(expectedWriteSet, sortedActualWriteSet)) {
            println(AnsiColor.WHITE)
            sys.exit(1)
          }
          _ <- go(counter + 1)
        } yield ()
      }

    go(counter = 0)
  }

  private def compareWriteSets(expectedWriteSet: WriteSet, actualWriteSet: WriteSet): Boolean = {
    if (expectedWriteSet == actualWriteSet) {
      println(s"${AnsiColor.GREEN}OK${AnsiColor.WHITE}")
      true
    } else {
      println(s"${AnsiColor.RED}FAIL")
      if (expectedWriteSet.size == actualWriteSet.size) {
        for (((expectedKey, expectedValue), (actualKey, actualValue)) <- expectedWriteSet.zip(
            actualWriteSet)) {
          if (expectedKey == actualKey && expectedValue != actualValue) {
            println(
              s"expected value: ${bytesAsHexString(expectedValue)} vs. actual value: ${bytesAsHexString(actualValue)}")
            println(explainDifference(expectedKey, expectedValue, actualValue))
          } else if (expectedKey != actualKey) {
            println(
              s"expected key: ${bytesAsHexString(expectedKey)} vs. actual key: ${bytesAsHexString(actualKey)}")
          }
        }
      } else {
        println(s"Expected write-set of size ${expectedWriteSet.size} vs. ${actualWriteSet.size}")
      }
      false
    }
  }

  private def explainDifference(key: Key, expectedValue: Value, actualValue: Value): String =
    kvutils.Envelope
      .openStateValue(expectedValue)
      .toOption
      .map { expectedStateValue =>
        val stateKey =
          commitStrategySupport.stateKeySerializationStrategy().deserializeStateKey(key)
        val actualStateValue = kvutils.Envelope.openStateValue(actualValue)
        s"State key: $stateKey${System.lineSeparator()}" +
          s"Expected: $expectedStateValue${System.lineSeparator()}Actual: $actualStateValue"
      }
      .getOrElse(commitStrategySupport.explainMismatchingValue(key, expectedValue, actualValue))

  private def readSubmissionAndOutputs(input: DataInputStream): (SubmissionInfo, WriteSet) = {
    val (submissionInfo, writeSet) = Serialization.readEntry(input)
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

object IntegrityChecker {
  def bytesAsHexString(bytes: ByteString): String =
    bytes.toByteArray.map(byte => "%02x".format(byte)).mkString
}
