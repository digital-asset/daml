// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting
import com.daml.ledger.participant.state.kvutils.export.{
  LedgerDataImporter,
  NoOpLedgerDataExporter,
  WriteSet
}
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.Color.color
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorParameters,
  ConflictDetection
}
import com.daml.ledger.validator.{CommitStrategy, DamlLedgerStateReader}
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString

import scala.PartialFunction.condOpt
import scala.concurrent.{ExecutionContext, Future}

class IntegrityChecker[LogResult](commitStrategySupport: CommitStrategySupport[LogResult]) {

  import IntegrityChecker._

  def run(
      importer: LedgerDataImporter,
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    val actorSystem: ActorSystem = ActorSystem("integrity-checker")
    implicit val materializer: Materializer = Materializer(actorSystem)

    val engine = new Engine(EngineConfig.Stable)
    val metricRegistry = new MetricRegistry
    val metrics = new Metrics(metricRegistry)
    val submissionValidator = BatchedSubmissionValidator[LogResult](
      BatchedSubmissionValidatorParameters.reasonableDefault,
      new KeyValueCommitting(engine, metrics),
      new ConflictDetection(metrics),
      metrics,
      NoOpLedgerDataExporter,
    )
    val ComponentsForReplay(reader, commitStrategy, queryableWriteSet) =
      commitStrategySupport.createComponentsForReplay()
    processSubmissions(importer, submissionValidator, reader, commitStrategy, queryableWriteSet)
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
      importer: LedgerDataImporter,
      submissionValidator: BatchedSubmissionValidator[LogResult],
      reader: DamlLedgerStateReader,
      commitStrategy: CommitStrategy[LogResult],
      queryableWriteSet: QueryableWriteSet,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] = {
    Source(importer.read())
      .mapAsync(1) {
        case (submissionInfo, expectedWriteSet) =>
          println(
            "Read submission"
              + s" correlationId=${submissionInfo.correlationId}"
              + s" submissionEnvelopeSize=${submissionInfo.submissionEnvelope.size()}"
              + s" writeSetSize=${expectedWriteSet.size}"
          )
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
            _ = compareWriteSets(expectedWriteSet, actualWriteSet)
          } yield ()
      }
      .runWith(Sink.fold(0)((n, _) => n + 1))
      .map { counter =>
        println(s"Processed $counter submissions.".white)
        println()
      }
  }

  private def compareWriteSets(expectedWriteSet: WriteSet, actualWriteSet: WriteSet): Unit =
    if (expectedWriteSet == actualWriteSet) {
      println("OK".green)
    } else {
      val messageMaybe =
        if (expectedWriteSet.size == actualWriteSet.size) {
          compareSameSizeWriteSets(expectedWriteSet, actualWriteSet)
        } else {
          Some(s"Expected write-set of size ${expectedWriteSet.size} vs. ${actualWriteSet.size}")
        }
      messageMaybe.foreach { message =>
        println("FAIL".red)
        throw new ComparisonFailureException(message)
      }
    }

  private[tools] def compareSameSizeWriteSets(
      expectedWriteSet: WriteSet,
      actualWriteSet: WriteSet): Option[String] = {
    val differencesExplained = expectedWriteSet
      .zip(actualWriteSet)
      .map {
        case ((expectedKey, expectedValue), (actualKey, actualValue)) =>
          if (expectedKey == actualKey && expectedValue != actualValue) {
            explainDifference(expectedKey, expectedValue, actualValue).map { explainedDifference =>
              Seq(
                s"expected value:    ${bytesAsHexString(expectedValue)}",
                s" vs. actual value: ${bytesAsHexString(actualValue)}",
                explainedDifference,
              )
            }
          } else if (expectedKey != actualKey) {
            Some(
              Seq(
                s"expected key:    ${bytesAsHexString(expectedKey)}",
                s" vs. actual key: ${bytesAsHexString(actualKey)}",
              ))
          } else {
            None
          }
      }
      .map(_.toList)
      .filterNot(_.isEmpty)
      .flatten
      .flatten
      .mkString(System.lineSeparator())
    condOpt(differencesExplained.isEmpty) {
      case false => differencesExplained
    }
  }

  private def explainDifference(
      key: Key,
      expectedValue: Value,
      actualValue: Value): Option[String] =
    kvutils.Envelope
      .openStateValue(expectedValue)
      .toOption
      .map { expectedStateValue =>
        val stateKey =
          commitStrategySupport.stateKeySerializationStrategy().deserializeStateKey(key)
        val actualStateValue = kvutils.Envelope.openStateValue(actualValue)
        s"""|State key: $stateKey
            |Expected: $expectedStateValue
            |Actual: $actualStateValue""".stripMargin
      }
      .orElse(commitStrategySupport.explainMismatchingValue(key, expectedValue, actualValue))

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

  class CheckFailedException(message: String) extends RuntimeException(message)

  class ComparisonFailureException(message: String) extends CheckFailedException(message)

}
