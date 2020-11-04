// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.on.memory.Index
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting
import com.daml.ledger.participant.state.kvutils.`export`.ProtobufBasedLedgerDataImporter
import com.daml.ledger.participant.state.kvutils.export.{
  LedgerDataImporter,
  NoOpLedgerDataExporter,
  WriteSet
}
import com.daml.ledger.participant.state.v1.{ParticipantId, ReadService}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorParameters,
  ConflictDetection
}
import com.daml.ledger.validator.{CommitStrategy, DamlLedgerStateReader}
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode, JdbcIndexer}
import com.daml.platform.store.dao.events.LfValueTranslation
import com.google.protobuf.ByteString

import scala.PartialFunction.condOpt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success}

class IntegrityChecker[LogResult](commitStrategySupport: CommitStrategySupport[LogResult]) {

  import IntegrityChecker._

  def run(
      importer: LedgerDataImporter,
      config: Config,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): Future[Unit] = {
    if (!config.performByteComparison) {
      println("Skipping byte-for-byte comparison.".yellow)
      println()
    }

    val engine = new Engine(EngineConfig.Stable)
    val metricRegistry = new MetricRegistry
    val metrics = new Metrics(metricRegistry)
    val submissionValidator = BatchedSubmissionValidator[LogResult](
      params = BatchedSubmissionValidatorParameters(cpuParallelism = 1, readParallelism = 1),
      committer = new KeyValueCommitting(engine, metrics),
      conflictDetection = new ConflictDetection(metrics),
      metrics = metrics,
      ledgerDataExporter = NoOpLedgerDataExporter,
    )
    val expectedReadServiceFactory = commitStrategySupport.newReadServiceFactory()
    val actualReadServiceFactory = commitStrategySupport.newReadServiceFactory()
    val stateUpdates = new StateUpdates(
      expectedReadServiceFactory.getReadService,
      actualReadServiceFactory.getReadService,
    )
    checkIntegrity(
      config,
      importer,
      submissionValidator,
      expectedReadServiceFactory,
      actualReadServiceFactory,
      stateUpdates,
      metrics).andThen {
      case _ =>
        reportDetailedMetrics(metricRegistry)
    }
  }

  private def checkIntegrity(
      config: Config,
      importer: LedgerDataImporter,
      submissionValidator: BatchedSubmissionValidator[LogResult],
      expectedReadServiceFactory: ReplayingReadServiceFactory,
      actualReadServiceFactory: ReplayingReadServiceFactory,
      stateUpdates: StateUpdates,
      metrics: Metrics)(
      implicit executionContext: ExecutionContext,
      materializer: Materializer): Future[Unit] = {
    for {
      _ <- processSubmissions(
        importer,
        submissionValidator,
        commitStrategySupport.ledgerStateReader,
        commitStrategySupport.commitStrategy,
        commitStrategySupport.writeSet,
        expectedReadServiceFactory,
        actualReadServiceFactory,
        config,
      )
      _ <- stateUpdates.compare()
      _ <- indexStateUpdates(config.name, metrics, actualReadServiceFactory.getReadService)
    } yield ()
  }

  private def indexStateUpdates(
      name: String,
      metrics: Metrics,
      readService: ReplayingReadService,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] = {
    val jdbcUrl = s"jdbc:h2:mem:$name;db_close_delay=-1;db_close_on_exit=false"
    val config = IndexerConfig(
      participantId = ParticipantId.assertFromString("IntegrityCheckerParticipant"),
      jdbcUrl = jdbcUrl,
      startupMode = IndexerStartupMode.MigrateAndStart,
    )

    implicit val resourceContext: ResourceContext = ResourceContext(executionContext)

    // Start the indexer consuming the recorded state updates
    println(s"Starting to index ${readService.updateCount()} updates.".white)
    newLoggingContext { implicit loggingContext =>
      val feedHandleResourceOwner = for {
        indexerFactory <- ResourceOwner
          .forFuture(() =>
            migrateAndStartIndexer(config, readService, metrics, LfValueTranslation.Cache.none))
        indexer <- indexerFactory
        feedHandle <- indexer.subscription(readService)
      } yield feedHandle

      // Wait for the indexer to finish consuming the state updates.
      // This works because ReplayingReadService.stateUpdates() closes the update stream
      // when it is done streaming the recorded updates, and IndexFeedHandle.complete()
      // completes when it finishes consuming the state update stream.
      // Any failure (e.g., during the decoding of the recorded state updates, or
      // during the indexing of a state update) will result in a failed Future.
      feedHandleResourceOwner.use(_.completed())
    }.transform {
      case Success(value) =>
        println(s"Successfully indexed all updates.".green)
        println()
        Success(value)
      case Failure(exception) =>
        val message =
          s"""Failure indexing updates: $exception
             |Indexer metrics:
             |  stateUpdateProcessing:  ${metrics.daml.indexer.stateUpdateProcessing.getCount}
             |  lastReceivedRecordTime: ${metrics.daml.indexer.lastReceivedRecordTime.getValue()}
             |  lastReceivedOffset:     ${metrics.daml.indexer.lastReceivedOffset.getValue()}
             |""".stripMargin
        Failure(new IndexingFailureException(message))
    }
  }

  private def processSubmissions(
      importer: LedgerDataImporter,
      submissionValidator: BatchedSubmissionValidator[LogResult],
      reader: DamlLedgerStateReader,
      commitStrategy: CommitStrategy[LogResult],
      queryableWriteSet: QueryableWriteSet,
      expectedReadServiceFactory: ReplayingReadServiceFactory,
      actualReadServiceFactory: ReplayingReadServiceFactory,
      config: Config,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] = {
    println(s"Importing the ledger export.".white)
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
            orderedActualWriteSet = if (config.sortWriteSet) {
              actualWriteSet.sortBy(_._1.asReadOnlyByteBuffer())
            } else {
              actualWriteSet
            }
            _ = expectedReadServiceFactory.appendBlock(expectedWriteSet)
            _ = actualReadServiceFactory.appendBlock(orderedActualWriteSet)
            _ = if (config.performByteComparison) {
              compareWriteSets(expectedWriteSet, orderedActualWriteSet)
            } else {
              ()
            }
          } yield ()
      }
      .runWith(Sink.fold(0)((n, _) => n + 1))
      .map { counter =>
        println(s"Processed $counter submissions.".green)
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
          commitStrategySupport.stateKeySerializationStrategy.deserializeStateKey(key)
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

  private def migrateAndStartIndexer(
      config: IndexerConfig,
      readService: ReadService,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslation.Cache,
  )(
      implicit resourceContext: ResourceContext,
      materializer: Materializer,
      loggingContext: LoggingContext): Future[ResourceOwner[JdbcIndexer]] = {
    val indexerFactory = new JdbcIndexer.Factory(
      ServerRole.Indexer,
      config,
      readService,
      metrics,
      lfValueTranslationCache,
    )
    indexerFactory.migrateSchema(allowExistingSchema = false)
  }
}

object IntegrityChecker {
  def bytesAsHexString(bytes: ByteString): String =
    bytes.toByteArray.map(byte => "%02x".format(byte)).mkString

  class CheckFailedException(message: String) extends RuntimeException(message)

  class ComparisonFailureException(lines: String*)
      extends CheckFailedException(("FAIL" +: lines).mkString(System.lineSeparator))

  class IndexingFailureException(message: String) extends CheckFailedException(message)

  def run(
      args: Array[String],
      commitStrategySupportFactory: ExecutionContext => CommitStrategySupport[Index],
  ): Unit = {
    val config = Config.parse(args).getOrElse { sys.exit(1) }

    run(config, commitStrategySupportFactory).failed
      .foreach {
        case exception: IntegrityChecker.CheckFailedException =>
          println(exception.getMessage.red)
          sys.exit(1)
        case exception =>
          exception.printStackTrace()
          sys.exit(1)
      }(DirectExecutionContext)
  }

  private def run(
      config: Config,
      commitStrategySupportFactory: ExecutionContext => CommitStrategySupport[Index],
  ): Future[Unit] = {
    println(s"Verifying integrity of ${config.exportFilePath}...")

    val actorSystem: ActorSystem = ActorSystem("integrity-checker")
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
    implicit val materializer: Materializer = Materializer(actorSystem)

    val importer = ProtobufBasedLedgerDataImporter(config.exportFilePath)
    new IntegrityChecker(commitStrategySupportFactory(executionContext))
      .run(importer, config)
      .andThen {
        case _ =>
          sys.exit(0)
      }(DirectExecutionContext)
  }
}
