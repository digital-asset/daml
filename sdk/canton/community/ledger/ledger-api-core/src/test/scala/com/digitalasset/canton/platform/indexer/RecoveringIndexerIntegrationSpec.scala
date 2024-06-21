// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.daml.lf.data.Ref.{Party, SubmissionId}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state.{
  InternalStateServiceProviderImpl,
  ReadService,
  SubmissionResult,
  Update,
  WritePartyService,
}
import com.digitalasset.canton.ledger.resources.TestResourceContext
import com.digitalasset.canton.logging.{
  LoggingContextWithTrace,
  NamedLogging,
  SuppressingLogger,
  SuppressionRule,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.LedgerApiServer
import com.digitalasset.canton.platform.config.{
  CommandServiceConfig,
  IndexServiceConfig,
  ServerRole,
}
import com.digitalasset.canton.platform.indexer.IndexerStartupMode.MigrateAndStart
import com.digitalasset.canton.platform.indexer.RecoveringIndexerIntegrationSpec.*
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.DbSupport.{
  ConnectionPoolConfig,
  DbConfig,
  ParticipantDataSourceConfig,
}
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.tracing.TraceContext.{withNewTraceContext, wrapWithNewTraceContext}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, Traced}
import com.digitalasset.canton.{HasExecutionContext, config}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Keep, Source}
import org.apache.pekko.stream.{
  BoundedSourceQueue,
  Materializer,
  QueueCompletionResult,
  QueueOfferResult,
}
import org.mockito.Mockito.*
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterEach, Succeeded}
import org.slf4j.event.Level

import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CompletionStage, Executors}
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.{CompletionStageOps, FutureOps}

class RecoveringIndexerIntegrationSpec
    extends AsyncWordSpec
    with Matchers
    with TestResourceContext
    with Eventually
    with IntegrationPatience
    with BeforeAndAfterEach
    with MockitoSugar
    with NamedLogging
    with HasExecutionContext {

  private[this] var testId: UUID = _
  private implicit val loggingContext: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)
  val tracer: Tracer = NoReportingTracerProvider.tracer

  override def beforeEach(): Unit = {
    super.beforeEach()
    testId = UUID.randomUUID()
  }

  val RecoveringIndexerSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains(s"${classOf[RecoveringIndexer].getSimpleName}:")

  private def assertLogsInOrder(expected: Seq[(Level, String)]): Assertion = {
    loggerFactory.fetchRecordedLogEntries
      .map(entry => entry.level -> entry.message) should contain theSameElementsInOrderAs expected
    Succeeded
  }

  "indexer" should {
    "index the participant state" in withNewTraceContext { implicit traceContext =>
      loggerFactory.suppress(RecoveringIndexerSuppressionRule) {
        participantServer(InMemoryPartyParticipantState)
          .use { case (participantState, dbSupport) =>
            for {
              _ <- participantState
                .allocateParty(
                  hint = Some(Ref.Party.assertFromString("alice")),
                  displayName = Some("Alice"),
                  submissionId = randomSubmissionId(),
                )
                .asScala
              _ <- eventuallyPartiesShouldBe(dbSupport, "Alice")
            } yield ()
          }
          .map { _ =>
            assertLogsInOrder(
              Seq(
                Level.INFO -> "Starting Indexer Server",
                Level.INFO -> "Started Indexer Server",
                Level.INFO -> "Stopping Indexer Server",
                Level.INFO -> "Successfully finished processing state updates",
                Level.INFO -> "Stopped Indexer Server",
              )
            )
          }
      }
    }

    "index the participant state, even on spurious failures" in withNewTraceContext {
      implicit traceContext =>
        loggerFactory.suppress(RecoveringIndexerSuppressionRule) {
          participantServer(ParticipantStateThatFailsOften)
            .use { case (participantState, dbSupport) =>
              for {
                _ <- participantState
                  .allocateParty(
                    hint = Some(Ref.Party.assertFromString("alice")),
                    displayName = Some("Alice"),
                    submissionId = randomSubmissionId(),
                  )
                  .asScala
                _ <- participantState
                  .allocateParty(
                    hint = Some(Ref.Party.assertFromString("bob")),
                    displayName = Some("Bob"),
                    submissionId = randomSubmissionId(),
                  )
                  .asScala
                _ <- participantState
                  .allocateParty(
                    hint = Some(Ref.Party.assertFromString("carol")),
                    displayName = Some("Carol"),
                    submissionId = randomSubmissionId(),
                  )
                  .asScala
                _ <- eventuallyPartiesShouldBe(dbSupport, "Alice", "Bob", "Carol")
              } yield ()
            }
            .map { _ =>
              assertLogsInOrder(
                Seq(
                  Level.INFO -> "Starting Indexer Server",
                  Level.INFO -> "Started Indexer Server",
                  Level.ERROR -> "Error while running indexer, restart scheduled after 100 milliseconds",
                  Level.INFO -> "Restarting Indexer Server",
                  Level.INFO -> "Restarted Indexer Server",
                  Level.ERROR -> "Error while running indexer, restart scheduled after 100 milliseconds",
                  Level.INFO -> "Restarting Indexer Server",
                  Level.INFO -> "Restarted Indexer Server",
                  Level.ERROR -> "Error while running indexer, restart scheduled after 100 milliseconds",
                  Level.INFO -> "Restarting Indexer Server",
                  Level.INFO -> "Restarted Indexer Server",
                  Level.INFO -> "Stopping Indexer Server",
                  Level.INFO -> "Successfully finished processing state updates",
                  Level.INFO -> "Stopped Indexer Server",
                )
              )
            }
        }
    }

    "stop when the kill switch is hit after a failure" in withNewTraceContext {
      implicit traceContext =>
        loggerFactory.suppress(RecoveringIndexerSuppressionRule) {
          participantServer(
            ParticipantStateThatFailsOften,
            restartDelay = config.NonNegativeFiniteDuration.ofSeconds(10),
          )
            .use { case (participantState, _) =>
              for {
                _ <- participantState
                  .allocateParty(
                    hint = Some(Ref.Party.assertFromString("alice")),
                    displayName = Some("Alice"),
                    submissionId = randomSubmissionId(),
                  )
                  .asScala
              } yield {
                eventually(
                  loggerFactory.fetchRecordedLogEntries
                    .map(entry => entry.level -> entry.message)
                    .take(3) should contain theSameElementsInOrderAs Seq(
                    Level.INFO -> "Starting Indexer Server",
                    Level.INFO -> "Started Indexer Server",
                    Level.ERROR -> "Error while running indexer, restart scheduled after 10 seconds",
                  )
                )
                Instant.now()
              }
            }
            .map { timeBeforeStop =>
              val timeAfterStop = Instant.now()
              SECONDS.between(timeBeforeStop, timeAfterStop) should be <= 5L
              // stopping the server and logging the error can happen in either order
              assertLogsInOrder(
                Seq(
                  Level.INFO -> "Starting Indexer Server",
                  Level.INFO -> "Started Indexer Server",
                  Level.ERROR -> "Error while running indexer, restart scheduled after 10 seconds",
                  Level.INFO -> "Stopping Indexer Server",
                  Level.INFO -> "Indexer Server was stopped; cancelling the restart",
                  Level.INFO -> "Stopped Indexer Server",
                )
              )
            }
        }
    }
  }

  private def participantServer(
      newParticipantState: ParticipantStateFactory,
      restartDelay: config.NonNegativeFiniteDuration =
        config.NonNegativeFiniteDuration.ofMillis(100),
  )(implicit traceContext: TraceContext): ResourceOwner[(WritePartyService, DbSupport)] = {
    val participantId = Ref.ParticipantId.assertFromString(s"participant-$testId")
    val jdbcUrl =
      s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase()}-$testId;db_close_delay=-1;db_close_on_exit=false"
    val metrics = LedgerApiServerMetrics.ForTesting
    val participantDataSourceConfig = ParticipantDataSourceConfig(jdbcUrl)
    val indexerConfig = IndexerConfig(restartDelay = restartDelay)
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem())
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      participantState <- newParticipantState(participantId)(materializer, traceContext)
      servicesExecutionContext <- ResourceOwner
        .forExecutorService(() => Executors.newWorkStealingPool())
        .map(ExecutionContext.fromExecutorService)
      (inMemoryState, inMemoryStateUpdaterFlow) <-
        LedgerApiServer
          .createInMemoryStateAndUpdater(
            IndexServiceConfig(),
            CommandServiceConfig.DefaultMaxCommandsInFlight,
            metrics,
            parallelExecutionContext,
            tracer,
            loggerFactory,
          )
      dbSupport <- DbSupport
        .owner(
          serverRole = ServerRole.Testing(getClass),
          metrics = metrics,
          dbConfig = DbConfig(
            jdbcUrl,
            connectionPool = ConnectionPoolConfig(
              connectionPoolSize = 16,
              connectionTimeout = 250.millis,
            ),
          ),
          loggerFactory = loggerFactory,
        )
      _ <- new IndexerServiceOwner(
        readService = participantState._1,
        participantId = participantId,
        config = indexerConfig,
        metrics = metrics,
        participantDataSourceConfig = participantDataSourceConfig,
        inMemoryState = inMemoryState,
        inMemoryStateUpdaterFlow = inMemoryStateUpdaterFlow,
        executionContext = servicesExecutionContext,
        tracer = tracer,
        loggerFactory = loggerFactory,
        startupMode = MigrateAndStart,
        dataSourceProperties = IndexerConfig.createDataSourcePropertiesForTesting(
          indexerConfig.ingestionParallelism.unwrap
        ),
        highAvailability = HaConfig(),
        indexServiceDbDispatcher = Some(dbSupport.dbDispatcher),
      )(materializer, traceContext)
    } yield (participantState._2, dbSupport)
  }

  private def eventuallyPartiesShouldBe(dbSupport: DbSupport, partyNames: String*)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    val metrics = LedgerApiServerMetrics.ForTesting
    val ledgerEndCache = MutableLedgerEndCache()
    val storageBackendFactory = dbSupport.storageBackendFactory
    val partyStorageBacked = storageBackendFactory.createPartyStorageBackend(ledgerEndCache)
    val parameterStorageBackend = storageBackendFactory.createParameterStorageBackend
    val dbDispatcher = dbSupport.dbDispatcher

    eventually {
      for {
        ledgerEnd <- dbDispatcher
          .executeSql(metrics.index.db.getLedgerEnd)(parameterStorageBackend.ledgerEnd)
        _ = ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
        knownParties <- dbDispatcher
          .executeSql(metrics.index.db.loadAllParties)(
            partyStorageBacked.knownParties(None, 10)
          )
      } yield {
        knownParties.map(_.displayName) shouldBe partyNames.map(Some(_))
        ()
      }
    }
  }
}

object RecoveringIndexerIntegrationSpec {

  private type ParticipantState = (ReadService, WritePartyService)

  private def randomSubmissionId() =
    Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)

  private trait ParticipantStateFactory {
    def apply(participantId: Ref.ParticipantId)(implicit
        materializer: Materializer,
        traceContext: TraceContext,
    ): ResourceOwner[ParticipantState]
  }

  private object InMemoryPartyParticipantState extends ParticipantStateFactory {
    override def apply(participantId: Ref.ParticipantId)(implicit
        materializer: Materializer,
        traceContext: TraceContext,
    ): ResourceOwner[ParticipantState] = {
      ResourceOwner
        .forReleasable(() =>
          // required for the indexer to resubscribe to the update source
          Source
            .queue[(Offset, Traced[Update])](bufferSize = 16)
            .toMat(BroadcastHub.sink)(Keep.both)
            .run()
        )({ case (queue, _) =>
          Future {
            queue.complete()
          }(materializer.executionContext)
        })
        .map { case (queue, source) =>
          val readWriteService = new PartyOnlyQueueWriteService(
            participantId,
            queue,
            source,
          )
          readWriteService -> readWriteService
        }
    }
  }

  private object ParticipantStateThatFailsOften extends ParticipantStateFactory {
    override def apply(participantId: Ref.ParticipantId)(implicit
        materializer: Materializer,
        traceContext: TraceContext,
    ): ResourceOwner[ParticipantState] =
      InMemoryPartyParticipantState(participantId)
        .map { case (readingDelegate, writeDelegate) =>
          var lastFailure: Option[Offset] = None
          // This spy inserts a failure after each state update to force the indexer to restart.
          val failingParticipantState = spy(readingDelegate)
          doAnswer(invocation => {
            val beginAfter = invocation.getArgument[Option[Offset]](0)
            readingDelegate.stateUpdates(beginAfter).flatMapConcat { case value @ (offset, _) =>
              if (lastFailure.forall(_ < offset)) {
                lastFailure = Some(offset)
                Source.single(value).concat(Source.failed(new StateUpdatesFailedException))
              } else {
                Source.single(value)
              }
            }
          }).when(failingParticipantState)
            .stateUpdates(
              ArgumentMatchers.any[Option[Offset]]()
            )(ArgumentMatchers.any[TraceContext])
          failingParticipantState -> writeDelegate
        }

    private class StateUpdatesFailedException extends RuntimeException("State updates failed.")
  }

  class PartyOnlyQueueWriteService(
      participantId: Ref.ParticipantId,
      queue: BoundedSourceQueue[(Offset, Traced[Update])],
      source: Source[(Offset, Traced[Update]), NotUsed],
  ) extends WritePartyService
      with ReadService
      with InternalStateServiceProviderImpl {

    private val offset = new AtomicLong(0)
    private val writtenUpdates = mutable.Buffer.empty[(Offset, Traced[Update])]

    override def stateUpdates(beginAfter: Option[Offset])(implicit
        traceContext: TraceContext
    ): Source[(Offset, Traced[Update]), NotUsed] = {
      val updatesForStream = writtenUpdates.toSeq
      Source
        .fromIterator(() => updatesForStream.iterator)
        .concat(source.filterNot(updatesForStream.contains))
        .filter(offsetWithUpdate => beginAfter.forall(_ < offsetWithUpdate._1))
    }

    override def currentHealth(): HealthStatus = HealthStatus.healthy

    override def allocateParty(
        hint: Option[Party],
        displayName: Option[String],
        submissionId: SubmissionId,
    )(implicit
        traceContext: TraceContext
    ): CompletionStage[SubmissionResult] = {
      val updateOffset = Offset.fromByteArray(offset.incrementAndGet().toString.getBytes)
      val update = wrapWithNewTraceContext(
        Update.PartyAddedToParticipant(
          hint
            .map(Party.assertFromString)
            .getOrElse(Party.assertFromString(UUID.randomUUID().toString)),
          displayName.orElse(hint).getOrElse("Unknown"),
          participantId,
          Time.Timestamp.now(),
          Some(submissionId),
        )
      )
      writtenUpdates.append(updateOffset -> update)
      queue.offer(
        updateOffset -> update
      ) match {
        case completionResult: QueueCompletionResult =>
          Future.failed(new RuntimeException(s"Queue completed $completionResult.")).asJava
        case QueueOfferResult.Enqueued =>
          Future.successful[SubmissionResult](SubmissionResult.Acknowledged).asJava
        case QueueOfferResult.Dropped =>
          Future.failed(new RuntimeException("Element dropped")).asJava
      }
    }
  }
}
