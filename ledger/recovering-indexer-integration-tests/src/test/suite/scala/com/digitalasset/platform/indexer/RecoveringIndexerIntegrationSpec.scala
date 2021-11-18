// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS
import java.util.UUID
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import ch.qos.logback.classic.Level
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.on.memory
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v2.{ReadService, WriteService}
import com.daml.ledger.resources.{ResourceOwner, TestResourceContext}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.RecoveringIndexerIntegrationSpec._
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.{DbDispatcher, JdbcLedgerDao, LedgerReadDao}
import com.daml.platform.store.{DbType, LfValueTranslationCache}
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.cache.MutableLedgerEndCache
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.testing.LogCollector
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import com.daml.timer.RetryStrategy
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class RecoveringIndexerIntegrationSpec
    extends AsyncWordSpec
    with Matchers
    with TestResourceContext
    with BeforeAndAfterEach
    with MockitoSugar {
  private[this] var testId: UUID = _

  private implicit val telemetryContext: TelemetryContext = NoOpTelemetryContext

  override def beforeEach(): Unit = {
    super.beforeEach()
    testId = UUID.randomUUID()
    LogCollector.clear[this.type]
  }

  private def readLog(): Seq[(Level, String)] = LogCollector.read[this.type, RecoveringIndexer]

  "indexer" should {
    "index the participant state" in newLoggingContext { implicit loggingContext =>
      participantServer(SimpleParticipantState)
        .use { case (participantState, materializer) =>
          for {
            _ <- participantState
              .allocateParty(
                hint = Some(Ref.Party.assertFromString("alice")),
                displayName = Some("Alice"),
                submissionId = randomSubmissionId(),
              )
              .toScala
            _ <- eventuallyPartiesShouldBe("Alice")(materializer)
          } yield ()
        }
        .map { _ =>
          readLog() should contain theSameElementsInOrderAs Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.INFO -> "Stopping Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
            Level.INFO -> "Stopped Indexer Server",
          )
        }
    }

    "index the participant state, even on spurious failures" in newLoggingContext {
      implicit loggingContext =>
        participantServer(ParticipantStateThatFailsOften)
          .use { case (participantState, materializer) =>
            for {
              _ <- participantState
                .allocateParty(
                  hint = Some(Ref.Party.assertFromString("alice")),
                  displayName = Some("Alice"),
                  submissionId = randomSubmissionId(),
                )
                .toScala
              _ <- participantState
                .allocateParty(
                  hint = Some(Ref.Party.assertFromString("bob")),
                  displayName = Some("Bob"),
                  submissionId = randomSubmissionId(),
                )
                .toScala
              _ <- participantState
                .allocateParty(
                  hint = Some(Ref.Party.assertFromString("carol")),
                  displayName = Some("Carol"),
                  submissionId = randomSubmissionId(),
                )
                .toScala
              _ <- eventuallyPartiesShouldBe("Alice", "Bob", "Carol")(materializer)
            } yield ()
          }
          .map { _ =>
            readLog() should contain theSameElementsInOrderAs Seq(
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
          }
    }

    "stop when the kill switch is hit after a failure" in newLoggingContext {
      implicit loggingContext =>
        participantServer(ParticipantStateThatFailsOften, restartDelay = 10.seconds)
          .use { case (participantState, _) =>
            for {
              _ <- participantState
                .allocateParty(
                  hint = Some(Ref.Party.assertFromString("alice")),
                  displayName = Some("Alice"),
                  submissionId = randomSubmissionId(),
                )
                .toScala
              _ <- eventually { (_, _) =>
                Future.fromTry(
                  Try(
                    readLog().take(3) should contain theSameElementsInOrderAs Seq(
                      Level.INFO -> "Starting Indexer Server",
                      Level.INFO -> "Started Indexer Server",
                      Level.ERROR -> "Error while running indexer, restart scheduled after 10 seconds",
                    )
                  )
                )
              }
            } yield Instant.now()
          }
          .map { timeBeforeStop =>
            val timeAfterStop = Instant.now()
            SECONDS.between(timeBeforeStop, timeAfterStop) should be <= 5L
            // stopping the server and logging the error can happen in either order
            readLog() should contain theSameElementsInOrderAs Seq(
              Level.INFO -> "Starting Indexer Server",
              Level.INFO -> "Started Indexer Server",
              Level.ERROR -> "Error while running indexer, restart scheduled after 10 seconds",
              Level.INFO -> "Stopping Indexer Server",
              Level.INFO -> "Indexer Server was stopped; cancelling the restart",
              Level.INFO -> "Stopped Indexer Server",
            )
          }
    }
  }

  private def participantServer(
      newParticipantState: ParticipantStateFactory,
      restartDelay: FiniteDuration = 100.millis,
  )(implicit loggingContext: LoggingContext): ResourceOwner[(ParticipantState, Materializer)] = {
    val ledgerId = Ref.LedgerString.assertFromString(s"ledger-$testId")
    val participantId = Ref.ParticipantId.assertFromString(s"participant-$testId")
    val jdbcUrl =
      s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase()}-$testId;db_close_delay=-1;db_close_on_exit=false"
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem())
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      servicesExecutionContext <- ResourceOwner
        .forExecutorService(() => Executors.newWorkStealingPool())
        .map(ExecutionContext.fromExecutorService)
      participantState <- newParticipantState(ledgerId, participantId)(materializer, loggingContext)
      _ <- new StandaloneIndexerServer(
        readService = participantState,
        config = IndexerConfig(
          participantId = participantId,
          jdbcUrl = jdbcUrl,
          startupMode = IndexerStartupMode.MigrateAndStart,
          restartDelay = restartDelay,
        ),
        servicesExecutionContext = servicesExecutionContext,
        metrics = new Metrics(new MetricRegistry),
        lfValueTranslationCache = LfValueTranslationCache.Cache.none,
      )(materializer, loggingContext)
    } yield participantState -> materializer
  }

  private def eventuallyPartiesShouldBe(partyNames: String*)(materializer: Materializer)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    dao(materializer).use { case (ledgerDao, ledgerEndCache) =>
      eventually { (_, _) =>
        for {
          ledgerEnd <- ledgerDao.lookupLedgerEnd()
          _ = ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
          knownParties <- ledgerDao.listKnownParties()
        } yield {
          knownParties.map(_.displayName) shouldBe partyNames.map(Some(_))
          ()
        }
      }
    }

  // TODO we probably do not need a full dao for this purpose: refactoring with direct usage of StorageBackend?
  private def dao(materializer: Materializer)(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[(LedgerReadDao, MutableLedgerEndCache)] = {
    val mutableLedgerEndCache = MutableLedgerEndCache()
    val stringInterning = new StringInterningView((_, _) => _ => Future.successful(Nil)) // not used
    val jdbcUrl =
      s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase}-$testId;db_close_delay=-1;db_close_on_exit=false"
    val errorFactories: ErrorFactories = mock[ErrorFactories]
    val storageBackendFactory = StorageBackendFactory.of(DbType.jdbcType(jdbcUrl))
    val metrics = new Metrics(new MetricRegistry)
    DbDispatcher
      .owner(
        dataSource = storageBackendFactory.createDataSourceStorageBackend.createDataSource(jdbcUrl),
        serverRole = ServerRole.Testing(getClass),
        connectionPoolSize = 16,
        connectionTimeout = 250.millis,
        metrics = metrics,
      )
      .map(dbDispatcher =>
        JdbcLedgerDao.read(
          dbDispatcher = dbDispatcher,
          eventsPageSize = 100,
          eventsProcessingParallelism = 8,
          servicesExecutionContext = executionContext,
          metrics = metrics,
          lfValueTranslationCache = LfValueTranslationCache.Cache.none,
          enricher = None,
          participantId = Ref.ParticipantId.assertFromString("RecoveringIndexerIntegrationSpec"),
          storageBackendFactory = storageBackendFactory,
          ledgerEndCache = mutableLedgerEndCache,
          errorFactories = errorFactories,
          stringInterning = stringInterning,
          materializer = materializer,
        ) -> mutableLedgerEndCache
      )
  }
}

object RecoveringIndexerIntegrationSpec {

  private type ParticipantState = ReadService with WriteService

  private val eventually = RetryStrategy.exponentialBackoff(10, 10.millis)

  private def randomSubmissionId() =
    Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)

  private trait ParticipantStateFactory {
    def apply(ledgerId: LedgerId, participantId: Ref.ParticipantId)(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[ParticipantState]
  }

  private object SimpleParticipantState extends ParticipantStateFactory {
    override def apply(ledgerId: LedgerId, participantId: Ref.ParticipantId)(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[ParticipantState] = {
      val metrics = new Metrics(new MetricRegistry)
      for {
        dispatcher <- memory.dispatcherOwner
        committerExecutionContext <- ResourceOwner
          .forExecutorService(() => Executors.newCachedThreadPool())
          .map(ExecutionContext.fromExecutorService)
        readerWriter <- new memory.InMemoryLedgerReaderWriter.Owner(
          ledgerId = ledgerId,
          participantId = participantId,
          offsetVersion = 0,
          keySerializationStrategy = StateKeySerializationStrategy.createDefault(),
          metrics = metrics,
          dispatcher = dispatcher,
          state = memory.InMemoryState.empty,
          engine = Engine.DevEngine(),
          committerExecutionContext = committerExecutionContext,
        )
      } yield new KeyValueParticipantState(
        readerWriter,
        readerWriter,
        metrics,
        enableSelfServiceErrorCodes = false,
      )
    }
  }

  private object ParticipantStateThatFailsOften extends ParticipantStateFactory {
    override def apply(ledgerId: LedgerId, participantId: Ref.ParticipantId)(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[ParticipantState] =
      SimpleParticipantState(ledgerId, participantId)
        .map { delegate =>
          var lastFailure: Option[Offset] = None
          // This spy inserts a failure after each state update to force the indexer to restart.
          val failingParticipantState = spy(delegate)
          doAnswer(invocation => {
            val beginAfter = invocation.getArgument[Option[Offset]](0)
            delegate.stateUpdates(beginAfter).flatMapConcat { case value @ (offset, _) =>
              if (lastFailure.isEmpty || lastFailure.get < offset) {
                lastFailure = Some(offset)
                Source.single(value).concat(Source.failed(new StateUpdatesFailedException))
              } else {
                Source.single(value)
              }
            }
          }).when(failingParticipantState)
            .stateUpdates(
              ArgumentMatchers.any[Option[Offset]]()
            )(ArgumentMatchers.any[LoggingContext])
          failingParticipantState
        }

    private class StateUpdatesFailedException extends RuntimeException("State updates failed.")
  }
}
