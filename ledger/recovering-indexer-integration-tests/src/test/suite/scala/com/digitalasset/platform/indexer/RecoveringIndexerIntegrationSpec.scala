// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import ch.qos.logback.classic.Level
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.LedgerString
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.RecoveringIndexerIntegrationSpec._
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerDao}
import com.daml.platform.testing.LogCollector
import com.daml.resources.ResourceOwner
import com.daml.resources.akka.AkkaResourceOwner
import com.daml.timer.RetryStrategy
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.{AsyncWordSpec, BeforeAndAfterEach, Matchers}

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

class RecoveringIndexerIntegrationSpec extends AsyncWordSpec with Matchers with BeforeAndAfterEach {
  private[this] var testId: UUID = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    testId = UUID.randomUUID()
    LogCollector.clear[this.type]
  }

  private def readLog(): Seq[(Level, String)] = LogCollector.read[this.type, RecoveringIndexer]

  "indexer" should {
    "index the participant state" in newLoggingContext { implicit logCtx =>
      participantServer(SimpleParticipantState)
        .use { participantState =>
          for {
            _ <- participantState
              .allocateParty(
                hint = Some(Ref.Party.assertFromString("alice")),
                displayName = Some("Alice"),
                submissionId = randomSubmissionId(),
              )
              .toScala
            _ <- index.use { ledgerDao =>
              eventually { (_, _) =>
                ledgerDao
                  .listKnownParties()
                  .map(parties => parties.map(_.displayName))
                  .map(_ shouldBe Seq(Some("Alice")))
              }
            }
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
      implicit logCtx =>
        participantServer(ParticipantStateThatFailsOften)
          .use { participantState =>
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
              _ <- index.use { ledgerDao =>
                eventually { (_, _) =>
                  ledgerDao
                    .listKnownParties()
                    .map(parties => parties.map(_.displayName))
                    .map(_ shouldBe Seq(Some("Alice"), Some("Bob"), Some("Carol")))
                }
              }
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

    "stop when the kill switch is hit after a failure" in newLoggingContext { implicit logCtx =>
      participantServer(ParticipantStateThatFailsOften, restartDelay = 10.seconds)
        .use { participantState =>
          for {
            _ <- participantState
              .allocateParty(
                hint = Some(Ref.Party.assertFromString("alice")),
                displayName = Some("Alice"),
                submissionId = randomSubmissionId(),
              )
              .toScala
            _ <- eventually { (_, _) =>
              Future.fromTry(Try(readLog().take(3) should contain theSameElementsInOrderAs Seq(
                Level.INFO -> "Starting Indexer Server",
                Level.INFO -> "Started Indexer Server",
                Level.ERROR -> "Error while running indexer, restart scheduled after 10 seconds",
              )))
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
  )(implicit logCtx: LoggingContext): ResourceOwner[ParticipantState] = {
    val ledgerId = LedgerString.assertFromString(s"ledger-$testId")
    val participantId = ParticipantId.assertFromString(s"participant-$testId")
    val jdbcUrl =
      s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase()}-$testId;db_close_delay=-1;db_close_on_exit=false"
    for {
      actorSystem <- AkkaResourceOwner.forActorSystem(() => ActorSystem())
      materializer <- AkkaResourceOwner.forMaterializer(() => Materializer(actorSystem))
      participantState <- newParticipantState(Some(ledgerId), participantId)(materializer, logCtx)
      _ <- new StandaloneIndexerServer(
        readService = participantState,
        config = IndexerConfig(
          participantId,
          jdbcUrl,
          startupMode = IndexerStartupMode.MigrateAndStart,
          restartDelay = restartDelay,
        ),
        metrics = new Metrics(new MetricRegistry),
      )(materializer, logCtx)
    } yield participantState
  }

  private def index(implicit logCtx: LoggingContext): ResourceOwner[LedgerDao] = {
    val jdbcUrl =
      s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase}-$testId;db_close_delay=-1;db_close_on_exit=false"
    JdbcLedgerDao.writeOwner(
      serverRole = ServerRole.Testing(getClass),
      jdbcUrl = jdbcUrl,
      eventsPageSize = 100,
      metrics = new Metrics(new MetricRegistry),
    )
  }
}

object RecoveringIndexerIntegrationSpec {

  private type ParticipantState = ReadService with WriteService

  private val eventually = RetryStrategy.exponentialBackoff(10, 10.millis)

  private def randomSubmissionId(): SubmissionId =
    SubmissionId.assertFromString(UUID.randomUUID().toString)

  private trait ParticipantStateFactory {
    def apply(ledgerId: Option[LedgerId], participantId: ParticipantId)(
        implicit materializer: Materializer,
        logCtx: LoggingContext,
    ): ResourceOwner[ParticipantState]
  }

  private object SimpleParticipantState extends ParticipantStateFactory {
    override def apply(ledgerId: Option[LedgerId], participantId: ParticipantId)(
        implicit materializer: Materializer,
        logCtx: LoggingContext
    ): ResourceOwner[ParticipantState] = {
      val metrics = new Metrics(new MetricRegistry)
      new InMemoryLedgerReaderWriter.SingleParticipantOwner(
        ledgerId,
        participantId,
        metrics = metrics,
        engine = Engine()
      ).map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter, metrics))
    }
  }

  private object ParticipantStateThatFailsOften extends ParticipantStateFactory {
    override def apply(ledgerId: Option[LedgerId], participantId: ParticipantId)(
        implicit materializer: Materializer,
        logCtx: LoggingContext
    ): ResourceOwner[ParticipantState] =
      SimpleParticipantState(ledgerId, participantId)
        .map { delegate =>
          var lastFailure: Option[Offset] = None
          // This spy inserts a failure after each state update to force the indexer to restart.
          val failingParticipantState = spy(delegate)
          doAnswer(invocation => {
            val beginAfter = invocation.getArgument[Option[Offset]](0)
            delegate.stateUpdates(beginAfter).flatMapConcat {
              case value @ (offset, _) =>
                if (lastFailure.isEmpty || lastFailure.get < offset) {
                  lastFailure = Some(offset)
                  Source.single(value).concat(Source.failed(new StateUpdatesFailedException))
                } else {
                  Source.single(value)
                }
            }
          }).when(failingParticipantState).stateUpdates(ArgumentMatchers.any[Option[Offset]]())
          failingParticipantState
        }

    private class StateUpdatesFailedException extends RuntimeException("State updates failed.")
  }
}
