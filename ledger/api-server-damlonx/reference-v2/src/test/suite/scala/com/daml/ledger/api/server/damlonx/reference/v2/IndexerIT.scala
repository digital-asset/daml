// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import ch.qos.logback.classic.Level
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.api.server.damlonx.reference.v2.IndexerIT._
import com.daml.ledger.participant.state.v1.Update.Heartbeat
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.platform.common.logging.TestNamedLoggerFactory
import com.digitalasset.platform.indexer.{
  IndexerConfig,
  IndexerStartupMode,
  RecoveringIndexer,
  StandaloneIndexerServer
}
import com.digitalasset.platform.resources.Resource
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{JdbcLedgerDao, LedgerDao}
import com.digitalasset.timer.RetryStrategy
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.{AsyncWordSpec, BeforeAndAfterEach, Matchers}

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class IndexerIT extends AsyncWordSpec with Matchers with BeforeAndAfterEach {
  private[this] implicit var actorSystem: ActorSystem = _
  private[this] implicit var materializer: Materializer = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    actorSystem = ActorSystem(getClass.getSimpleName)
    materializer = Materializer(actorSystem)
  }

  override def afterEach(): Unit = {
    loggerFactory.cleanup()
    materializer.shutdown()
    Await.result(actorSystem.terminate(), 10.seconds)
    super.afterEach()
  }

  "indexer" should {
    "index the participant state" in {
      val (participantState, server, ledgerDao) =
        initializeEverything(new InMemoryKVParticipantState(_, _))

      for {
        _ <- participantState
          .allocateParty(
            hint = Some(Ref.Party.assertFromString("alice")),
            displayName = Some("Alice"),
            submissionId = randomSubmissionId(),
          )
          .toScala
        _ <- eventually { (_, _) =>
          ledgerDao.asFuture
            .flatMap(_.getParties)
            .map(parties => parties.map(_.displayName))
            .map(_ shouldBe Seq(Some("Alice")))
        }
        _ <- server.release()
        _ <- ledgerDao.release()
      } yield {
        logs shouldBe Seq(
          Level.INFO -> "Starting Indexer Server",
          Level.INFO -> "Started Indexer Server",
          Level.INFO -> "Stopping Indexer Server",
          Level.INFO -> "Successfully finished processing state updates",
          Level.INFO -> "Stopped Indexer Server",
        )
      }
    }

    "index the participant state, even on spurious failures" in {
      val (participantState, server, ledgerDao) = initializeEverything((participantId, ledgerId) =>
        failingOften(new InMemoryKVParticipantState(participantId, ledgerId)))

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
        _ <- eventually { (_, _) =>
          ledgerDao.asFuture
            .flatMap(_.getParties)
            .map(parties => parties.map(_.displayName))
            .map(_ shouldBe Seq(Some("Alice"), Some("Bob"), Some("Carol")))
        }
        _ <- server.release()
        _ <- ledgerDao.release()
      } yield {
        logs shouldBe Seq(
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

    "stop when the kill switch is hit after a failure" in {
      val (participantState, server, ledgerDao) = initializeEverything(
        (participantId, ledgerId) =>
          failingOften(new InMemoryKVParticipantState(participantId, ledgerId)),
        restartDelay = 10.seconds,
      )

      for {
        _ <- ledgerDao.release()
        _ <- participantState
          .allocateParty(
            hint = Some(Ref.Party.assertFromString("alice")),
            displayName = Some("Alice"),
            submissionId = randomSubmissionId(),
          )
          .toScala
        _ <- eventually { (_, _) =>
          Future.fromTry(
            Try(logs.take(3) shouldBe Seq(
              Level.INFO -> "Starting Indexer Server",
              Level.INFO -> "Started Indexer Server",
              Level.ERROR -> "Error while running indexer, restart scheduled after 10 seconds",
            )))
        }
        _ <- server.release()
      } yield {
        // stopping the server and logging the error can happen in either order
        logs should contain theSameElementsAs Seq(
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

  private def initializeEverything(
      newParticipantState: (ParticipantId, LedgerString) => ParticipantState,
      restartDelay: FiniteDuration = 100.millis,
  ): (ParticipantState, Resource[Unit], Resource[LedgerDao]) = {
    val id = UUID.randomUUID()
    val participantId: ParticipantId = LedgerString.assertFromString(s"participant-$id")
    val ledgerId = LedgerString.assertFromString(s"ledger-$id")
    val participantState = newParticipantState(participantId, ledgerId)
    val jdbcUrl =
      s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase()}-$id;db_close_delay=-1;db_close_on_exit=false"
    val serverOwner = new StandaloneIndexerServer(
      participantState,
      IndexerConfig(
        participantId,
        jdbcUrl,
        startupMode = IndexerStartupMode.MigrateAndStart,
        restartDelay = restartDelay,
      ),
      loggerFactory,
      SharedMetricRegistries.getOrCreate(s"${getClass.getSimpleName}-server"),
    )
    val ledgerDaoOwner =
      JdbcLedgerDao.owner(
        jdbcUrl,
        loggerFactory,
        SharedMetricRegistries.getOrCreate(s"${getClass.getSimpleName}-ledger-dao"),
        ExecutionContext.global,
      )
    val server = serverOwner.acquire()
    val ledgerDao = ledgerDaoOwner.acquire()
    (participantState, server, ledgerDao)
  }
}

object IndexerIT {
  private type ParticipantState = ReadService with WriteService with AutoCloseable

  private val eventually = RetryStrategy.exponentialBackoff(10, 10.millis)

  private val loggerFactory = TestNamedLoggerFactory(classOf[IndexerIT])

  private def logs: Seq[(Level, String)] =
    loggerFactory.logs(classOf[RecoveringIndexer])

  private def randomSubmissionId(): LedgerString =
    LedgerString.assertFromString(UUID.randomUUID().toString)

  // This spy inserts a failure after each state update to force the RecoveringIndexer to restart.
  private def failingOften(delegate: ParticipantState): ParticipantState = {
    var lastFailure: Option[Offset] = None
    val failingParticipantState = spy(delegate)
    doAnswer(invocation => {
      val beginAfter = invocation.getArgument[Option[Offset]](0)
      delegate.stateUpdates(beginAfter).flatMapConcat {
        case value @ (_, Heartbeat(_)) =>
          Source.single(value)
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
