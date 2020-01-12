// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import java.util.UUID
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import ch.qos.logback.classic.Level
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.api.server.damlonx.reference.v2.IndexerIT._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.health.HealthStatus
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
import org.scalatest.{AsyncWordSpec, BeforeAndAfterEach, Matchers}

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}

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
            submissionId = Ref.LedgerString.assertFromString(UUID.randomUUID().toString),
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
        loggerFactory.logs(classOf[RecoveringIndexer]) shouldBe Seq(
          Level.INFO -> "Starting Indexer Server",
          Level.INFO -> "Started Indexer Server",
          Level.INFO -> "Stopping Indexer Server",
          Level.INFO -> "Successfully finished processing state updates",
          Level.INFO -> "Stopped Indexer Server",
        )
      }
    }

    "index the participant state, even on spurious failures" in {
      val (participantState, server, ledgerDao) = initializeEverything(
        (participantId, ledgerId) =>
          new ParticipantStateWhichFailsOften(
            new InMemoryKVParticipantState(participantId, ledgerId)))

      for {
        _ <- participantState
          .allocateParty(
            hint = Some(Ref.Party.assertFromString("alice")),
            displayName = Some("Alice"),
            submissionId = Ref.LedgerString.assertFromString(UUID.randomUUID().toString),
          )
          .toScala
        _ <- participantState
          .allocateParty(
            hint = Some(Ref.Party.assertFromString("bob")),
            displayName = Some("Bob"),
            submissionId = Ref.LedgerString.assertFromString(UUID.randomUUID().toString),
          )
          .toScala
        _ <- participantState
          .allocateParty(
            hint = Some(Ref.Party.assertFromString("carol")),
            displayName = Some("Carol"),
            submissionId = Ref.LedgerString.assertFromString(UUID.randomUUID().toString),
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
        loggerFactory.logs(classOf[RecoveringIndexer]) shouldBe Seq(
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
          new ParticipantStateWhichFailsOften(
            new InMemoryKVParticipantState(participantId, ledgerId)),
        restartDelay = 10.seconds,
      )

      for {
        _ <- ledgerDao.release()
        _ <- participantState
          .allocateParty(
            hint = Some(Ref.Party.assertFromString("alice")),
            displayName = Some("Alice"),
            submissionId = Ref.LedgerString.assertFromString(UUID.randomUUID().toString),
          )
          .toScala
        _ <- server.release()
      } yield {
        loggerFactory.logs(classOf[RecoveringIndexer]) shouldBe Seq(
          Level.INFO -> "Starting Indexer Server",
          Level.INFO -> "Started Indexer Server",
          Level.INFO -> "Stopping Indexer Server",
          Level.ERROR -> "Error while running indexer, restart scheduled after 10 seconds",
          Level.INFO -> "The Indexer Server was stopped, so not restarting",
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

  // This class inserts a failure after each state update to force the RecoveringIndexer to restart.
  private class ParticipantStateWhichFailsOften(delegate: ParticipantState)
      extends DelegatingParticipantState(delegate) {
    private var lastFailure: Option[Offset] = None

    override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
      super
        .stateUpdates(beginAfter)
        .flatMapConcat {
          case value @ (offset, _) =>
            if (lastFailure.isEmpty || lastFailure.get < offset) {
              lastFailure = Some(offset)
              Source.single(value).concat(Source.failed(new StateUpdatesFailedException))
            } else {
              Source.single(value)
            }
        }
  }

  private class DelegatingParticipantState(delegate: ParticipantState)
      extends ReadService
      with WriteService
      with AutoCloseable {
    override def close(): Unit =
      delegate.close()

    override def currentHealth(): HealthStatus =
      delegate.currentHealth()

    override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
      delegate.getLedgerInitialConditions()

    override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
      delegate.stateUpdates(beginAfter)

    override def uploadPackages(
        submissionId: SubmissionId,
        archives: List[DamlLf.Archive],
        sourceDescription: Option[String],
    ): CompletionStage[SubmissionResult] =
      delegate.uploadPackages(submissionId, archives, sourceDescription)

    override def allocateParty(
        hint: Option[Party],
        displayName: Option[String],
        submissionId: SubmissionId,
    ): CompletionStage[SubmissionResult] =
      delegate.allocateParty(hint, displayName, submissionId)

    override def submitConfiguration(
        maxRecordTime: Time.Timestamp,
        submissionId: SubmissionId,
        config: Configuration,
    ): CompletionStage[SubmissionResult] =
      delegate.submitConfiguration(maxRecordTime, submissionId, config)

    override def submitTransaction(
        submitterInfo: SubmitterInfo,
        transactionMeta: TransactionMeta,
        transaction: SubmittedTransaction,
    ): CompletionStage[SubmissionResult] =
      delegate.submitTransaction(submitterInfo, transactionMeta, transaction)
  }

  private class StateUpdatesFailedException extends RuntimeException("State updates failed.")
}
