// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS
import java.util.UUID
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{BroadcastHub, Keep, Source}
import akka.stream.{BoundedSourceQueue, Materializer, QueueCompletionResult, QueueOfferResult}
import ch.qos.logback.classic.Level
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.{Configuration, LedgerId, LedgerInitialConditions}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{
  ReadService,
  SubmissionResult,
  Update,
  WritePartyService,
}
import com.daml.ledger.resources.{ResourceOwner, TestResourceContext}
import com.daml.lf.data.Ref.{Party, SubmissionId}
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.RecoveringIndexerIntegrationSpec._
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerReadDao}
import com.daml.platform.store.cache.MutableLedgerEndCache
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.platform.testing.LogCollector
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import com.daml.timer.RetryStrategy
import org.mockito.Mockito._
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.FutureConverters.{CompletionStageOps, FutureOps}
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
      participantServer(InMemoryPartyParticipantState)
        .use { case (participantState, materializer) =>
          for {
            _ <- participantState
              .allocateParty(
                hint = Some(Ref.Party.assertFromString("alice")),
                displayName = Some("Alice"),
                submissionId = randomSubmissionId(),
              )
              .asScala
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
                .asScala
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
  )(implicit loggingContext: LoggingContext): ResourceOwner[(WritePartyService, Materializer)] = {
    val ledgerId = Ref.LedgerString.assertFromString(s"ledger-$testId")
    val participantId = Ref.ParticipantId.assertFromString(s"participant-$testId")
    val jdbcUrl =
      s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase()}-$testId;db_close_delay=-1;db_close_on_exit=false"
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem())
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      participantState <- newParticipantState(ledgerId, participantId)(materializer, loggingContext)
      _ <- new StandaloneIndexerServer(
        readService = participantState._1,
        config = IndexerConfig(
          participantId = participantId,
          jdbcUrl = jdbcUrl,
          startupMode = IndexerStartupMode.MigrateAndStart(),
          restartDelay = restartDelay,
        ),
        metrics = new Metrics(new MetricRegistry),
        lfValueTranslationCache = LfValueTranslationCache.Cache.none,
      )(materializer, loggingContext)
    } yield participantState._2 -> materializer
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
    val metrics = new Metrics(new MetricRegistry)
    DbSupport
      .owner(
        jdbcUrl = jdbcUrl,
        serverRole = ServerRole.Testing(getClass),
        connectionPoolSize = 16,
        connectionTimeout = 250.millis,
        metrics = metrics,
      )
      .map(dbSupport =>
        JdbcLedgerDao.read(
          dbSupport = dbSupport,
          eventsPageSize = 100,
          eventsProcessingParallelism = 8,
          acsIdPageSize = 20000,
          acsIdFetchingParallelism = 2,
          acsContractFetchingParallelism = 2,
          acsGlobalParallelism = 10,
          servicesExecutionContext = executionContext,
          metrics = metrics,
          lfValueTranslationCache = LfValueTranslationCache.Cache.none,
          enricher = None,
          participantId = Ref.ParticipantId.assertFromString("RecoveringIndexerIntegrationSpec"),
          ledgerEndCache = mutableLedgerEndCache,
          stringInterning = stringInterning,
          materializer = materializer,
        ) -> mutableLedgerEndCache
      )
  }
}

object RecoveringIndexerIntegrationSpec {

  private type ParticipantState = (ReadService, WritePartyService)

  private val eventually = RetryStrategy.exponentialBackoff(10, 10.millis)

  private def randomSubmissionId() =
    Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)

  private trait ParticipantStateFactory {
    def apply(ledgerId: LedgerId, participantId: Ref.ParticipantId)(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[ParticipantState]
  }

  private object InMemoryPartyParticipantState extends ParticipantStateFactory {
    override def apply(ledgerId: LedgerId, participantId: Ref.ParticipantId)(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[ParticipantState] = {
      ResourceOwner
        .forReleasable(() =>
          // required for the indexer to resubscribe to the update source
          Source.queue[(Offset, Update)](bufferSize = 16).toMat(BroadcastHub.sink)(Keep.both).run()
        )({ case (queue, _) =>
          Future {
            queue.complete()
          }(materializer.executionContext)
        })
        .map { case (queue, source) =>
          val readWriteService = new PartyOnlyQueueWriteService(
            ledgerId,
            participantId,
            queue,
            source,
          )
          readWriteService -> readWriteService
        }
    }
  }

  private object ParticipantStateThatFailsOften extends ParticipantStateFactory {
    override def apply(ledgerId: LedgerId, participantId: Ref.ParticipantId)(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[ParticipantState] =
      InMemoryPartyParticipantState(ledgerId, participantId)
        .map { case (readingDelegate, writeDelegate) =>
          var lastFailure: Option[Offset] = None
          // This spy inserts a failure after each state update to force the indexer to restart.
          val failingParticipantState = spy(readingDelegate)
          doAnswer(invocation => {
            val beginAfter = invocation.getArgument[Option[Offset]](0)
            readingDelegate.stateUpdates(beginAfter).flatMapConcat { case value @ (offset, _) =>
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
          failingParticipantState -> writeDelegate
        }

    private class StateUpdatesFailedException extends RuntimeException("State updates failed.")
  }

  class PartyOnlyQueueWriteService(
      ledgerId: String,
      participantId: Ref.ParticipantId,
      queue: BoundedSourceQueue[(Offset, Update)],
      source: Source[(Offset, Update), NotUsed],
  ) extends WritePartyService
      with ReadService {

    private val offset = new AtomicLong(0)
    private val writtenUpdates = mutable.Buffer.empty[(Offset, Update)]

    override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
      Source.repeat(
        LedgerInitialConditions(
          ledgerId,
          Configuration.reasonableInitialConfiguration,
          Time.Timestamp.Epoch,
        )
      )

    override def stateUpdates(beginAfter: Option[Offset])(implicit
        loggingContext: LoggingContext
    ): Source[(Offset, Update), NotUsed] = {
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
        loggingContext: LoggingContext,
        telemetryContext: TelemetryContext,
    ): CompletionStage[SubmissionResult] = {
      val updateOffset = Offset.fromByteArray(offset.incrementAndGet().toString.getBytes)
      val update = Update.PartyAddedToParticipant(
        hint
          .map(Party.assertFromString)
          .getOrElse(Party.assertFromString(UUID.randomUUID().toString)),
        displayName.orElse(hint).getOrElse("Unknown"),
        participantId,
        Time.Timestamp.now(),
        Some(submissionId),
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
