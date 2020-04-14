// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1.SubmissionResult.{Acknowledged, Overloaded}
import com.daml.ledger.participant.state.v1.{Configuration, LedgerInitialConditions, Offset, ParticipantId, ReadService, SubmissionId, SubmissionResult, SubmittedTransaction, SubmitterInfo, TimeModel, TransactionMeta, Update, WriteService}
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time
import com.daml.lf.data.Time.Timestamp
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.Success

private case class ConfigChange(
    timestamp: Instant,
    maxRecordTime: Time.Timestamp,
    submissionId: SubmissionId,
    config: Configuration)

sealed abstract class ConfigChangeResponse
case object AcceptConfigChange extends ConfigChangeResponse // Accept submission, write ConfigurationChanged
case object RejectConfigChange extends ConfigChangeResponse // Accept submission, write ConfigurationChangeRejected
case object RejectSubmission extends ConfigChangeResponse // Reject submission
case object LooseSubmission extends ConfigChangeResponse // Accept submission, don't write anything

private case class MockReadWriteService(onSubmit: (Configuration, Int) => ConfigChangeResponse)
    extends ReadService
    with WriteService {
  private[this] implicit val actorSystem: ActorSystem = ActorSystem("MockReadWriteService")
  private[this] implicit val materializer: Materializer = Materializer(actorSystem)

  private[this] val (updateQueue, updateSource) =
    Source.queue[(Offset, Update)](10000, OverflowStrategy.fail).preMaterialize()
  private[this] val nextOffset = new AtomicInteger(0)
  private[this] val participantId: ParticipantId = ParticipantId.assertFromString("MockParticipant")

  var currentConfig: Option[Configuration] = None
  val submissions: ListBuffer[ConfigChange] = ListBuffer.empty

  private[this] def randomSubmissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)

  def writeConfigChange(config: Configuration): Unit = this.synchronized {
    currentConfig = Some(config)
    write(
      Update.ConfigurationChanged(
        Time.Timestamp.assertFromInstant(Instant.now),
        randomSubmissionId,
        participantId,
        config))
    ()
  }
  def writeConfigRejection(config: Configuration): Unit = this.synchronized {
    write(
      Update.ConfigurationChangeRejected(
        Time.Timestamp.assertFromInstant(Instant.now),
        randomSubmissionId,
        participantId,
        config,
        "Test rejection"))
    ()
  }
  def writeRandomUnrelatedUpdates(n: Int): Unit = this.synchronized {
    (1 to n).foreach(_ => {
      val party = UUID.randomUUID().toString
      write(
        Update.PartyAddedToParticipant(
          Party.assertFromString(party),
          party,
          participantId,
          Timestamp.assertFromInstant(Instant.now),
          Some(randomSubmissionId)))
    })
    ()
  }
  private[this] def write(update: Update) = {
    val offset = Offset.fromByteArray(BigInt(nextOffset.getAndIncrement()).toByteArray)
    updateQueue.offer((offset, update))
  }

  override def currentHealth(): HealthStatus = HealthStatus.healthy
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = Source.empty
  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    updateSource
  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId): CompletionStage[SubmissionResult] =
    CompletableFuture.completedFuture(SubmissionResult.NotSupported)
  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration): CompletionStage[SubmissionResult] = this.synchronized {
    val attempt = submissions.length
    submissions.append(ConfigChange(Instant.now, maxRecordTime, submissionId, config))
    onSubmit(config, attempt) match {
      case AcceptConfigChange =>
        if (currentConfig.forall(_.generation == config.generation - 1)) {
          writeConfigChange(config)
        } else {
          writeConfigRejection(config)
        }
        CompletableFuture.completedFuture(Acknowledged)
      case RejectConfigChange =>
        writeConfigRejection(config)
        CompletableFuture.completedFuture(Acknowledged)
      case RejectSubmission =>
        CompletableFuture.completedFuture(Overloaded)
      case LooseSubmission =>
        CompletableFuture.completedFuture(Acknowledged)
    }
  }
  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): CompletionStage[SubmissionResult] =
    CompletableFuture.completedFuture(SubmissionResult.NotSupported)
  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String]): CompletionStage[SubmissionResult] =
    CompletableFuture.completedFuture(SubmissionResult.NotSupported)

  def close() = {
    updateQueue.complete()
    actorSystem.terminate()
  }
}

class LedgerInitializationSpec extends AsyncWordSpec with Matchers {
  implicit val actorSystem: ActorSystem = ActorSystem("LedgerInitializationSpec")
  implicit val materializer: Materializer = Materializer(actorSystem)

  val newConfig = Configuration(
    0,
    TimeModel(Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1)).get,
    Duration.ofSeconds(1)
  )
  val existingConfig = Configuration(
    0,
    TimeModel(Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofSeconds(2)).get,
    Duration.ofSeconds(2)
  )
  val initConfig = LedgerInitializationConfig(
    maxUpdates = 10,
    initialDelay = Duration.ofMillis(100),
    retryDelay = Duration.ofMillis(100),
    submissionTimeout = Duration.ofSeconds(1),
    maxAttempts = 5,
  )

  "LedgerInitialization" should {
    "Work on an empty ledger" in {
      val mockServer = MockReadWriteService((_, _) => AcceptConfigChange)
      val finished = LedgerInitialization.initialize(mockServer, mockServer, newConfig)

      finished.map(_ => {
        mockServer.submissions.size shouldBe 1
        mockServer.currentConfig shouldBe Some(newConfig)
      })
    }
    "Work on a ledger with one existing config" in {
      val mockServer = MockReadWriteService((_, _) => AcceptConfigChange)
      mockServer.writeConfigChange(existingConfig)

      // Initialization should not submit anything if there is a config change near the start
      LedgerInitialization.initialize(mockServer, mockServer, newConfig)
        .map(_ => mockServer.close())
        .map(_ => {
          mockServer.submissions.size shouldBe 0
          mockServer.currentConfig shouldBe Some(existingConfig)
        })
    }
    "Work on an unconfigured ledger with one existing unrelated update" in {
      val mockServer = MockReadWriteService((_, _) => AcceptConfigChange)
      mockServer.writeRandomUnrelatedUpdates(1)

      // Initialization should inspect more than the first update
      LedgerInitialization.initialize(mockServer, mockServer, newConfig)
        .map(_ => mockServer.close())
        .map(_ => {
          mockServer.submissions.size shouldBe 1
          mockServer.currentConfig shouldBe Some(newConfig)
        })
    }
    "Work on a configured ledger with one existing unrelated update" in {
      val mockServer = MockReadWriteService((_, _) => AcceptConfigChange)
      mockServer.writeRandomUnrelatedUpdates(1)
      mockServer.writeConfigChange(existingConfig)

      // Initialization should inspect more than the first update
      LedgerInitialization.initialize(mockServer, mockServer, newConfig)
        .map(_ => mockServer.close())
        .map(_ => {
          mockServer.submissions.size shouldBe 0
          mockServer.currentConfig shouldBe Some(existingConfig)
        })
    }
    "Work on a ledger with many existing unrelated updates" in {
      val mockServer = MockReadWriteService((_, _) => AcceptConfigChange)
      mockServer.writeRandomUnrelatedUpdates(1000)

      // Initialization should give up after reading too many unrelated updates
      LedgerInitialization.initialize(mockServer, mockServer, newConfig)
        .map(_ => mockServer.close())
        .map(_ => {
          mockServer.submissions.size shouldBe 0
          mockServer.currentConfig shouldBe None
        })
    }
    "Work on an empty ledger if the submission is rejected" in {
      val mockServer = MockReadWriteService(
        (_, attempt) => if (attempt == 0) RejectSubmission else AcceptConfigChange)

      // Initialization should not give up if the submission is rejected once
      LedgerInitialization.initialize(mockServer, mockServer, newConfig)
        .map(_ => mockServer.close())
        .map(_ => {
          mockServer.submissions.size shouldBe 2
          mockServer.currentConfig shouldBe Some(newConfig)
        })
    }
    "Work on an empty ledger if the change is rejected" in {
      val mockServer = MockReadWriteService(
        (_, attempt) => if (attempt == 0) RejectConfigChange else AcceptConfigChange)

      // Initialization should not give up if the config change is rejected once
      LedgerInitialization.initialize(mockServer, mockServer, newConfig)
        .map(_ => mockServer.close())
        .map(_ => {
          mockServer.submissions.size shouldBe 2
          mockServer.currentConfig shouldBe Some(newConfig)
        })
    }
    "Work on an empty ledger if the submission is lost" in {
      val mockServer = MockReadWriteService(
        (_, attempt) => if (attempt == 0) LooseSubmission else AcceptConfigChange)

      // Initialization should not give up if the config change is lost once
      LedgerInitialization.initialize(mockServer, mockServer, newConfig, initConfig)
        .map(_ => mockServer.close())
        .map(_ => {
          mockServer.submissions.size shouldBe 2
          mockServer.currentConfig shouldBe Some(newConfig)
        })
    }
    "Back off exponentially" in {
      val mockServer = MockReadWriteService((_, _) => RejectConfigChange)

      // Time between submissions should increase
      LedgerInitialization.initialize(mockServer, mockServer, newConfig, initConfig)
        .transform { case _ => Success(()) }
        .map(_ => mockServer.close())
        .map(_ => {
          // Delays between submissions
          val delays = mockServer.submissions
            .sliding(2)
            .map { case Seq(x, y, _*) => Duration.between(x.timestamp, y.timestamp) }
            .toList
          // Ratios between submission delays
          val factors = delays
            .sliding(2)
            .map { case Seq(x, y, _*) => y.toNanos.toDouble / x.toNanos.toDouble }
            .toList
          every(factors) should be > 100.1
        })
    }
    "Work on an empty ledger when running in parallel" in {
      val mockServer = MockReadWriteService((_, _) => AcceptConfigChange)
      Future.sequence(
        List(
          LedgerInitialization.initialize(mockServer, mockServer, newConfig),
          LedgerInitialization.initialize(mockServer, mockServer, newConfig),
          LedgerInitialization.initialize(mockServer, mockServer, newConfig),
        ))
        .map(_ => mockServer.close())
        .map(_ => {
          mockServer.submissions.size shouldBe 1
          mockServer.currentConfig shouldBe Some(newConfig)
        })
    }
  }
}
