// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

import java.io.File
import java.time.Instant

import ch.qos.logback.classic.Level
import com.daml.api.util.TimeProvider
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.api.health.Healthy
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.resources.{Resource, ResourceContext, TestResourceContext}
import com.daml.ledger.test.ModelTestDar
import com.daml.lf.archive.DarParser
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.LegacyTransactionCommitter
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.{LedgerIdMode, MismatchException}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.MetricsAround
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.stores.ledger.Ledger
import com.daml.platform.sandbox.stores.ledger.sql.SqlLedgerSpecAppendOnly._
import com.daml.platform.store.{IndexMetadata, LfValueTranslationCache}
import com.daml.platform.testing.LogCollector
import com.daml.testing.postgresql.PostgresAroundEach
import com.daml.timer.RetryStrategy
import org.scalatest.concurrent.{AsyncTimeLimitedTests, Eventually, ScaledTimeSpans}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minute, Seconds, Span}
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, Future}

// TODO append-only: Remove this class once the mutating schema is removed
final class SqlLedgerSpecAppendOnly
    extends AsyncWordSpec
    with Matchers
    with AsyncTimeLimitedTests
    with ScaledTimeSpans
    with Eventually
    with TestResourceContext
    with AkkaBeforeAndAfterAll
    with PostgresAroundEach
    with MetricsAround {

  protected implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  override val timeLimit: Span = scaled(Span(1, Minute))
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)))

  private val createdLedgers = mutable.Buffer[Resource[Ledger]]()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    createdLedgers.clear()
  }

  override protected def afterEach(): Unit = {
    LogCollector.clear[SqlLedgerSpecAppendOnly]
    for (ledger <- createdLedgers)
      Await.result(ledger.release(), 2.seconds)
    super.afterEach()
  }

  "SQL Ledger" should {
    "be able to be created from scratch with a random ledger ID" in {
      for {
        ledger <- createSqlLedger(validatePartyAllocation = false)
      } yield {
        ledger.ledgerId should not be ""
      }
    }

    "be able to be created from scratch with a given ledger ID" in {
      for {
        ledger <- createSqlLedger(ledgerId, validatePartyAllocation = false)
      } yield {
        ledger.ledgerId should be(ledgerId)
      }
    }

    "be able to be reused keeping the old ledger ID" in {
      for {
        ledger1 <- createSqlLedger(ledgerId, validatePartyAllocation = false)
        ledger2 <- createSqlLedger(ledgerId, validatePartyAllocation = false)
        ledger3 <- createSqlLedger(validatePartyAllocation = false)
      } yield {
        ledger1.ledgerId should not be LedgerId
        ledger1.ledgerId should be(ledger2.ledgerId)
        ledger2.ledgerId should be(ledger3.ledgerId)
      }
    }

    "refuse to create a new ledger when there is already one with a different ledger ID" in {
      for {
        _ <- createSqlLedger(ledgerId = "TheLedger", validatePartyAllocation = false)
        throwable <- createSqlLedger(
          ledgerId = "AnotherLedger",
          validatePartyAllocation = false,
        ).failed
      } yield {
        throwable.getMessage should be(
          "The provided ledger id does not match the existing one. Existing: \"TheLedger\", Provided: \"AnotherLedger\"."
        )
      }
    }

    "correctly initialized the participant ID" in {
      val participantId = makeParticipantId("TheOnlyParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(participantId, validatePartyAllocation = false)
        metadata <- IndexMetadata.read(postgresDatabase.url)
      } yield {
        metadata.participantId shouldEqual participantId
      }
    }

    "allow to resume on an existing participant ID" in {
      val participantId = makeParticipantId("TheParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(participantId, validatePartyAllocation = false)
        _ <- createSqlLedgerWithParticipantId(participantId, validatePartyAllocation = false)
        metadata <- IndexMetadata.read(postgresDatabase.url)
      } yield {
        metadata.participantId shouldEqual participantId
      }
    }

    "refuse to create a new ledger when there is already one with a different participant ID" in {
      val expectedExisting = makeParticipantId("TheParticipant")
      val expectedProvided = makeParticipantId("AnotherParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(expectedExisting, validatePartyAllocation = false)
        throwable <- createSqlLedgerWithParticipantId(
          expectedProvided,
          validatePartyAllocation = false,
        ).failed
      } yield {
        throwable match {
          case mismatch: MismatchException.ParticipantId =>
            mismatch.existing shouldEqual expectedExisting
            mismatch.provided shouldEqual expectedProvided
          case _ =>
            fail("Did not get the expected exception type", throwable)
        }
      }
    }

    "load no packages by default" in {
      for {
        ledger <- createSqlLedger(validatePartyAllocation = false)
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size 0
      }
    }

    "load packages if provided with a dynamic ledger ID" in {
      for {
        ledger <- createSqlLedger(
          packages = testDar.all,
          validatePartyAllocation = false,
        )
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size testDar.all.length.toLong
      }
    }

    "load packages if provided with a static ledger ID" in {
      for {
        ledger <- createSqlLedger(
          ledgerId = "TheLedger",
          packages = testDar.all,
          validatePartyAllocation = false,
        )
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size testDar.all.length.toLong
      }
    }

    "load no packages if the ledger already exists" in {
      for {
        _ <- createSqlLedger(ledgerId = "TheLedger", validatePartyAllocation = false)
        ledger <- createSqlLedger(
          ledgerId = "TheLedger",
          packages = testDar.all,
          validatePartyAllocation = false,
        )
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size 0
      }
    }

    "be healthy" in {
      for {
        ledger <- createSqlLedger(validatePartyAllocation = false)
      } yield {
        ledger.currentHealth() should be(Healthy)
      }
    }

    "not allow insertion of subsequent parties with the same identifier: operations should succeed without effect" in {
      val party1 = Ref.Party.assertFromString("party1")
      val party2 = Ref.Party.assertFromString("party2")
      val submissionId1 = Ref.SubmissionId.assertFromString("submissionId1")
      val submissionId2 = Ref.SubmissionId.assertFromString("submissionId2")
      val submissionId3 = Ref.SubmissionId.assertFromString("submissionId3")
      for {
        sqlLedger <- createSqlLedger(validatePartyAllocation = false)
        partyAllocationResult1 <- sqlLedger.publishPartyAllocation(
          submissionId = submissionId1,
          party = party1,
          displayName = Some("displayname1"),
        )
        // This will fail until the party is available
        retrievedParty1 <- eventually(sqlLedger.getParties(Seq(party1)).map(_.head))
        partyAllocationResult2 <- sqlLedger.publishPartyAllocation(
          submissionId = submissionId2,
          party = party1,
          displayName = Some("displayname2"),
        )
        partyAllocationResult3 <- sqlLedger.publishPartyAllocation(
          submissionId = submissionId3,
          party = party2,
          displayName = None,
        )
        // This will fail until the party which is added the last time is visible
        // This also means that the second party addition is already processed as well
        _ <- eventually(sqlLedger.getParties(Seq(party2)).map(_.head))
        retrievedParty2 <- eventually(sqlLedger.getParties(Seq(party1)).map(_.head))
      } yield {
        partyAllocationResult1 shouldBe SubmissionResult.Acknowledged
        retrievedParty1.displayName shouldBe Some("displayname1")
        partyAllocationResult2 shouldBe SubmissionResult.Acknowledged
        partyAllocationResult3 shouldBe SubmissionResult.Acknowledged
        retrievedParty2.displayName shouldBe Some("displayname1")
        val logs =
          LogCollector.read[this.type]("com.daml.platform.sandbox.stores.ledger.sql.SqlLedger")
        logs should contain(
          Level.WARN -> "Ignoring duplicate party submission with ID party1 for submissionId Some(submissionId2)"
        )
      }
    }
  }

  private val retryStrategy = RetryStrategy.exponentialBackoff(10, Duration(12, "millis"))

  private def eventually[T](f: => Future[T]): Future[T] = retryStrategy.apply((_, _) => f)

  private def createSqlLedger(validatePartyAllocation: Boolean): Future[Ledger] =
    createSqlLedger(None, None, List.empty, validatePartyAllocation)

  private def createSqlLedger(
      ledgerId: String,
      validatePartyAllocation: Boolean,
  ): Future[Ledger] =
    createSqlLedger(ledgerId, List.empty, validatePartyAllocation)

  private def createSqlLedger(
      ledgerId: LedgerId,
      validatePartyAllocation: Boolean,
  ): Future[Ledger] =
    createSqlLedger(ledgerId, List.empty, validatePartyAllocation)

  private def createSqlLedger(
      packages: List[DamlLf.Archive],
      validatePartyAllocation: Boolean,
  ): Future[Ledger] =
    createSqlLedger(None, None, packages, validatePartyAllocation)

  private def createSqlLedger(
      ledgerId: String,
      packages: List[DamlLf.Archive],
      validatePartyAllocation: Boolean,
  ): Future[Ledger] = {
    val assertedLedgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString(ledgerId))
    createSqlLedger(assertedLedgerId, packages, validatePartyAllocation)
  }

  private def createSqlLedger(
      ledgerId: LedgerId,
      packages: List[DamlLf.Archive],
      validatePartyAllocation: Boolean,
  ): Future[Ledger] =
    createSqlLedger(Some(ledgerId), None, packages, validatePartyAllocation)

  private def createSqlLedgerWithParticipantId(
      participantId: ParticipantId,
      validatePartyAllocation: Boolean,
  ): Future[Ledger] =
    createSqlLedger(None, Some(participantId), List.empty, validatePartyAllocation)

  private def makeParticipantId(id: String): ParticipantId =
    ParticipantId(Ref.ParticipantId.assertFromString(id))

  private val DefaultParticipantId = makeParticipantId("test-participant-id")

  private def createSqlLedger(
      ledgerId: Option[LedgerId],
      participantId: Option[ParticipantId],
      packages: List[DamlLf.Archive],
      validatePartyAllocation: Boolean,
  ): Future[Ledger] = {
    metrics.getNames.forEach(name => { val _ = metrics.remove(name) })
    val ledger =
      new SqlLedger.Owner(
        name = LedgerName(getClass.getSimpleName),
        serverRole = ServerRole.Testing(getClass),
        jdbcUrl = postgresDatabase.url,
        databaseConnectionPoolSize = 16,
        databaseConnectionTimeout = 250.millis,
        providedLedgerId = ledgerId.fold[LedgerIdMode](LedgerIdMode.Dynamic)(LedgerIdMode.Static),
        participantId = participantId.getOrElse(DefaultParticipantId),
        timeProvider = TimeProvider.UTC,
        packages = InMemoryPackageStore.empty
          .withPackages(Instant.EPOCH, None, packages)
          .fold(sys.error, identity),
        initialLedgerEntries = ImmArray.empty,
        queueDepth = queueDepth,
        transactionCommitter = LegacyTransactionCommitter,
        startMode = SqlStartMode.MigrateAndStart,
        eventsPageSize = 100,
        eventsProcessingParallelism = 8,
        servicesExecutionContext = executionContext,
        metrics = new Metrics(metrics),
        lfValueTranslationCache = LfValueTranslationCache.Cache.none,
        engine = new Engine(),
        validatePartyAllocation = validatePartyAllocation,
        enableAppendOnlySchema = true,
        enableCompression = false,
      ).acquire()(ResourceContext(system.dispatcher))
    createdLedgers += ledger
    ledger.asFuture
  }
}

object SqlLedgerSpecAppendOnly {
  private val queueDepth = 128

  private val ledgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString("TheLedger"))

  private val testDar =
    DarParser.assertReadArchiveFromFile(new File(rlocation(ModelTestDar.path)))
}
