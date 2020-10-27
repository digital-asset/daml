// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

import java.nio.file.Paths
import java.time.Instant

import com.daml.api.util.TimeProvider
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.api.health.Healthy
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, TestResourceContext}
import com.daml.lf.archive.DarReader
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.LegacyTransactionCommitter
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.{LedgerIdMode, MismatchException}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.MetricsAround
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.stores.ledger.Ledger
import com.daml.platform.sandbox.stores.ledger.sql.SqlLedgerSpec._
import com.daml.platform.store.IndexMetadata
import com.daml.platform.store.dao.events.LfValueTranslation
import com.daml.testing.postgresql.PostgresAroundEach
import org.scalatest.concurrent.{AsyncTimeLimitedTests, Eventually, ScaledTimeSpans}
import org.scalatest.time.{Minute, Seconds, Span}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

final class SqlLedgerSpec
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
    for (ledger <- createdLedgers)
      Await.result(ledger.release(), 2.seconds)
    super.afterEach()
  }

  "SQL Ledger" should {
    "be able to be created from scratch with a random ledger ID" in {
      for {
        ledger <- createSqlLedger()
      } yield {
        ledger.ledgerId should not be ""
      }
    }

    "be able to be created from scratch with a given ledger ID" in {
      for {
        ledger <- createSqlLedger(ledgerId)
      } yield {
        ledger.ledgerId should be(ledgerId)
      }
    }

    "be able to be reused keeping the old ledger ID" in {
      for {
        ledger1 <- createSqlLedger(ledgerId)
        ledger2 <- createSqlLedger(ledgerId)
        ledger3 <- createSqlLedger()
      } yield {
        ledger1.ledgerId should not be LedgerId
        ledger1.ledgerId should be(ledger2.ledgerId)
        ledger2.ledgerId should be(ledger3.ledgerId)
      }
    }

    "refuse to create a new ledger when there is already one with a different ledger ID" in {
      for {
        _ <- createSqlLedger(ledgerId = "TheLedger")
        throwable <- createSqlLedger(ledgerId = "AnotherLedger").failed
      } yield {
        throwable.getMessage should be(
          "The provided ledger id does not match the existing one. Existing: \"TheLedger\", Provided: \"AnotherLedger\".")
      }
    }

    "correctly initialized the participant ID" in {
      val participantId = makeParticipantId("TheOnlyParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(participantId)
        metadata <- IndexMetadata.read(postgresDatabase.url)
      } yield {
        metadata.participantId shouldEqual participantId
      }
    }

    "allow to resume on an existing participant ID" in {
      val participantId = makeParticipantId("TheParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(participantId)
        _ <- createSqlLedgerWithParticipantId(participantId)
        metadata <- IndexMetadata.read(postgresDatabase.url)
      } yield {
        metadata.participantId shouldEqual participantId
      }
    }

    "refuse to create a new ledger when there is already one with a different participant ID" in {
      val expectedExisting = makeParticipantId("TheParticipant")
      val expectedProvided = makeParticipantId("AnotherParticipant")
      for {
        _ <- createSqlLedgerWithParticipantId(expectedExisting)
        throwable <- createSqlLedgerWithParticipantId(expectedProvided).failed
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
        ledger <- createSqlLedger()
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size 0
      }
    }

    "load packages if provided with a dynamic ledger ID" in {
      for {
        ledger <- createSqlLedger(packages = testDar.all)
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size testDar.all.length.toLong
      }
    }

    "load packages if provided with a static ledger ID" in {
      for {
        ledger <- createSqlLedger(ledgerId = "TheLedger", packages = testDar.all)
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size testDar.all.length.toLong
      }
    }

    "load no packages if the ledger already exists" in {
      for {
        _ <- createSqlLedger(ledgerId = "TheLedger")
        ledger <- createSqlLedger(ledgerId = "TheLedger", packages = testDar.all)
        packages <- ledger.listLfPackages()
      } yield {
        packages should have size 0
      }
    }

    "be healthy" in {
      for {
        ledger <- createSqlLedger()
      } yield {
        ledger.currentHealth() should be(Healthy)
      }
    }
  }

  private def createSqlLedger(): Future[Ledger] =
    createSqlLedger(None, None, List.empty)

  private def createSqlLedger(ledgerId: String): Future[Ledger] =
    createSqlLedger(ledgerId, List.empty)

  private def createSqlLedger(ledgerId: LedgerId): Future[Ledger] =
    createSqlLedger(ledgerId, List.empty)

  private def createSqlLedger(packages: List[DamlLf.Archive]): Future[Ledger] =
    createSqlLedger(None, None, packages)

  private def createSqlLedger(ledgerId: String, packages: List[DamlLf.Archive]): Future[Ledger] = {
    val assertedLedgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString(ledgerId))
    createSqlLedger(assertedLedgerId, packages)
  }

  private def createSqlLedger(ledgerId: LedgerId, packages: List[DamlLf.Archive]): Future[Ledger] =
    createSqlLedger(Some(ledgerId), None, packages)

  private def createSqlLedgerWithParticipantId(participantId: ParticipantId): Future[Ledger] =
    createSqlLedger(None, Some(participantId), List.empty)

  private def makeParticipantId(id: String): ParticipantId =
    ParticipantId(Ref.ParticipantId.assertFromString(id))

  private val DefaultParticipantId = makeParticipantId("test-participant-id")

  private def createSqlLedger(
      ledgerId: Option[LedgerId],
      participantId: Option[ParticipantId],
      packages: List[DamlLf.Archive],
  ): Future[Ledger] = {
    metrics.getNames.forEach(name => { val _ = metrics.remove(name) })
    val ledger =
      new SqlLedger.Owner(
        name = LedgerName(getClass.getSimpleName),
        serverRole = ServerRole.Testing(getClass),
        jdbcUrl = postgresDatabase.url,
        providedLedgerId = ledgerId.fold[LedgerIdMode](LedgerIdMode.Dynamic)(LedgerIdMode.Static),
        participantId = participantId.getOrElse(DefaultParticipantId),
        timeProvider = TimeProvider.UTC,
        packages = InMemoryPackageStore.empty
          .withPackages(Instant.EPOCH, None, packages)
          .fold(sys.error, identity),
        initialLedgerEntries = ImmArray.empty,
        queueDepth = queueDepth,
        transactionCommitter = LegacyTransactionCommitter,
        startMode = SqlStartMode.ContinueIfExists,
        eventsPageSize = 100,
        metrics = new Metrics(metrics),
        lfValueTranslationCache = LfValueTranslation.Cache.none,
      ).acquire()(ResourceContext(system.dispatcher))
    createdLedgers += ledger
    ledger.asFuture
  }
}

object SqlLedgerSpec {
  private val queueDepth = 128

  private val ledgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString("TheLedger"))

  private val testArchivePath = rlocation(Paths.get("ledger/test-common/model-tests.dar"))
  private val darReader = DarReader { (_, stream) =>
    Try(DamlLf.Archive.parseFrom(stream))
  }
  private lazy val Success(testDar) =
    darReader.readArchiveFromFile(testArchivePath.toFile)
}
