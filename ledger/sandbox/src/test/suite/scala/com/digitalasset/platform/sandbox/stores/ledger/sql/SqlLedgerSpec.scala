// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

import java.nio.file.Paths
import java.time.Instant

import com.daml.api.util.TimeProvider
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.health.Healthy
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.archive.DarReader
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.LegacyTransactionCommitter
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.ServerRole
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.MetricsAround
import com.daml.platform.sandbox.stores.InMemoryActiveLedgerState
import com.daml.platform.sandbox.stores.ledger.Ledger
import com.daml.platform.sandbox.stores.ledger.sql.SqlLedgerSpec._
import com.daml.resources.Resource
import com.daml.testing.postgresql.PostgresAroundEach
import org.scalatest.concurrent.{AsyncTimeLimitedTests, Eventually, ScaledTimeSpans}
import org.scalatest.time.{Minute, Seconds, Span}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

class SqlLedgerSpec
    extends AsyncWordSpec
    with Matchers
    with AsyncTimeLimitedTests
    with ScaledTimeSpans
    with Eventually
    with AkkaBeforeAndAfterAll
    with PostgresAroundEach
    with MetricsAround {

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
          "The provided ledger ID does not match the existing ID. Existing: \"TheLedger\", Provided: \"AnotherLedger\".")
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
    createSqlLedger(None, List.empty)

  private def createSqlLedger(ledgerId: String): Future[Ledger] =
    createSqlLedger(ledgerId, List.empty)

  private def createSqlLedger(ledgerId: LedgerId): Future[Ledger] =
    createSqlLedger(ledgerId, List.empty)

  private def createSqlLedger(packages: List[DamlLf.Archive]): Future[Ledger] =
    createSqlLedger(None, packages)

  private def createSqlLedger(ledgerId: String, packages: List[DamlLf.Archive]): Future[Ledger] = {
    val assertedLedgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString(ledgerId))
    createSqlLedger(assertedLedgerId, packages)
  }

  private def createSqlLedger(ledgerId: LedgerId, packages: List[DamlLf.Archive]): Future[Ledger] =
    createSqlLedger(Some(ledgerId), packages)

  private def createSqlLedger(
      ledgerId: Option[LedgerId],
      packages: List[DamlLf.Archive],
  ): Future[Ledger] = {
    metrics.getNames.forEach(name => { val _ = metrics.remove(name) })
    val ledger = newLoggingContext { implicit logCtx =>
      SqlLedger
        .owner(
          serverRole = ServerRole.Testing(getClass),
          jdbcUrl = postgresDatabase.url,
          ledgerId = ledgerId.fold[LedgerIdMode](LedgerIdMode.Dynamic)(LedgerIdMode.Static),
          participantId = participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty
            .withPackages(Instant.EPOCH, None, packages)
            .fold(sys.error, identity),
          initialLedgerEntries = ImmArray.empty,
          queueDepth = queueDepth,
          transactionCommitter = LegacyTransactionCommitter,
          startMode = SqlStartMode.ContinueIfExists,
          eventsPageSize = 100,
          metrics = new Metrics(metrics),
        )
        .acquire()(system.dispatcher)
    }
    createdLedgers += ledger
    ledger.asFuture
  }
}

object SqlLedgerSpec {
  private val queueDepth = 128

  private val ledgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString("TheLedger"))
  private val participantId: ParticipantId = Ref.ParticipantId.assertFromString("TheParticipant")

  private val testArchivePath = rlocation(Paths.get("ledger/test-common/Test-stable.dar"))
  private val darReader = DarReader { (_, stream) =>
    Try(DamlLf.Archive.parseFrom(stream))
  }
  private lazy val Success(testDar) =
    darReader.readArchiveFromFile(testArchivePath.toFile)
}
