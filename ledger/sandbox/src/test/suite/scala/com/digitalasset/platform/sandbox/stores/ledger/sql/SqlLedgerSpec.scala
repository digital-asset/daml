// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.health.{Healthy, Unhealthy}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.packages.InMemoryPackageStore
import com.digitalasset.platform.sandbox.MetricsAround
import com.digitalasset.platform.sandbox.stores.InMemoryActiveLedgerState
import com.digitalasset.platform.sandbox.stores.ledger.Ledger
import com.digitalasset.resources.Resource
import com.digitalasset.testing.postgresql.PostgresAroundEach
import org.scalatest.concurrent.{AsyncTimeLimitedTests, Eventually, ScaledTimeSpans}
import org.scalatest.time.{Minute, Seconds, Span}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

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

  private val queueDepth = 128

  private val ledgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString("TheLedger"))
  private val participantId: ParticipantId = Ref.ParticipantId.assertFromString("TheParticipant")

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
        ledger.ledgerId should not be equal("")
      }
    }

    "be able to be created from scratch with a given ledger ID" in {
      for {
        ledger <- createSqlLedger(ledgerId)
      } yield {
        ledger.ledgerId should not be equal(LedgerId)
      }
    }

    "be able to be reused keeping the old ledger ID" in {
      for {
        ledger1 <- createSqlLedger(ledgerId)
        ledger2 <- createSqlLedger(ledgerId)
        ledger3 <- createSqlLedger()
      } yield {
        ledger1.ledgerId should not be equal(LedgerId)
        ledger1.ledgerId shouldEqual ledger2.ledgerId
        ledger2.ledgerId shouldEqual ledger3.ledgerId
      }
    }

    "refuse to create a new ledger when there is already one with a different ledger ID" in {
      for {
        _ <- createSqlLedger(ledgerId = "TheLedger")
        throwable <- createSqlLedger(ledgerId = "AnotherLedger").failed
      } yield {
        throwable.getMessage shouldEqual "Ledger id mismatch. Ledger id given ('AnotherLedger') is not equal to the existing one ('TheLedger')!"
      }
    }

    "be healthy" in {
      for {
        ledger <- createSqlLedger()
      } yield {
        ledger.currentHealth() should be(Healthy)
      }
    }

    "be unhealthy if the underlying database is inaccessible 3 or more times in a row" in {
      for {
        ledger <- createSqlLedger()
      } yield {
        withClue("before shutting down postgres,") {
          ledger.currentHealth() should be(Healthy)
        }

        stopPostgres()

        eventually {
          withClue("after shutting down postgres,") {
            ledger.currentHealth() should be(Unhealthy)
          }
        }

        startPostgres()

        eventually {
          withClue("after starting up postgres,") {
            ledger.currentHealth() should be(Healthy)
          }
        }
      }
    }
  }

  private def createSqlLedger(): Future[Ledger] =
    createSqlLedger(None)

  private def createSqlLedger(ledgerId: LedgerId): Future[Ledger] =
    createSqlLedger(Some(ledgerId))

  private def createSqlLedger(ledgerId: String): Future[Ledger] = {
    val assertedLedgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString(ledgerId))
    createSqlLedger(Some(assertedLedgerId))
  }

  private def createSqlLedger(ledgerId: Option[LedgerId]): Future[Ledger] = {
    metrics.getNames.forEach(name => { val _ = metrics.remove(name) })
    val ledger = newLoggingContext { implicit logCtx =>
      SqlLedger
        .owner(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = ledgerId,
          participantId = participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          metrics,
        )
        .acquire()(system.dispatcher)
    }
    createdLedgers += ledger
    ledger.asFuture
  }
}
