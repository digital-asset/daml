// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.sql.SQLException

import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.health.{Healthy, Unhealthy}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.sandbox.MetricsAround
import com.digitalasset.platform.sandbox.persistence.PostgresAroundEach
import com.digitalasset.platform.sandbox.stores.ledger.{Ledger, PartyIdGenerator}
import com.digitalasset.platform.sandbox.stores.{InMemoryActiveLedgerState, InMemoryPackageStore}
import org.scalatest.concurrent.{AsyncTimeLimitedTests, Eventually, ScaledTimeSpans}
import org.scalatest.time.{Second, Span}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration._
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

  override val timeLimit: Span = scaled(60.seconds)
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Second)))

  private val queueDepth = 128

  private val ledgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString("TheLedger"))
  private val participantId: ParticipantId = Ref.LedgerString.assertFromString("TheParticipant")

  private val loggerFactory = NamedLoggerFactory(this.getClass)

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
        def allocateParty(displayName: String): Unit = {
          Await.result(
            ledger.allocateParty(PartyIdGenerator.generateRandomId(), Some(displayName)),
            patienceConfig.timeout)
          ()
        }

        allocateParty("Alice")
        withClue("after allocating Alice,") {
          ledger.currentHealth() should be(Healthy)
        }

        stopPostgres()

        assertThrows[SQLException](allocateParty("Bob"))
        withClue("after allocating Bob,") {
          ledger.currentHealth() should be(Healthy)
        }

        assertThrows[SQLException](allocateParty("Carol"))
        withClue("after allocating Carol,") {
          ledger.currentHealth() should be(Healthy)
        }

        assertThrows[SQLException](allocateParty("Dan"))
        eventually {
          withClue("after allocating Dan,") {
            ledger.currentHealth() should be(Unhealthy)
          }
        }

        startPostgres()

        allocateParty("Erin")
        eventually {
          withClue("after allocating Erin,") {
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

  private def createSqlLedger(ledgerId: Option[LedgerId]) = SqlLedger(
    jdbcUrl = postgresFixture.jdbcUrl,
    ledgerId = ledgerId,
    participantId = participantId,
    timeProvider = TimeProvider.UTC,
    acs = InMemoryActiveLedgerState.empty,
    packages = InMemoryPackageStore.empty,
    initialLedgerEntries = ImmArray.empty,
    queueDepth,
    startMode = SqlStartMode.ContinueIfExists,
    loggerFactory,
    metrics,
  )
}
