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
import com.digitalasset.platform.sandbox.stores.ledger.PartyIdGenerator
import com.digitalasset.platform.sandbox.stores.{InMemoryActiveLedgerState, InMemoryPackageStore}
import org.scalatest.concurrent.{AsyncTimeLimitedTests, Eventually, ScaledTimeSpans}
import org.scalatest.time.{Second, Span}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

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
    "be able to be created from scratch with a random ledger id" in {
      val ledgerF = SqlLedger(
        jdbcUrl = postgresFixture.jdbcUrl,
        ledgerId = None,
        participantId = participantId,
        timeProvider = TimeProvider.UTC,
        acs = InMemoryActiveLedgerState.empty,
        packages = InMemoryPackageStore.empty,
        initialLedgerEntries = ImmArray.empty,
        queueDepth,
        startMode = SqlStartMode.ContinueIfExists,
        loggerFactory,
        metrics
      )

      ledgerF.map { ledger =>
        ledger.ledgerId should not be equal("")
      }
    }

    "be able to be created from scratch with a given ledger id" in {
      val ledgerF = SqlLedger(
        jdbcUrl = postgresFixture.jdbcUrl,
        ledgerId = Some(ledgerId),
        participantId = participantId,
        timeProvider = TimeProvider.UTC,
        acs = InMemoryActiveLedgerState.empty,
        packages = InMemoryPackageStore.empty,
        initialLedgerEntries = ImmArray.empty,
        queueDepth,
        startMode = SqlStartMode.ContinueIfExists,
        loggerFactory,
        metrics
      )

      ledgerF.map { ledger =>
        ledger.ledgerId should not be equal(LedgerId)
      }
    }

    "be able to be reused keeping the old ledger id" in {

      for {
        ledger1 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(ledgerId),
          participantId = participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory,
          metrics
        )

        ledger2 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(ledgerId),
          participantId = participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory,
          metrics
        )

        ledger3 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = None,
          participantId = participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory,
          metrics
        )

      } yield {
        ledger1.ledgerId should not be equal(LedgerId)
        ledger1.ledgerId shouldEqual ledger2.ledgerId
        ledger2.ledgerId shouldEqual ledger3.ledgerId
      }
    }

    "refuse to create a new ledger when there is already one with a different ledger id" in {

      val ledgerF = for {
        _ <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(LedgerId(Ref.LedgerString.assertFromString("TheLedger"))),
          participantId = participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory,
          metrics
        )
        _ <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(LedgerId(Ref.LedgerString.assertFromString("AnotherLedger"))),
          participantId = participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory,
          metrics
        )
      } yield ()

      ledgerF.failed.map { t =>
        t.getMessage shouldEqual "Ledger id mismatch. Ledger id given ('AnotherLedger') is not equal to the existing one ('TheLedger')!"
      }
    }

    "be healthy" in {
      for {
        ledger <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = None,
          participantId = participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory,
          metrics
        )
      } yield {
        ledger.currentHealth() should be(Healthy)
      }
    }

    "be unhealthy if the underlying database is inaccessible 3 or more times in a row" in {
      for {
        ledger <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = None,
          participantId = participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory,
          metrics
        )
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
        withClue("after allocating Dan,") {
          ledger.currentHealth() should be(Unhealthy)
        }

        startPostgres()

        eventually {
          allocateParty("Erin")
          withClue("after allocating Erin,") {
            ledger.currentHealth() should be(Healthy)
          }
        }
      }
    }
  }
}
