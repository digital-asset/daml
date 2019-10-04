// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.sandbox.MetricsAround
import com.digitalasset.platform.sandbox.persistence.PostgresAroundEach
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.platform.sandbox.stores.{InMemoryActiveLedgerState, InMemoryPackageStore}
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScaledTimeSpans}
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration._
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.platform.common.logging.NamedLoggerFactory

class SqlLedgerSpec
    extends AsyncWordSpec
    with AsyncTimeLimitedTests
    with Matchers
    with PostgresAroundEach
    with AkkaBeforeAndAfterAll
    with ScaledTimeSpans
    with MetricsAround {

  override val timeLimit: Span = scaled(60.seconds)

  private val queueDepth = 128

  private val ledgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString("TheLedger"))

  private val loggerFactory = NamedLoggerFactory(this.getClass)

  "SQL Ledger" should {
    "be able to be created from scratch with a random ledger id" in {
      val ledgerF = SqlLedger(
        jdbcUrl = postgresFixture.jdbcUrl,
        ledgerId = None,
        timeProvider = TimeProvider.UTC,
        acs = InMemoryActiveLedgerState.empty,
        packages = InMemoryPackageStore.empty,
        initialLedgerEntries = ImmArray.empty,
        queueDepth,
        startMode = SqlStartMode.ContinueIfExists,
        loggerFactory
      )

      ledgerF.map { ledger =>
        ledger.ledgerId should not be equal("")
      }
    }

    "be able to be created from scratch with a given ledger id" in {
      val ledgerF = SqlLedger(
        jdbcUrl = postgresFixture.jdbcUrl,
        ledgerId = Some(ledgerId),
        timeProvider = TimeProvider.UTC,
        acs = InMemoryActiveLedgerState.empty,
        packages = InMemoryPackageStore.empty,
        initialLedgerEntries = ImmArray.empty,
        queueDepth,
        startMode = SqlStartMode.ContinueIfExists,
        loggerFactory
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
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory
        )

        ledger2 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(ledgerId),
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory
        )

        ledger3 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = None,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory
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
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory
        )
        _ <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(LedgerId(Ref.LedgerString.assertFromString("AnotherLedger"))),
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveLedgerState.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth,
          startMode = SqlStartMode.ContinueIfExists,
          loggerFactory
        )
      } yield (())

      ledgerF.failed.map { t =>
        t.getMessage shouldEqual "Ledger id mismatch. Ledger id given ('AnotherLedger') is not equal to the existing one ('TheLedger')!"
      }
    }

  }

}
