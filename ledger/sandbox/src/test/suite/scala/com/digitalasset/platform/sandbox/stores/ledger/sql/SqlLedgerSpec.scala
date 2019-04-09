// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundEach
import org.scalatest.{AsyncWordSpec, Matchers}

class SqlLedgerSpec
    extends AsyncWordSpec
    with Matchers
    with PostgresAroundEach
    with AkkaBeforeAndAfterAll {

  "SQL Ledger" should {
    "be able to be created from scratch with a random ledger id" in {
      val ledgerF = SqlLedger(
        jdbcUrl = postgresFixture.jdbcUrl,
        ledgerId = None,
        timeProvider = TimeProvider.UTC,
        ledgerEntries = Nil)

      ledgerF.map { ledger =>
        ledger.ledgerId should not be equal("")
      }
    }

    "be able to be created from scratch with a given ledger id" in {
      val ledgerId = "TheLedger"

      val ledgerF = SqlLedger(
        jdbcUrl = postgresFixture.jdbcUrl,
        ledgerId = Some(ledgerId),
        timeProvider = TimeProvider.UTC,
        ledgerEntries = Nil)

      ledgerF.map { ledger =>
        ledger.ledgerId should not be equal(ledgerId)
      }
    }

    "be able to be reused keeping the old ledger id" in {
      val ledgerId = "TheLedger"

      for {
        ledger1 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(ledgerId),
          timeProvider = TimeProvider.UTC,
          ledgerEntries = Nil)
        ledger2 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(ledgerId),
          timeProvider = TimeProvider.UTC,
          ledgerEntries = Nil)
        ledger3 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = None,
          timeProvider = TimeProvider.UTC,
          ledgerEntries = Nil)
      } yield {
        ledger1.ledgerId should not be equal(ledgerId)
        ledger1.ledgerId shouldEqual ledger2.ledgerId
        ledger2.ledgerId shouldEqual ledger3.ledgerId
      }
    }

    "refuse to create a new ledger when there is already one with a different ledger id" in {

      val ledgerF = for {
        _ <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some("TheLedger"),
          timeProvider = TimeProvider.UTC,
          ledgerEntries = Nil
        )
        _ <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some("AnotherLedger"),
          timeProvider = TimeProvider.UTC,
          ledgerEntries = Nil
        )
      } yield (())

      ledgerF.failed.map { t =>
        t.getMessage shouldEqual "Ledger id mismatch. Ledger id given ('AnotherLedger') is not equal to the existing one ('TheLedger')!"
      }
    }

  }

}
