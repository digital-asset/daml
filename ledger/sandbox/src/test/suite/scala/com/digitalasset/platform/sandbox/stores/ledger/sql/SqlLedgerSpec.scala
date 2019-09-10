// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.sandbox.MetricsAround
import com.digitalasset.platform.sandbox.persistence.PostgresAroundEach
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.ledger.api.domain
import com.digitalasset.platform.sandbox.stores.{InMemoryActiveContracts, InMemoryPackageStore}
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScaledTimeSpans}
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration._
import com.digitalasset.ledger.api.domain.LedgerId

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
  private val participantId =
    domain.ParticipantId(Ref.LedgerString.assertFromString("TheParticipant"))

  "SQL Ledger" should {
    "be able to be created from scratch with a random ledger id" in {
      val ledgerF = SqlLedger(
        jdbcUrl = postgresFixture.jdbcUrl,
        ledgerId = None,
        participantId,
        timeProvider = TimeProvider.UTC,
        acs = InMemoryActiveContracts.empty,
        packages = InMemoryPackageStore.empty,
        initialLedgerEntries = ImmArray.empty,
        queueDepth
      )

      ledgerF.map { ledger =>
        ledger.ledgerId should not be equal("")
        ledger.participantId shouldBe participantId
      }
    }

    "be able to be created from scratch with a given ledger id" in {
      val ledgerF = SqlLedger(
        jdbcUrl = postgresFixture.jdbcUrl,
        ledgerId = Some(ledgerId),
        participantId,
        timeProvider = TimeProvider.UTC,
        acs = InMemoryActiveContracts.empty,
        packages = InMemoryPackageStore.empty,
        initialLedgerEntries = ImmArray.empty,
        queueDepth
      )

      ledgerF.map { ledger =>
        ledger.ledgerId should not be equal(LedgerId)
        ledger.participantId shouldBe participantId
      }
    }

    "be able to be reused keeping the old ledger id" in {

      for {
        ledger1 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(ledgerId),
          participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveContracts.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth
        )

        ledger2 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(ledgerId),
          participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveContracts.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth
        )

        ledger3 <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = None,
          participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveContracts.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth
        )

      } yield {
        ledger1.ledgerId shouldBe ledgerId
        ledger1.ledgerId shouldEqual ledger2.ledgerId
        ledger2.ledgerId shouldEqual ledger3.ledgerId

        ledger1.participantId shouldBe participantId
        ledger2.participantId shouldBe participantId
        ledger3.participantId shouldBe participantId
      }
    }

    "refuse to create a new ledger when there is already one with a different ledger id" in {

      val ledgerF = for {
        _ <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(LedgerId(Ref.LedgerString.assertFromString("TheLedger"))),
          participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveContracts.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth
        )
        _ <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(LedgerId(Ref.LedgerString.assertFromString("AnotherLedger"))),
          participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveContracts.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth
        )
      } yield (())

      ledgerF.failed.map { t =>
        t.getMessage shouldEqual "Ledger id mismatch. Ledger id given ('AnotherLedger') is not equal to the existing one ('TheLedger')!"
      }
    }

    "refuse to create a new ledger when there is already one with a different participant id" in {

      val ledgerF = for {
        _ <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(ledgerId),
          participantId,
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveContracts.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth
        )
        _ <- SqlLedger(
          jdbcUrl = postgresFixture.jdbcUrl,
          ledgerId = Some(ledgerId),
          domain.ParticipantId(Ref.LedgerString.assertFromString("AnotherParticipant")),
          timeProvider = TimeProvider.UTC,
          acs = InMemoryActiveContracts.empty,
          packages = InMemoryPackageStore.empty,
          initialLedgerEntries = ImmArray.empty,
          queueDepth
        )
      } yield (())

      ledgerF.failed.map { t =>
        t.getMessage shouldEqual "Participant id mismatch. Participant id given ('AnotherParticipant') is not equal to the existing one ('TheParticipant')!"
      }
    }

  }

}
