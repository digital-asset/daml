// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID
import com.daml.ledger.api.domain.CommandId
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationDuplicate,
  CommandDeduplicationNew,
}
import com.daml.lf.data.Time.Timestamp
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[dao] trait JdbcLedgerDaoCommandDeduplicationSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (command deduplication)"

  it should "correctly deduplicate a command" in {
    val commandId: CommandId = CommandId(UUID.randomUUID.toString)
    for {
      original <- ledgerDao.deduplicateCommand(commandId, List(alice), t(0), t(5000))
      duplicate <- ledgerDao.deduplicateCommand(commandId, List(alice), t(500), t(5500))
    } yield {
      original shouldBe CommandDeduplicationNew
      duplicate shouldBe CommandDeduplicationDuplicate(t(5000))
    }
  }

  it should "correctly deduplicate commands with multiple submitters" in {
    val commandId: CommandId = CommandId(UUID.randomUUID.toString)
    for {
      original <- ledgerDao.deduplicateCommand(commandId, List(alice, bob), t(0), t(5000))
      duplicate1 <- ledgerDao.deduplicateCommand(commandId, List(alice, bob), t(500), t(5500))
      duplicate2 <- ledgerDao.deduplicateCommand(commandId, List(bob, alice), t(500), t(5500))
      duplicate3 <- ledgerDao.deduplicateCommand(
        commandId,
        List(alice, bob, alice),
        t(500),
        t(5500),
      )
    } yield {
      original shouldBe CommandDeduplicationNew
      duplicate1 shouldBe CommandDeduplicationDuplicate(t(5000))
      duplicate2 shouldBe CommandDeduplicationDuplicate(t(5000))
      duplicate3 shouldBe CommandDeduplicationDuplicate(t(5000))
    }
  }

  it should "not deduplicate a command after it expired" in {
    val commandId: CommandId = CommandId(UUID.randomUUID.toString)
    for {
      original1 <- ledgerDao.deduplicateCommand(commandId, List(alice), t(0), t(100))
      original2 <- ledgerDao.deduplicateCommand(commandId, List(alice), t(101), t(200))
    } yield {
      original1 shouldBe CommandDeduplicationNew
      original2 shouldBe CommandDeduplicationNew
    }
  }

  it should "not deduplicate a command after its deduplication was stopped" in {
    val commandId: CommandId = CommandId(UUID.randomUUID.toString)
    for {
      original1 <- ledgerDao.deduplicateCommand(commandId, List(alice), t(0), t(10000))
      _ <- ledgerDao.stopDeduplicatingCommand(commandId, List(alice))
      original2 <- ledgerDao.deduplicateCommand(commandId, List(alice), t(1), t(10001))
    } yield {
      original1 shouldBe CommandDeduplicationNew
      original2 shouldBe CommandDeduplicationNew
    }
  }

  it should "not deduplicate commands with different command ids" in {
    val commandId1: CommandId = CommandId(UUID.randomUUID.toString)
    val commandId2: CommandId = CommandId(UUID.randomUUID.toString)
    for {
      original1 <- ledgerDao.deduplicateCommand(commandId1, List(alice, bob), t(0), t(1000))
      original2 <- ledgerDao.deduplicateCommand(commandId2, List(alice, bob), t(0), t(1000))
    } yield {
      original1 shouldBe CommandDeduplicationNew
      original2 shouldBe CommandDeduplicationNew
    }
  }

  it should "not deduplicate commands with different submitters" in {
    val commandId: CommandId = CommandId(UUID.randomUUID.toString)
    for {
      original1 <- ledgerDao.deduplicateCommand(commandId, List(alice, bob), t(0), t(1000))
      original2 <- ledgerDao.deduplicateCommand(commandId, List(alice, charlie), t(0), t(1000))
    } yield {
      original1 shouldBe CommandDeduplicationNew
      original2 shouldBe CommandDeduplicationNew
    }
  }

  private[this] val t0 = Instant.now()
  private[this] def t(ms: Long): Timestamp = {
    Timestamp.assertFromInstant(t0.plusMillis(ms))
  }
}
