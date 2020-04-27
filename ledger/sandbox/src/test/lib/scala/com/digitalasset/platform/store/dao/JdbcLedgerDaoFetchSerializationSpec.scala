// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, Matchers}

private[dao] trait JdbcLedgerDaoFetchSerializationSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (fetch serialization)"

  it should "serialize a valid fetch" in {
    val let = Instant.now
    val offset1 = nextOffset()
    val offset2 = nextOffset()
    val keyValue = s"key-${offset1.toLong}"

    for {
      _ <- ledgerDao.storeLedgerEntry(
        offset1,
        txCreateContractWithKey(let, offset1, alice, keyValue)
      )
      _ <- ledgerDao.storeLedgerEntry(
        offset2,
        txFetch(let, offset2, alice, offset1)
      )
      res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
      res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)
    } yield {
      res1 shouldBe a[LedgerEntry.Transaction]
      res2 shouldBe a[LedgerEntry.Transaction]
    }
  }

  it should "refuse to serialize invalid fetch" in {
    val let = Instant.now
    val offset1 = nextOffset()
    val offset2 = nextOffset()
    val offset3 = nextOffset()
    val keyValue = s"key-${offset1.toLong}"

    // Scenario: Two concurrent commands: one exercise and one fetch.
    // At command interpretation time, the fetch finds a contract.
    // At serialization time, it should be rejected because now the contract was archived.
    for {
      _ <- ledgerDao.storeLedgerEntry(
        offset1,
        txCreateContractWithKey(let, offset1, alice, keyValue)
      )
      _ <- ledgerDao.storeLedgerEntry(
        offset2,
        txArchiveContract(let, offset2, alice, offset1, keyValue)
      )
      _ <- ledgerDao.storeLedgerEntry(
        offset3,
        txFetch(let, offset3, alice, offset1)
      )
      res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
      res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)
      res3 <- ledgerDao.lookupLedgerEntryAssert(offset3)
    } yield {
      res1 shouldBe a[LedgerEntry.Transaction]
      res2 shouldBe a[LedgerEntry.Transaction]
      res3 shouldBe a[LedgerEntry.Rejection]
    }
  }

}
