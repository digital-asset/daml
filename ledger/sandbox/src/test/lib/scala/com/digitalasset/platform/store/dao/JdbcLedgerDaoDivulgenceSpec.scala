// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import com.daml.platform.ApiOffset
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, Matchers}

private[dao] trait JdbcLedgerDaoDivulgenceSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (divulgence)"

  it should "be able to use divulged contract in later transaction" in {
    val let = Instant.now
    val offset1 = nextOffset()
    val offset2 = nextOffset()
    val offset3 = nextOffset()

    for {
      // First try and index a transaction fetching a completely unknown contract.
      _ <- ledgerDao.storeLedgerEntry(
        offset1,
        txFetch(let, offset1, bob, ApiOffset.assertFromString("00")))
      res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)

      // Then index a transaction that just divulges the contract to bob.
      _ <- ledgerDao.storeLedgerEntry(offset2, emptyTxWithDivulgedContracts(offset2, let))
      res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)

      // Finally try and fetch the divulged contract. LedgerDao should be able to look up the divulged contract
      // and index the transaction without finding the contract metadata (LET) for it, as long as the contract
      // exists in contract_data.
      _ <- ledgerDao.storeLedgerEntry(offset3, txFetch(let.plusSeconds(1), offset3, bob, offset2))
      res3 <- ledgerDao.lookupLedgerEntryAssert(offset3)
    } yield {
      res1 shouldBe a[LedgerEntry.Rejection]
      res2 shouldBe a[LedgerEntry.Transaction]
      res3 shouldBe a[LedgerEntry.Transaction]
    }
  }

}
