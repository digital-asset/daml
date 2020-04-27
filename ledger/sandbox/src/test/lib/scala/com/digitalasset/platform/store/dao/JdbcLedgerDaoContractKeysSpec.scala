// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.GenTransaction
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.platform.ApiOffset
import com.daml.platform.store.PersistenceEntry
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.collection.immutable.HashMap

private[dao] trait JdbcLedgerDaoContractKeysSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (contract keys)"

  it should "refuse to serialize duplicate contract keys" in {
    val let = Instant.now
    val offset1 = nextOffset()
    val offset2 = nextOffset()
    val keyValue = s"key-${offset1.toLong}"

    // Scenario: Two concurrent commands create the same contract key.
    // At command interpretation time, the keys do not exist yet.
    // At serialization time, the ledger should refuse to serialize one of them.
    for {
      _ <- ledgerDao.storeLedgerEntry(
        offset1,
        txCreateContractWithKey(let, offset1, alice, keyValue)
      )
      _ <- ledgerDao.storeLedgerEntry(
        offset2,
        txCreateContractWithKey(let, offset2, alice, keyValue)
      )
      res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
      res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)
    } yield {
      res1 shouldBe a[LedgerEntry.Transaction]
      res2 shouldBe a[LedgerEntry.Rejection]
    }
  }

  it should "serialize a valid positive lookupByKey" in {
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
        txLookupByKey(let, offset2, alice, keyValue, Some(offset1))
      )
      res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
    } yield {
      res1 shouldBe a[LedgerEntry.Transaction]
    }
  }

  it should "refuse to serialize invalid negative lookupByKey" in {
    val let = Instant.now
    val offset1 = nextOffset()
    val offset2 = nextOffset()
    val keyValue = s"key-${offset1.toLong}"

    // Scenario: Two concurrent commands: one create and one lookupByKey.
    // At command interpretation time, the lookupByKey does not find any contract.
    // At serialization time, it should be rejected because now the key is there.
    for {
      _ <- ledgerDao.storeLedgerEntry(
        offset1,
        txCreateContractWithKey(let, offset1, alice, keyValue)
      )
      _ <- ledgerDao.storeLedgerEntry(
        offset2,
        txLookupByKey(let, offset2, alice, keyValue, None)
      )
      res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)
      res2 <- ledgerDao.lookupLedgerEntryAssert(offset2)
    } yield {
      res1 shouldBe a[LedgerEntry.Transaction]
      res2 shouldBe a[LedgerEntry.Rejection]
    }
  }

  it should "refuse to serialize invalid positive lookupByKey" in {
    val let = Instant.now
    val offset1 = nextOffset()
    val offset2 = nextOffset()
    val offset3 = nextOffset()
    val keyValue = s"key-${offset1.toLong}"

    // Scenario: Two concurrent commands: one exercise and one lookupByKey.
    // At command interpretation time, the lookupByKey finds a contract.
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
        txLookupByKey(let, offset3, alice, keyValue, Some(offset1))
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

  it should "be able to use divulged contract in later transaction" in {
    val let = Instant.now
    val offset1 = nextOffset()
    val offset2 = nextOffset()
    val offset3 = nextOffset()
    def emptyTxWithDivulgedContracts(offset: Offset) = {
      val id = offset.toLong
      PersistenceEntry.Transaction(
        LedgerEntry.Transaction(
          Some(s"commandId$id"),
          s"transactionId$id",
          Some("applicationId"),
          Some(alice),
          Some("workflowId"),
          let,
          let,
          GenTransaction(HashMap.empty, ImmArray.empty),
          Map.empty
        ),
        Map(AbsoluteContractId.assertFromString(s"#contractId$id") -> Set(bob)),
        List(AbsoluteContractId.assertFromString(s"#contractId$id") -> someContractInstance)
      )
    }

    for {
      // First try and index a transaction fetching a completely unknown contract.
      _ <- ledgerDao.storeLedgerEntry(
        offset1,
        txFetch(let, offset1, bob, ApiOffset.assertFromString("00")))
      res1 <- ledgerDao.lookupLedgerEntryAssert(offset1)

      // Then index a transaction that just divulges the contract to bob.
      _ <- ledgerDao.storeLedgerEntry(offset2, emptyTxWithDivulgedContracts(offset2))
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
