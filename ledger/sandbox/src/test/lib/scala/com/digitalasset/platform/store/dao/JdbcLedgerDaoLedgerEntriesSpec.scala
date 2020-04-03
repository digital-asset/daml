// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.time.Instant

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Node.{KeyWithMaintainers, NodeCreate, NodeFetch}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ValueText, VersionedValue}
import com.digitalasset.daml.lf.value.ValueVersions
import com.digitalasset.ledger.api.domain.RejectionReason
import com.digitalasset.platform.store.PersistenceEntry
import com.digitalasset.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, LoneElement, Matchers}

import scala.collection.immutable.HashMap

private[dao] trait JdbcLedgerDaoLedgerEntriesSpec extends LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (ledger entries)"

  it should "be able to persist and load a rejection" in {
    val offset = nextOffset()
    val rejection = LedgerEntry.Rejection(
      Instant.now,
      s"commandId-${offset.toLong}",
      s"applicationId-${offset.toLong}",
      "party",
      RejectionReason.Inconsistent("\uED7Eᇫ뭳ꝳꍆꃓ왎"))

    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(offset, PersistenceEntry.Rejection(rejection))
      entry <- ledgerDao.lookupLedgerEntry(offset)
      endingOffset <- ledgerDao.lookupLedgerEnd()
    } yield {
      entry shouldEqual Some(rejection)
      endingOffset should be > startingOffset
    }
  }

  it should "be able to persist and load a transaction" in {
    val offset = nextOffset()
    val absCid = AbsoluteContractId.assertFromString("#cId2")
    val let = Instant.now
    val txid = "trId2"
    val event1 = event(txid, 1)
    val event2 = event(txid, 2)

    val keyWithMaintainers = KeyWithMaintainers(
      VersionedValue(ValueVersions.acceptedVersions.head, ValueText("key2")),
      Set(Ref.Party.assertFromString("Alice"))
    )

    val transaction = LedgerEntry.Transaction(
      Some("commandId2"),
      txid,
      Some("appID2"),
      Some("Alice"),
      Some("workflowId"),
      let,
      let,
      GenTransaction(
        HashMap(
          event1 -> NodeCreate(
            nodeSeed = None,
            coid = absCid,
            coinst = someContractInstance,
            optLocation = None,
            signatories = Set(alice, bob),
            stakeholders = Set(alice, bob),
            key = Some(keyWithMaintainers)
          )),
        ImmArray(event1),
      ),
      Map(event1 -> Set("Alice", "Bob"), event2 -> Set("Alice", "In", "Chains"))
    )

    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(
        offset,
        PersistenceEntry.Transaction(transaction, Map.empty, List.empty))
      entry <- ledgerDao.lookupLedgerEntry(offset)
      endingOffset <- ledgerDao.lookupLedgerEnd()
    } yield {
      entry shouldEqual Some(transaction)
      endingOffset should be > startingOffset
    }
  }

  it should "be able to load contracts within a transaction" in {
    val offset = nextOffset()
    val offsetString = offset.toLong
    val absCid = AbsoluteContractId.assertFromString(s"#cId$offsetString")
    val let = Instant.now

    val transactionId = s"trId$offsetString"
    val event1 = event(transactionId, 1)
    val event2 = event(transactionId, 2)

    val transaction = LedgerEntry.Transaction(
      Some(s"commandId$offsetString"),
      transactionId,
      Some(s"appID$offsetString"),
      Some("Alice"),
      Some("workflowId"),
      let,
      // normally the record time is some time after the ledger effective time
      let.plusMillis(42),
      GenTransaction(
        HashMap(
          event1 -> NodeCreate(
            nodeSeed = None,
            coid = absCid,
            coinst = someContractInstance,
            optLocation = None,
            signatories = Set(alice, bob),
            stakeholders = Set(alice, bob),
            key = None
          ),
          event2 -> NodeFetch(
            absCid,
            someTemplateId,
            None,
            Some(Set(alice, bob)),
            Set(alice, bob),
            Set(alice, bob),
            None,
          )
        ),
        ImmArray(event1, event2),
      ),
      Map(event1 -> Set("Alice", "Bob"), event2 -> Set("Alice", "In", "Chains"))
    )

    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(
        offset,
        PersistenceEntry.Transaction(transaction, Map.empty, List.empty))
      entry <- ledgerDao.lookupLedgerEntry(offset)
      endingOffset <- ledgerDao.lookupLedgerEnd()
    } yield {
      entry shouldEqual Some(transaction)
      endingOffset should be > startingOffset
    }
  }

}
