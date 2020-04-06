// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.GenTransaction
import com.daml.lf.transaction.Node.{KeyWithMaintainers, NodeCreate}
import com.daml.lf.value.Value.{AbsoluteContractId, ValueText, VersionedValue}
import com.daml.lf.value.ValueVersions
import com.daml.platform.store.PersistenceEntry
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.collection.immutable.HashMap

private[dao] trait JdbcLedgerDaoContractsSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (contracts)"

  it should "be able to persist and load contracts" in {
    val offset = nextOffset()
    val offsetString = offset.toLong
    val absCid = AbsoluteContractId.assertFromString(s"#cId1-$offsetString")
    val txId = s"trId-$offsetString"
    val workflowId = s"workflowId-$offsetString"
    val let = Instant.now
    val keyWithMaintainers = KeyWithMaintainers(
      VersionedValue(ValueVersions.acceptedVersions.head, ValueText(s"key-$offsetString")),
      Set(alice)
    )
    val event1 = event(txId, 1)
    val event2 = event(txId, 2)

    val transaction = LedgerEntry.Transaction(
      Some("commandId1"),
      txId,
      Some(s"appID-$offsetString"),
      Some("Alice"),
      Some(workflowId),
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
      Map(event1 -> Set[Party]("Alice", "Bob"), event2 -> Set[Party]("Alice", "In", "Chains"))
    )
    for {
      result1 <- ledgerDao.lookupActiveOrDivulgedContract(absCid, alice)
      _ <- ledgerDao.storeLedgerEntry(
        offset,
        PersistenceEntry.Transaction(
          transaction,
          Map(
            absCid -> Set(Ref.Party.assertFromString("Alice"), Ref.Party.assertFromString("Bob"))),
          List.empty
        )
      )
      result2 <- ledgerDao.lookupActiveOrDivulgedContract(absCid, alice)
    } yield {
      result1 shouldEqual None
      result2 shouldEqual Some(someContractInstance)
    }

  }

}
