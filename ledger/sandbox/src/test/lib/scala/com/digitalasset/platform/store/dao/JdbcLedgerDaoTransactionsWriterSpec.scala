// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.util.UUID

import com.daml.lf.value.Value.ContractId
import org.scalatest.{AsyncFlatSpec, LoneElement, Matchers}

private[dao] trait JdbcLedgerDaoTransactionsWriterSpec extends LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private val ok = io.grpc.Status.Code.OK.value()

  behavior of "JdbcLedgerDao (TransationsWriter)"

  it should "serialize a valid positive lookupByKey" in {
    val keyValue = s"positive-lookup-by-key"

    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(txCreateContractWithKey(alice, keyValue))
      createdContractId = nonTransient(create).loneElement
      (_, lookup) <- store(txLookupByKey(alice, keyValue, Some(createdContractId)))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain allOf (
        create.commandId.get -> ok,
        lookup.commandId.get -> ok,
      )
    }
  }

  it should "serialize a valid fetch" in {
    val keyValue = s"valid-fetch"

    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(txCreateContractWithKey(alice, keyValue))
      createdContractId = nonTransient(create).loneElement
      (_, fetch) <- store(txFetch(alice, createdContractId))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain allOf (
        create.commandId.get -> ok,
        fetch.commandId.get -> ok,
      )
    }
  }

  it should "serialize a batch of transactions with transient contracts" in {
    val divulgedContractId = ContractId.assertFromString(s"#divulged-${UUID.randomUUID}")

    // This test writes a batch of transactions that create one regular and one divulged
    // contract that are archived within the same batch.
    for {
      from <- ledgerDao.lookupLedgerEnd()
      // TX1: create a single contract that will stay active
      (offset1, tx1) = singleCreate
      contractId = nonTransient(tx1).loneElement
      // TX2: create a single contract that will be archived within the same batch
      (offset2, tx2) = singleCreate
      transientContractId = nonTransient(tx2).loneElement
      // TX3: archive contract created in TX2
      (offset3, tx3) = singleExercise(transientContractId)
      // TX4: divulge a contract
      (offset4, tx4) = emptyTransaction(alice)
      // TX5: archive previously divulged contract
      (offset5, tx5) = singleExercise(divulgedContractId)
      _ <- storeBatch(
        List(
          offset1 -> tx1 -> Map.empty,
          offset2 -> tx2 -> Map.empty,
          offset3 -> tx3 -> Map.empty,
          offset4 -> tx4 -> Map((divulgedContractId, someContractInstance) -> Set(charlie)),
          offset5 -> tx5 -> Map.empty,
        ))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
      contractLookup <- ledgerDao.lookupActiveOrDivulgedContract(contractId, alice)
      transientLookup <- ledgerDao.lookupActiveOrDivulgedContract(transientContractId, alice)
      divulgedLookup <- ledgerDao.lookupActiveOrDivulgedContract(divulgedContractId, alice)
    } yield {
      completions should contain allOf (
        tx1.commandId.get -> ok,
        tx2.commandId.get -> ok,
        tx3.commandId.get -> ok,
        tx4.commandId.get -> ok,
        tx5.commandId.get -> ok,
      )
      contractLookup shouldNot be(None)
      transientLookup shouldBe None
      divulgedLookup shouldBe None
    }
  }

}
