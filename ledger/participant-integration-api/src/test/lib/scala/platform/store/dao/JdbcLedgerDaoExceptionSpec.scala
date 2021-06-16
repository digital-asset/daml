// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.{ContractId, ContractInst}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

/** There are two important parts to cover with Daml exceptions:
  * - Create and exercise nodes under rollback nodes should not be indexed
  * - Lookup and fetch nodes under rollback nodes may lead to divulgence
  */
private[dao] trait JdbcLedgerDaoExceptionSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def contractsReader = ledgerDao.contractsReader

  behavior of "JdbcLedgerDao (exceptions)"

  it should "not find contracts created under rollback nodes" in {
    val builder = TransactionBuilder(TransactionVersion.VDev)
    val rollback = builder.add(builder.rollback())
    val cid1 = builder.newCid
    val cid2 = builder.newCid
    builder.add(
      createNode(absCid = cid1, signatories = Set(alice), stakeholders = Set(alice)),
      rollback,
    )
    builder.add(createNode(absCid = cid2, signatories = Set(alice), stakeholders = Set(alice)))
    val offsetAndEntry = fromTransaction(builder.buildCommitted())

    for {
      _ <- store(offsetAndEntry)
      result1 <- contractsReader.lookupActiveContractAndLoadArgument(Set(alice), cid1)
      result2 <- contractsReader.lookupActiveContractAndLoadArgument(Set(alice), cid2)
    } yield {
      result1 shouldBe None
      result2.value shouldBe a[ContractInst[_]]
    }
  }

  it should "divulge contracts fetched under rollback nodes" in {
    val stakeholders = Set(alice)
    val divulgees = Set(bob)

    // A transaction that fetches a contract under a rollback node
    def rolledBackFetch(createCid: ContractId, fetcherCid: ContractId) = {
      val builder = TransactionBuilder(TransactionVersion.VDev)
      val exercise1 = exerciseNode(fetcherCid).copy(
        consuming = false,
        actingParties = stakeholders,
        signatories = divulgees,
        stakeholders = stakeholders.union(divulgees),
      )
      val rollback = builder.rollback()
      val exercise2 = fetchNode(createCid).copy(
        actingParties = stakeholders,
        signatories = stakeholders,
        stakeholders = stakeholders,
      )

      val exercise1Nid = builder.add(exercise1)
      val rollbackNid = builder.add(rollback, exercise1Nid)
      builder.add(exercise2, rollbackNid)
      fromTransaction(builder.buildCommitted()).copy()
    }

    for {
      // Create contract to be divulged
      (_, tx1) <- createAndStoreContract(
        submittingParties = stakeholders,
        signatories = stakeholders,
        stakeholders = stakeholders,
        key = None,
      )
      create = nonTransient(tx1).loneElement
      createCid = ContractId.assertFromString(create.coid)

      // Create "Fetcher" contract
      (_, tx2) <- createAndStoreContract(
        submittingParties = divulgees,
        signatories = divulgees,
        stakeholders = stakeholders.union(divulgees),
        key = None,
      )
      fetcher = nonTransient(tx1).loneElement
      fetcherCid = ContractId.assertFromString(fetcher.coid)

      // Exercise a choice on the "Fetcher" contract that divulges the first contract
      _ <- store(
        divulgedContracts = Map((create, someVersionedContractInstance) -> divulgees),
        blindingInfo = None,
        offsetAndTx = rolledBackFetch(createCid, fetcherCid),
      )
      resultAlice <- contractsReader.lookupActiveContractAndLoadArgument(Set(alice), createCid)
      resultBob <- contractsReader.lookupActiveContractAndLoadArgument(Set(bob), createCid)
      resultCharlie <- contractsReader.lookupActiveContractAndLoadArgument(Set(charlie), createCid)
    } yield {
      withClue("Alice is stakeholder") {
        resultAlice.value shouldBe a[ContractInst[_]]
      }
      withClue("Contract was divulged to Bob under a rollback node") {
        resultBob.value shouldBe a[ContractInst[_]]
      }
      withClue("Charlie is unrelated and must not see the contract") {
        resultCharlie shouldBe None
      }
    }
  }

}
