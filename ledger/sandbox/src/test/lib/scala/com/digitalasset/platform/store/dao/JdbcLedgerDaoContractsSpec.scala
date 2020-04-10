// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import com.daml.lf.value.Value.AbsoluteContractId
import org.scalatest.{AsyncFlatSpec, LoneElement, Matchers}

private[dao] trait JdbcLedgerDaoContractsSpec extends LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (contracts)"

  it should "be able to persist and load contracts" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.lookupActiveOrDivulgedContract(nonTransient(tx).loneElement, alice)
    } yield {
      // The agreement text is always empty when retrieved from the contract store
      result shouldEqual Some(someContractInstance.copy(agreementText = ""))
    }
  }

  it should "not find contracts that are not visible to the requester" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.lookupActiveOrDivulgedContract(nonTransient(tx).loneElement, charlie)
    } yield {
      result shouldEqual None
    }
  }

  it should "prevent retrieving the maximum ledger time if some contracts are not found" in {
    val randomContractId = AbsoluteContractId.assertFromString(s"#random-${UUID.randomUUID}")
    for {
      failure <- ledgerDao.lookupMaximumLedgerTime(Set(randomContractId)).failed
    } yield {
      failure shouldBe an[IllegalArgumentException]
      failure.getMessage should startWith(
        "One or more of the following contract identifiers has been found"
      )
    }
  }

  it should "allow the retrieval of the maximum ledger time" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.lookupMaximumLedgerTime(nonTransient(tx))
    } yield {
      result should be <= Instant.now
    }
  }

  it should "allow the retrieval of the maximum ledger time even when there are divulged contracts" in {
    val divulgedContractId = AbsoluteContractId.assertFromString(s"#divulged-${UUID.randomUUID}")
    for {
      (_, tx) <- store(
        divulgedContracts = Map(
          (divulgedContractId, someContractInstance) -> Set(charlie)
        ),
        offsetAndTx = singleCreate
      )
      result <- ledgerDao.lookupMaximumLedgerTime(nonTransient(tx))
    } yield {
      result should be <= Instant.now
    }
  }

}
