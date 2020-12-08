// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import com.daml.lf.value.Value.ContractId
import org.scalatest.{Inside, LoneElement}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[dao] trait JdbcLedgerDaoContractsSpec extends LoneElement with Inside {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (contracts)"

  it should "be able to persist and load contracts" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.lookupActiveOrDivulgedContract(nonTransient(tx).loneElement, alice)
    } yield {
      // The agreement text is always empty when retrieved from the contract store
      result shouldEqual Some(someVersionedContractInstance.copy(agreementText = ""))
    }
  }

  it should "allow to divulge a contract that has already been committed" in {
    for {
      (_, tx) <- store(singleCreate)
      create = nonTransient(tx).loneElement
      _ <- store(
        divulgedContracts = Map((create, someVersionedContractInstance) -> Set(charlie)),
        blindingInfo = None,
        offsetAndTx = divulgeAlreadyCommittedContract(id = create, divulgees = Set(charlie)),
      )
      result <- ledgerDao.lookupActiveOrDivulgedContract(create, charlie)
    } yield {
      // The agreement text is always empty when retrieved from the contract store
      result shouldEqual Some(someVersionedContractInstance.copy(agreementText = ""))
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
    val randomContractId = ContractId.assertFromString(s"#random-${UUID.randomUUID}")
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
      inside(result) {
        case Some(time) => time should be <= Instant.now
      }
    }
  }

  it should "allow the retrieval of the maximum ledger time even when there are divulged contracts" in {
    val divulgedContractId = ContractId.assertFromString(s"#divulged-${UUID.randomUUID}")
    for {
      (_, _) <- store(
        divulgedContracts = Map(
          (divulgedContractId, someVersionedContractInstance) -> Set(charlie)
        ),
        blindingInfo = None,
        offsetAndTx = singleNonConsumingExercise(divulgedContractId)
      )
      (_, tx) <- store(singleCreate)
      contractIds = nonTransient(tx) + divulgedContractId
      result <- ledgerDao.lookupMaximumLedgerTime(contractIds)
    } yield {
      inside(result) {
        case Some(tx.ledgerEffectiveTime) => succeed
      }
    }
  }

  it should "allow the retrieval of the maximum ledger time even when there are only divulged contracts" in {
    val divulgedContractId = ContractId.assertFromString(s"#divulged-${UUID.randomUUID}")
    for {
      (_, _) <- store(
        divulgedContracts = Map(
          (divulgedContractId, someVersionedContractInstance) -> Set(charlie)
        ),
        blindingInfo = None,
        offsetAndTx = singleNonConsumingExercise(divulgedContractId)
      )
      result <- ledgerDao.lookupMaximumLedgerTime(Set(divulgedContractId))
    } yield {
      result shouldBe None
    }
  }

  it should "not allow the retrieval of the maximum ledger time of archived divulged contracts" in {
    val divulgedContractId = ContractId.assertFromString(s"#divulged-${UUID.randomUUID}")
    for {
      // This divulges and archives a contract in the same transaction
      (_, _) <- store(
        divulgedContracts = Map(
          (divulgedContractId, someVersionedContractInstance) -> Set(charlie)
        ),
        blindingInfo = None,
        offsetAndTx = singleExercise(divulgedContractId)
      )
      failure <- ledgerDao.lookupMaximumLedgerTime(Set(divulgedContractId)).failed
    } yield {
      failure shouldBe an[IllegalArgumentException]
      failure.getMessage should startWith(
        "One or more of the following contract identifiers has been found"
      )
    }
  }

  it should "store contracts with a transient contract in the global divulgence" in {
    store(fullyTransientWithChildren).flatMap(_ => succeed)
  }

}
