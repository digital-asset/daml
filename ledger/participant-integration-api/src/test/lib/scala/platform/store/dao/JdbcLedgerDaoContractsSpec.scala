// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.Node.KeyWithMaintainers
import com.daml.lf.value.Value.{ContractId, ContractInst, ValueText}
import com.daml.platform.apiserver.execution.MissingContracts
import org.scalatest.{Inside, LoneElement, OptionValues}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[dao] trait JdbcLedgerDaoContractsSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (contracts)"

  it should "be able to persist and load contracts" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.lookupActiveOrDivulgedContract(nonTransient(tx).loneElement, Set(alice))
    } yield {
      // The agreement text is always empty when retrieved from the contract store
      result shouldEqual Some(someVersionedContractInstance.copy(agreementText = ""))
    }
  }

  it should "allow to divulge a contract that has already been committed" in {
    for {
      (_, tx) <- store(singleCreate)
      create = nonTransient(tx).loneElement
      _ <- storeCommitedContractDivulgence(
        id = create,
        divulgees = Set(charlie),
      )
      result <- ledgerDao.lookupActiveOrDivulgedContract(create, Set(charlie))
    } yield {
      // The agreement text is always empty when retrieved from the contract store
      result shouldEqual Some(someVersionedContractInstance.copy(agreementText = ""))
    }
  }

  it should "not find contracts that are not visible to the requester" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.lookupActiveOrDivulgedContract(nonTransient(tx).loneElement, Set(charlie))
    } yield {
      result shouldEqual None
    }
  }

  it should "not find contracts that are not visible to any of the requesters" in {
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob),
        key = None,
      )
      contractId = nonTransient(tx).loneElement
      result <- ledgerDao.lookupActiveOrDivulgedContract(contractId, Set(charlie, emma))
    } yield {
      result shouldBe None
    }
  }

  it should "find contract if at least one of requesters is a stakeholder" in {
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob, charlie),
        key = None,
      )
      contractId = nonTransient(tx).loneElement
      result <- ledgerDao.lookupActiveOrDivulgedContract(contractId, Set(charlie, emma))
    } yield {
      result.value shouldBe a[ContractInst[_]]
    }
  }

  it should "find contract if at least one of requesters is a divulgee" in {
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob, charlie),
        key = None,
      )
      contractId = nonTransient(tx).loneElement
      _ <- storeCommitedContractDivulgence(
        id = contractId,
        divulgees = Set(emma),
      )
      result <- ledgerDao.lookupActiveOrDivulgedContract(contractId, Set(david, emma))
    } yield {
      result.value shouldBe a[ContractInst[_]]
    }
  }

  it should "not find keys if none of requesters are stakeholders" in {
    val aTextValue = ValueText(scala.util.Random.nextString(10))
    for {
      (_, _) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob),
        key = Some(KeyWithMaintainers(aTextValue, Set(alice, bob))),
      )
      key = GlobalKey.assertBuild(someTemplateId, aTextValue)
      result <- ledgerDao.lookupKey(key, Set(charlie, emma))
    } yield {
      result shouldBe None
    }
  }

  it should "find a key if at least one of requesters is a stakeholder" in {
    val aTextValue = ValueText(scala.util.Random.nextString(10))
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob, charlie),
        key = Some(KeyWithMaintainers(aTextValue, Set(alice, bob))),
      )
      contractId = nonTransient(tx).loneElement
      key = GlobalKey.assertBuild(someTemplateId, aTextValue)
      result <- ledgerDao.lookupKey(key, Set(emma, charlie))
    } yield {
      result.value shouldBe contractId
    }
  }

  it should "not find a key if the requesters are only divulgees" in {
    val aTextValue = ValueText(scala.util.Random.nextString(10))
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob, charlie),
        key = Some(KeyWithMaintainers(aTextValue, Set(alice, bob))),
      )
      _ <- storeCommitedContractDivulgence(
        id = nonTransient(tx).loneElement,
        divulgees = Set(david, emma),
      )
      key = GlobalKey.assertBuild(someTemplateId, aTextValue)
      result <- ledgerDao.lookupKey(key, Set(david, emma))
    } yield {
      result shouldBe None
    }
  }

  it should "prevent retrieving the maximum ledger time if some contracts are not found" in {
    val randomContractId = ContractId.assertFromString(s"#random-${UUID.randomUUID}")
    for {
      failure <- ledgerDao.lookupMaximumLedgerTime(Set(randomContractId)).failed
    } yield {
      failure shouldBe an[MissingContracts]
    }
  }

  it should "allow the retrieval of the maximum ledger time" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.lookupMaximumLedgerTime(nonTransient(tx))
    } yield {
      inside(result) { case Some(time) =>
        time should be <= Instant.now
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
        offsetAndTx = singleNonConsumingExercise(divulgedContractId),
      )
      (_, tx) <- store(singleCreate)
      contractIds = nonTransient(tx) + divulgedContractId
      result <- ledgerDao.lookupMaximumLedgerTime(contractIds)
    } yield {
      inside(result) { case Some(tx.ledgerEffectiveTime) =>
        succeed
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
        offsetAndTx = singleNonConsumingExercise(divulgedContractId),
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
        offsetAndTx = singleExercise(divulgedContractId),
      )
      failure <- ledgerDao.lookupMaximumLedgerTime(Set(divulgedContractId)).failed
    } yield {
      failure shouldBe an[MissingContracts]
    }
  }

  it should "store contracts with a transient contract in the global divulgence" in {
    store(fullyTransientWithChildren).flatMap(_ => succeed)
  }

}
