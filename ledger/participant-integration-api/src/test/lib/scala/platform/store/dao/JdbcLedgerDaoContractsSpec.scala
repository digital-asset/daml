// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.lf.value.Value.VersionedContractInstance
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

private[dao] trait JdbcLedgerDaoContractsSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def contractsReader = ledgerDao.contractsReader

  behavior of "JdbcLedgerDao (contracts)"

  it should "be able to persist and load contracts" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- contractsReader.lookupActiveContractAndLoadArgument(
        Set(alice),
        nonTransient(tx).loneElement,
      )
    } yield {
      // The agreement text is always empty when retrieved from the contract store
      result shouldEqual Some(someVersionedContractInstance.map(_.copy(agreementText = "")))
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
      result <- contractsReader.lookupActiveContractAndLoadArgument(Set(charlie), create)
    } yield {
      // The agreement text is always empty when retrieved from the contract store
      result shouldEqual Some(someVersionedContractInstance.map(_.copy(agreementText = "")))
    }
  }

  it should "not find contracts that are not visible to the requester" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- contractsReader.lookupActiveContractAndLoadArgument(
        Set(charlie),
        nonTransient(tx).loneElement,
      )
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
      result <- contractsReader.lookupActiveContractAndLoadArgument(Set(charlie, emma), contractId)
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
      result <- contractsReader.lookupActiveContractAndLoadArgument(Set(charlie, emma), contractId)
    } yield {
      result.value shouldBe a[VersionedContractInstance]
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
      result <- contractsReader.lookupActiveContractAndLoadArgument(Set(david, emma), contractId)
    } yield {
      result.value shouldBe a[VersionedContractInstance]
    }
  }

  it should "store contracts with a transient contract in the global divulgence" in {
    store(fullyTransientWithChildren).flatMap(_ => succeed)
  }
}
