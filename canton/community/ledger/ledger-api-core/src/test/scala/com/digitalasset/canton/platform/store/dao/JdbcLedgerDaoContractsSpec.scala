// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.lf.transaction.{GlobalKeyWithMaintainers, Util}
import com.daml.lf.value.Value.{ValueText, VersionedContractInstance}
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
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
      result shouldEqual Some(someVersionedContractInstance)
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
      result shouldEqual Some(someVersionedContractInstance)
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

  it should "present the contract state at a specific event sequential id" in {
    for {
      (_, tx) <- store(singleCreate(create(_, signatories = Set(alice))))
      contractId = nonTransient(tx).loneElement
      _ <- store(singleNonConsumingExercise(contractId))
      ledgerEndAtCreate <- ledgerDao.lookupLedgerEnd()
      _ <- store(txArchiveContract(alice, (contractId, None)))
      ledgerEndAfterArchive <- ledgerDao.lookupLedgerEnd()
      queryAfterCreate <- contractsReader.lookupContractState(
        contractId,
        ledgerEndAtCreate.lastOffset,
      )
      queryAfterArchive <- contractsReader.lookupContractState(
        contractId,
        ledgerEndAfterArchive.lastOffset,
      )
    } yield {
      queryAfterCreate.value match {
        case LedgerDaoContractsReader.ActiveContract(contract, stakeholders, _, _, _, _, _, _) =>
          contract shouldBe someVersionedContractInstance
          stakeholders should contain theSameElementsAs Set(alice)
        case LedgerDaoContractsReader.ArchivedContract(_) =>
          fail("Contract should appear as active")
      }
      queryAfterArchive.value match {
        case _: LedgerDaoContractsReader.ActiveContract =>
          fail("Contract should appear as archived")
        case LedgerDaoContractsReader.ArchivedContract(stakeholders) =>
          stakeholders should contain theSameElementsAs Set(alice)
      }
    }
  }

  it should "present the contract key state at a specific event sequential id" in {
    val aTextValue = ValueText(scala.util.Random.nextString(10))

    val key = GlobalKeyWithMaintainers.assertBuild(
      someTemplateId,
      aTextValue,
      Set(alice, bob),
      Util.sharedKey(testLanguageVersion),
    )

    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob),
        key = Some(key),
      )
      contractId = nonTransient(tx).loneElement
      _ <- store(singleNonConsumingExercise(contractId))
      ledgerEndAtCreate <- ledgerDao.lookupLedgerEnd()
      _ <- store(txArchiveContract(alice, (contractId, None)))
      ledgerEndAfterArchive <- ledgerDao.lookupLedgerEnd()
      queryAfterCreate <- contractsReader.lookupKeyState(
        key.globalKey,
        ledgerEndAtCreate.lastOffset,
      )
      queryAfterArchive <- contractsReader.lookupKeyState(
        key.globalKey,
        ledgerEndAfterArchive.lastOffset,
      )
    } yield {
      queryAfterCreate match {
        case LedgerDaoContractsReader.KeyAssigned(fetchedContractId, stakeholders) =>
          fetchedContractId shouldBe contractId
          stakeholders shouldBe Set(alice, bob)
        case _ => fail("Key should be assigned")
      }
      queryAfterArchive shouldBe LedgerDaoContractsReader.KeyUnassigned
    }
  }
}
