// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.Node.KeyWithMaintainers
import com.daml.lf.value.Value.ValueText
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{LoneElement, OptionValues}

// These tests use lookups of the contract state at a specific event sequential ID, an operation that
// is not supported by the old mutating schema.
// TODO append-only: Merge this class with JdbcLedgerDaoContractsSpec
private[dao] trait JdbcLedgerDaoContractsAppendOnlySpec extends LoneElement with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def contractsReader = ledgerDao.contractsReader

  behavior of "JdbcLedgerDao (contracts) on append-only schema"

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
        ledgerEndAtCreate.lastEventSeqId,
      )
      queryAfterArchive <- contractsReader.lookupContractState(
        contractId,
        ledgerEndAfterArchive.lastEventSeqId,
      )
    } yield {
      queryAfterCreate.value match {
        case LedgerDaoContractsReader.ActiveContract(contract, stakeholders, _) =>
          contract shouldBe someVersionedContractInstance.map(_.copy(agreementText = ""))
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

    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob),
        key = Some(KeyWithMaintainers(aTextValue, Set(alice, bob))),
      )
      key = GlobalKey.assertBuild(someTemplateId, aTextValue)
      contractId = nonTransient(tx).loneElement
      _ <- store(singleNonConsumingExercise(contractId))
      ledgerEndAtCreate <- ledgerDao.lookupLedgerEnd()
      _ <- store(txArchiveContract(alice, (contractId, None)))
      ledgerEndAfterArchive <- ledgerDao.lookupLedgerEnd()
      queryAfterCreate <- contractsReader.lookupKeyState(key, ledgerEndAtCreate.lastEventSeqId)
      queryAfterArchive <- contractsReader.lookupKeyState(key, ledgerEndAfterArchive.lastEventSeqId)
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
