// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.daml.lf.data.Ref.PackageVersion
import com.digitalasset.daml.lf.transaction.{
  GlobalKeyWithMaintainers,
  TransactionVersion,
  Versioned,
}
import com.digitalasset.daml.lf.value.Value.{ContractInstance, ValueText}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside, LoneElement, OptionValues}

import scala.concurrent.Future
import scala.math.Ordering.Implicits.infixOrderingOps

private[dao] trait JdbcLedgerDaoContractsSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def contractsReader = ledgerDao.contractsReader

  behavior of "JdbcLedgerDao (contracts)"

  it should "be able to persist and load contracts with the right visibility" in {
    for {
      (offset, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob),
        key = None,
      )
      result <- contractsReader.lookupContractState(
        nonTransient(tx).loneElement,
        offset,
      )
    } yield {
      result.collect { case active: LedgerDaoContractsReader.ActiveContract =>
        (active.contract, active.stakeholders, active.signatories)
      } shouldEqual Some((someVersionedContractInstance, Set(alice, bob), Set(alice, bob)))
    }
  }

  it should "be able to persist and load contracts with package-version populated dependent on transaction version" in {
    def testCreatePackageVersionLookup(
        testedTransactionVersion: TransactionVersion,
        expectedPackageVersion: Option[PackageVersion],
    ): Future[Assertion] = for {
      (offset, tx) <- store(
        singleCreate(
          createNode(
            _,
            Set(alice),
            Set(alice),
            // Package version is not set in the transaction version 31
            packageVersion = expectedPackageVersion,
            transactionVersion = testedTransactionVersion,
          )
        )
      )
      result <- contractsReader.lookupContractState(
        nonTransient(tx).loneElement,
        offset,
      )
    } yield {
      result.collect { case active: LedgerDaoContractsReader.ActiveContract =>
        (active.contract, active.stakeholders, active.signatories)
      } shouldEqual Some(
        (
          Versioned(
            testedTransactionVersion,
            ContractInstance(
              packageName = somePackageName,
              packageVersion = expectedPackageVersion,
              template = someTemplateId,
              arg = someContractArgument,
            ),
          ),
          Set(alice),
          Set(alice),
        )
      )
    }

    for {
      // TODO(#19494): Remove once TransactionVersion V31 becomes minimum supoported
      _ <- testCreatePackageVersionLookup(
        testedTransactionVersion = TransactionVersion.minVersion,
        expectedPackageVersion =
          // Package version is set only for contracts created in transaction version > 31 (language version > 2.1)
          Option.when(TransactionVersion.minVersion > TransactionVersion.V31)(somePackageVersion),
      )
      _ <- testCreatePackageVersionLookup(
        // TODO(#19494): Replace with minVersion
        testedTransactionVersion = TransactionVersion.maxVersion,
        expectedPackageVersion = Some(somePackageVersion),
      )
    } yield succeed
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
        case LedgerDaoContractsReader.ActiveContract(contract, stakeholders, _, _, _, _, _) =>
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
      somePackageName,
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
