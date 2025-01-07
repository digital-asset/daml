// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import cats.syntax.parallel.*
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.daml.lf.data.Ref.PackageVersion
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, Versioned}
import com.digitalasset.daml.lf.value.Value.{ContractId, ContractInstance, ValueText}
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
        testedTransactionVersion: LanguageVersion,
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
        testedTransactionVersion = LanguageVersion.AllV2.min,
        expectedPackageVersion =
          // Package version is set only for contracts created in LF > 2.1
          Option.when(LanguageVersion.AllV2.min > LanguageVersion.v2_dev)(somePackageVersion),
      )
      _ <- testCreatePackageVersionLookup(
        // TODO(#19494): Replace with minVersion
        testedTransactionVersion = LanguageVersion.v2_dev,
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
      Some(ledgerEndAtCreate) <- ledgerDao.lookupLedgerEnd()
      _ <- store(txArchiveContract(alice, (contractId, None)))
      Some(ledgerEndAfterArchive) <- ledgerDao.lookupLedgerEnd()
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
        ledgerEndAtCreate.value.lastOffset,
      )
      queryAfterArchive <- contractsReader.lookupKeyState(
        key.globalKey,
        ledgerEndAfterArchive.value.lastOffset,
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

  it should "support batch reading contract keys at the right offset" in {

    def genContractWithKey(string: String = scala.util.Random.nextString(5)) = {
      val aTextValue = ValueText(string)
      val key = GlobalKeyWithMaintainers.assertBuild(
        someTemplateId,
        aTextValue,
        Set(alice),
        somePackageName,
      )
      createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob),
        key = Some(key),
      ).map { case (offset, entry) =>
        val contractId = nonTransient(entry).loneElement
        (aTextValue, key, contractId, offset)
      }
    }

    def fetchAll(
        keys: Seq[GlobalKey],
        offset: Offset,
    ): Future[(Map[GlobalKey, KeyState], Map[GlobalKey, KeyState])] = {
      val oneByOneF = keys
        .parTraverse { key =>
          contractsReader.lookupKeyState(key, offset).map(state => key -> state)
        }
        .map(_.toMap)
      val togetherF = contractsReader.lookupKeyStatesFromDb(keys, offset)
      for {
        oneByOne <- oneByOneF
        together <- togetherF
      } yield (oneByOne, together)
    }

    def verifyMatch(
        results: (Map[GlobalKey, KeyState], Map[GlobalKey, KeyState]),
        expected: Map[GlobalKeyWithMaintainers, Option[ContractId]],
    ) = {
      val (oneByOne, together) = results
      oneByOne shouldBe together
      oneByOne.map {
        case (k, KeyAssigned(cid, _)) => (k, Some(cid))
        case (k, KeyUnassigned) => (k, None)
      } shouldBe expected.map { case (k, v) => (k.globalKey, v) }
    }

    for {
      // have AA at offsetA
      (textA, keyA, cidA, offsetA) <- genContractWithKey()
      _ <- store(singleNonConsumingExercise(cidA))
      // have AA,BB at offsetB
      (_, keyB, cidB, offsetB) <- genContractWithKey()
      // have BB at offsetPreC
      (offsetPreC, _) <- store(txArchiveContract(alice, (cidA, None)))
      // have BB, CC at offsetC
      (_, keyC, cidC, offsetC) <- genContractWithKey()
      // have AA, BB, CC at offsetA2
      (_, keyA2, cidA2, offsetA2) <- genContractWithKey(textA.value)
      // have AA, BB at offset D
      (offsetD, _) <- store(txArchiveContract(alice, (cidC, None)))
      // have AA at offsetE
      (offsetE, _) <- store(txArchiveContract(alice, (cidB, None)))
      allKeys = Seq(keyA, keyB, keyC).map(_.globalKey)
      atOffsetA <- fetchAll(allKeys, offsetA)
      atOffsetB <- fetchAll(allKeys, offsetB)
      atOffsetPreC <- fetchAll(allKeys, offsetPreC)
      atOffsetC <- fetchAll(allKeys, offsetC)
      atOffsetA2 <- fetchAll(allKeys, offsetA2)
      atOffsetD <- fetchAll(allKeys, offsetD)
      atOffsetE <- fetchAll(allKeys, offsetE)
    } yield {
      keyA shouldBe keyA2
      verifyMatch(atOffsetA, Map(keyA -> Some(cidA), keyB -> None, keyC -> None))
      verifyMatch(atOffsetB, Map(keyA -> Some(cidA), keyB -> Some(cidB), keyC -> None))
      verifyMatch(atOffsetPreC, Map(keyA -> None, keyB -> Some(cidB), keyC -> None))
      verifyMatch(atOffsetC, Map(keyA -> None, keyB -> Some(cidB), keyC -> Some(cidC)))
      verifyMatch(atOffsetA2, Map(keyA -> Some(cidA2), keyB -> Some(cidB), keyC -> Some(cidC)))
      verifyMatch(atOffsetD, Map(keyA -> Some(cidA2), keyB -> Some(cidB), keyC -> None))
      verifyMatch(atOffsetE, Map(keyA -> Some(cidA2), keyB -> None, keyC -> None))
    }
  }

}
