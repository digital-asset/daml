// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import cats.syntax.parallel.*
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.{Active, Archived}
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers}
import com.digitalasset.daml.lf.value.Value.{ContractId, ValueText}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

import scala.concurrent.Future

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
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      result <- contractsReader.lookupContractState(
        nonTransient(tx).loneElement,
        ledgerEnd.map(_.lastEventSeqId).getOrElse(0L),
      )
    } yield {
      result shouldBe Some(Active)
    }
  }

  it should "store contracts with a transient contract in the global divulgence and do not fetch it" in {
    for {
      (_, tx) <- store(fullyTransientWithChildren, contractActivenessChanged = false)
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      contractId1 = created(tx).head
      contractId2 = created(tx).tail.loneElement
      result1 <- contractsReader.lookupContractState(
        contractId1,
        ledgerEnd.map(_.lastEventSeqId).getOrElse(0L),
      )
      result2 <- contractsReader.lookupContractState(
        contractId2,
        ledgerEnd.map(_.lastEventSeqId).getOrElse(0L),
      )
    } yield {
      result1 shouldBe empty
      result2 shouldBe empty
    }
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
        ledgerEndAtCreate.lastEventSeqId,
      )
      queryAfterArchive <- contractsReader.lookupContractState(
        contractId,
        ledgerEndAfterArchive.lastEventSeqId,
      )
    } yield {
      queryAfterCreate.value shouldBe Active
      queryAfterArchive.value shouldBe Archived
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
        ledgerEndAtCreate.value.lastEventSeqId,
      )
      queryAfterArchive <- contractsReader.lookupKeyState(
        key.globalKey,
        ledgerEndAfterArchive.value.lastEventSeqId,
      )
    } yield {
      queryAfterCreate match {
        case LedgerDaoContractsReader.KeyAssigned(fetchedContractId) =>
          fetchedContractId shouldBe contractId
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
        eventSeqId: Long,
    ): Future[(Map[GlobalKey, KeyState], Map[GlobalKey, KeyState])] = {
      val oneByOneF = keys
        .parTraverse { key =>
          contractsReader.lookupKeyState(key, eventSeqId).map(state => key -> state)
        }
        .map(_.toMap)
      val togetherF = contractsReader.lookupKeyStatesFromDb(keys, eventSeqId)
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
        case (k, KeyAssigned(cid)) => (k, Some(cid))
        case (k, KeyUnassigned) => (k, None)
      } shouldBe expected.map { case (k, v) => (k.globalKey, v) }
    }

    for {
      // have AA at offsetA
      (textA, keyA, cidA, _) <- genContractWithKey()
      eventSeqIdA <- ledgerDao.lookupLedgerEnd().map(_.value.lastEventSeqId)
      _ <- store(singleNonConsumingExercise(cidA))
      // have AA,BB at offsetB
      (_, keyB, cidB, _) <- genContractWithKey()
      eventSeqIdB <- ledgerDao.lookupLedgerEnd().map(_.value.lastEventSeqId)
      // have BB at offsetPreC
      (_, _) <- store(txArchiveContract(alice, (cidA, None)))
      eventSeqIdPreC <- ledgerDao.lookupLedgerEnd().map(_.value.lastEventSeqId)
      // have BB, CC at offsetC
      (_, keyC, cidC, _) <- genContractWithKey()
      eventSeqIdC <- ledgerDao.lookupLedgerEnd().map(_.value.lastEventSeqId)
      // have AA, BB, CC at offsetA2
      (_, keyA2, cidA2, _) <- genContractWithKey(textA.value)
      eventSeqIdA2 <- ledgerDao.lookupLedgerEnd().map(_.value.lastEventSeqId)
      // have AA, BB at offset D
      (_, _) <- store(txArchiveContract(alice, (cidC, None)))
      eventSeqIdD <- ledgerDao.lookupLedgerEnd().map(_.value.lastEventSeqId)
      // have AA at offsetE
      (_, _) <- store(txArchiveContract(alice, (cidB, None)))
      eventSeqIdE <- ledgerDao.lookupLedgerEnd().map(_.value.lastEventSeqId)
      allKeys = Seq(keyA, keyB, keyC).map(_.globalKey)
      atEventSeqIdA <- fetchAll(allKeys, eventSeqIdA)
      atEventSeqIdB <- fetchAll(allKeys, eventSeqIdB)
      atEventSeqIdPreC <- fetchAll(allKeys, eventSeqIdPreC)
      atEventSeqIdC <- fetchAll(allKeys, eventSeqIdC)
      atEventSeqIdA2 <- fetchAll(allKeys, eventSeqIdA2)
      atEventSeqIdD <- fetchAll(allKeys, eventSeqIdD)
      atEventSeqIdE <- fetchAll(allKeys, eventSeqIdE)
    } yield {
      keyA shouldBe keyA2
      verifyMatch(atEventSeqIdA, Map(keyA -> Some(cidA), keyB -> None, keyC -> None))
      verifyMatch(atEventSeqIdB, Map(keyA -> Some(cidA), keyB -> Some(cidB), keyC -> None))
      verifyMatch(atEventSeqIdPreC, Map(keyA -> None, keyB -> Some(cidB), keyC -> None))
      verifyMatch(atEventSeqIdC, Map(keyA -> None, keyB -> Some(cidB), keyC -> Some(cidC)))
      verifyMatch(atEventSeqIdA2, Map(keyA -> Some(cidA2), keyB -> Some(cidB), keyC -> Some(cidC)))
      verifyMatch(atEventSeqIdD, Map(keyA -> Some(cidA2), keyB -> Some(cidB), keyC -> None))
      verifyMatch(atEventSeqIdE, Map(keyA -> Some(cidA2), keyB -> None, keyC -> None))
    }
  }

}
