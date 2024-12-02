// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  packageId,
}
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  ExampleTransactionFactory,
  SerializableContract,
}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, LfPartyId, RequestCounter}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.QualifiedName
import org.scalatest.wordspec.AsyncWordSpec

trait ContractStoreTest { this: AsyncWordSpec & BaseTest =>

  protected val alice: LfPartyId = LfPartyId.assertFromString("alice")
  protected val bob: LfPartyId = LfPartyId.assertFromString("bob")
  protected val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
  protected val david: LfPartyId = LfPartyId.assertFromString("david")

  def contractStore(mk: () => ContractStore): Unit = {
    val contractId = ExampleTransactionFactory.suffixedId(0, 0)
    val contractId2 = ExampleTransactionFactory.suffixedId(2, 0)
    val contractId3 = ExampleTransactionFactory.suffixedId(3, 0)
    val contractId4 = ExampleTransactionFactory.suffixedId(4, 0)
    val contractId5 = ExampleTransactionFactory.suffixedId(5, 0)
    val contract = asSerializable(contractId, contractInstance = contractInstance())
    val rc = RequestCounter(0)
    val storedContract = StoredContract(contract, rc)

    val rc2 = RequestCounter(1)
    val let2 = CantonTimestamp.Epoch.plusSeconds(5)
    val pkgId2 = Ref.PackageId.assertFromString("different_id")
    val contract2 = asSerializable(
      contractId2,
      contractInstance = contractInstance(
        templateId = Ref.Identifier(pkgId2, QualifiedName.assertFromString("module:template"))
      ),
      ledgerTime = let2,
    )
    val templateName3 = QualifiedName.assertFromString("Foo:Bar")
    val templateId3 = Ref.Identifier(packageId, templateName3)
    val contract3 =
      asSerializable(
        contractId3,
        contractInstance = contractInstance(templateId = templateId3),
        ledgerTime = let2,
      )
    val contract4 =
      asSerializable(
        contractId4,
        contractInstance = contractInstance(
          templateId = Ref.Identifier(pkgId2, templateName3)
        ),
      )

    val contract5 =
      asSerializable(
        contractId5,
        contractInstance = contractInstance(
          templateId = Ref.Identifier(pkgId2, templateName3)
        ),
      )

    "store and retrieve a created contract" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(rc, contract)
        c <- store.lookupE(contractId)
        inst <- store.lookupContractE(contractId)
      } yield {
        c shouldEqual storedContract
        inst shouldEqual contract
      }
    }

    "update a created contract with instance size > 32kB" in {
      val store = mk()

      val manySignatories = 1
        .to(1000)
        .map { x =>
          LfPartyId.assertFromString(s"alicealicealicealicealicealice::$x")
        }
        .toSet
      val metadata = ContractMetadata.tryCreate(Set.empty, manySignatories, None)
      metadata.toByteArray(testedProtocolVersion).length should be > 32768
      val largeContract: SerializableContract =
        asSerializable(
          contractId,
          contractInstance = contractInstance(),
          metadata = metadata,
        )

      val storedLargeContractUpdated =
        StoredContract(largeContract, rc2)

      for {
        _ <- store.storeCreatedContract(rc, largeContract)
        _ <- store.storeCreatedContract(rc2, largeContract)
        c <- store.lookupE(contractId)
        inst <- store.lookupContractE(contractId)
      } yield {
        c shouldEqual storedLargeContractUpdated
        inst shouldEqual largeContract
      }
    }

    "store the same contract twice for the same id" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContracts(Seq((contract, rc), (contract, rc)))
        _ <- store.storeCreatedContracts(Seq((contract, rc)))
      } yield succeed
    }

    "take the second created contract" in {
      val store = mk()
      for {
        _ <- store.storeCreatedContract(rc, contract)
        _ <- store.storeCreatedContract(rc2, contract)
        c <- store.lookupE(contract.contractId)
      } yield c shouldBe StoredContract(contract, rc2)
    }

    "succeed when storing a different contract for an existing id" must {
      val storedContract2 = StoredContract(contract, rc2)
      "for created contracts" in {
        val store = mk()

        for {
          _ <- store.storeCreatedContract(rc, contract)
          _ <- store.storeCreatedContract(rc2, contract)
          c <- store.lookupE(contract.contractId)
        } yield c shouldBe storedContract2
      }

      "for created contracts reversed" in {
        val store = mk()

        for {
          _ <- store.storeCreatedContract(rc2, contract)
          _ <- store.storeCreatedContract(rc, contract)
          c <- store.lookupE(contract.contractId)
        } yield c shouldBe storedContract2
      }
    }

    "fail when looking up nonexistent contract" in {
      val store = mk()
      for {
        c <- store.lookup(contractId).value
      } yield c shouldEqual None
    }

    "delete a set of contracts as done by pruning" in {
      val store = mk()
      for {
        _ <- List(contract, contract2, contract4, contract5)
          .parTraverse(store.storeCreatedContract(rc, _))
        _ <- store.deleteIgnoringUnknown(Seq(contractId, contractId2, contractId3, contractId4))
        notFounds <- List(contractId, contractId2, contractId3, contractId4).parTraverse(
          store.lookupE(_).value
        )
        notDeleted <- store.lookupE(contractId5).value
      } yield {
        notFounds shouldEqual List(
          Left(UnknownContract(contractId)),
          Left(UnknownContract(contractId2)),
          Left(UnknownContract(contractId3)),
          Left(UnknownContract(contractId4)),
        )
        notDeleted.map(_.contract) shouldEqual Right(contract5)
      }
    }

    "purge contract store deletes all contracts" in {
      val store = mk()
      for {
        _ <- store.storeCreatedContract(rc, contract)
        _ <- store.storeCreatedContract(rc2, contract2)
        _ <- store.storeCreatedContract(rc2, contract3)
        contractsBeforePurge <- store
          .find(
            filterId = None,
            filterPackage = None,
            filterTemplate = None,
            limit = 5,
          )
          .failOnShutdown
        _ <- store.purge()
        contractsAfterPurge <- store
          .find(
            filterId = None,
            filterPackage = None,
            filterTemplate = None,
            limit = 5,
          )
          .failOnShutdown
      } yield {
        contractsBeforePurge.toSet shouldEqual Set(contract, contract2, contract3)
        contractsAfterPurge shouldBe empty
      }
    }

    "find contracts by filters" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(rc, contract)
        _ <- store.storeCreatedContract(rc, contract2)
        _ <- store.storeCreatedContract(rc, contract3)
        _ <- store.storeCreatedContract(rc, contract4)
        _ <- store.storeCreatedContract(
          rc,
          contract2.copy(
            contractId = contractId5,
            ledgerCreateTime = LedgerCreateTime(CantonTimestamp.Epoch),
          ),
        )

        resId <- store.find(filterId = Some(contractId.coid), None, None, 100).failOnShutdown
        resPkg <- store
          .find(filterId = None, filterPackage = Some(pkgId2), None, 100)
          .failOnShutdown
        resPkgLimit <- store
          .find(filterId = None, filterPackage = Some(pkgId2), None, 2)
          .failOnShutdown
        resTemplatePkg <- store
          .find(
            filterId = None,
            filterPackage = Some(contract4.contractInstance.unversioned.template.packageId),
            filterTemplate =
              Some(contract4.contractInstance.unversioned.template.qualifiedName.toString()),
            100,
          )
          .failOnShutdown
        resTemplate <- store.find(None, None, Some(templateName3.toString), 100).failOnShutdown
      } yield {
        resId shouldEqual List(contract)
        resTemplatePkg shouldEqual List(contract4)
        resPkg should have size 3
        resPkgLimit should have size 2
        resTemplate.toSet shouldEqual Set(contract4, contract3)
      }
    }

    "store contract and use contract lookups" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(rc, contract)
        _ <- store.storeCreatedContract(rc, contract2)
        c1 <- store.lookup(contractId).value
        c1inst <- store.lookupContract(contractId).value
        c3 <- store.lookup(contractId3).value
      } yield {
        c1 shouldEqual Some(storedContract)
        c1inst shouldEqual Some(contract)
        c3 shouldEqual None
      }

    }

    "lookup stakeholders when passed existing contract IDs" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(rc, contract)
        _ <- store.storeCreatedContract(rc, contract2)
        _ <- store.storeCreatedContract(rc, contract3)
        _ <- store.storeCreatedContract(rc, contract4)
        res <- store.lookupStakeholders(Set(contractId, contractId2, contractId4))
      } yield {
        res shouldBe Map(
          contractId -> contract.metadata.stakeholders,
          contractId2 -> contract2.metadata.stakeholders,
          contractId4 -> contract4.metadata.stakeholders,
        )
      }
    }

    "lookup stakeholders when passed no contract IDs" in {
      val store = mk()

      for {
        res <- store.lookupStakeholders(Set()).value
      } yield {
        res shouldBe Right(Map.empty)
      }
    }

    "fail stakeholder lookup when passed a non-existant contract IDs" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(rc, contract)
        _ <- store.storeCreatedContract(rc, contract3)
        _ <- store.storeCreatedContract(rc, contract4)
        res <- store.lookupStakeholders(Set(contractId, contractId2, contractId4)).value
      } yield {
        res shouldBe Left(UnknownContracts(Set(contractId2)))
      }
    }
  }
}
