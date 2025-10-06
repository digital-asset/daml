// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.ExampleTransactionFactory.packageId
import com.digitalasset.canton.protocol.{ExampleContractFactory, GenContractInstance, LfContractId}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, LfPartyId, LfTimestamp}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{IdString, PackageId, QualifiedName}
import com.digitalasset.daml.lf.transaction.CreationTime
import org.scalatest.wordspec.AsyncWordSpec

trait ContractStoreTest extends FailOnShutdown { this: AsyncWordSpec & BaseTest =>

  protected val alice: LfPartyId = LfPartyId.assertFromString("alice")
  protected val bob: LfPartyId = LfPartyId.assertFromString("bob")
  protected val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
  protected val david: LfPartyId = LfPartyId.assertFromString("david")

  protected val contract: GenContractInstance { type InstCreatedAtTime <: CreationTime.CreatedAt } =
    ExampleContractFactory.build()
  protected val contractId: LfContractId = contract.contractId

  protected val let2: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(5)
  protected val pkgId2: IdString.PackageId = Ref.PackageId.assertFromString("different_id")
  protected val contract2
      : GenContractInstance { type InstCreatedAtTime <: CreationTime.CreatedAt } =
    ExampleContractFactory.build(
      templateId = Ref.Identifier(pkgId2, QualifiedName.assertFromString("module:template")),
      createdAt = CreationTime.CreatedAt(let2.toLf),
    )
  protected val contractId2: LfContractId = contract2.contractId

  protected val templateName3: QualifiedName = QualifiedName.assertFromString("Foo:Bar")
  protected val templateId3: Ref.FullReference[PackageId] = Ref.Identifier(packageId, templateName3)
  protected val contract3
      : GenContractInstance { type InstCreatedAtTime <: CreationTime.CreatedAt } =
    ExampleContractFactory.build(
      templateId = templateId3,
      createdAt = CreationTime.CreatedAt(let2.toLf),
    )
  protected val contractId3: LfContractId = contract3.contractId

  protected val contract4
      : GenContractInstance { type InstCreatedAtTime <: CreationTime.CreatedAt } =
    ExampleContractFactory.build(templateId = Ref.Identifier(pkgId2, templateName3))
  protected val contractId4: LfContractId = contract4.contractId

  protected val contract5
      : GenContractInstance { type InstCreatedAtTime <: CreationTime.CreatedAt } =
    ExampleContractFactory.build(templateId = Ref.Identifier(pkgId2, templateName3))
  protected val contractId5: LfContractId = contract5.contractId

  def contractStore(mk: () => ContractStore): Unit = {

    "store and retrieve a created contract" in {
      val store = mk()

      for {
        _ <- store.storeContract(contract).failOnShutdown
        c <- store.lookupE(contractId)
      } yield c shouldEqual contract
    }

    "update a created contract with instance size > 32kB" in {
      val store = mk()

      val manySignatories = 1
        .to(1000)
        .map { x =>
          LfPartyId.assertFromString(s"alicealicealicealicealicealice::$x")
        }
        .toSet

      val largeContract =
        ExampleContractFactory.build(
          signatories = manySignatories,
          stakeholders = manySignatories,
        )

      largeContract.encoded.size() should be > 32768

      for {
        _ <- store.storeContract(largeContract).failOnShutdown
        _ <- store.storeContract(largeContract).failOnShutdown
        c <- store.lookupE(largeContract.contractId)
      } yield c shouldEqual largeContract
    }

    "store the same contract twice for the same id" in {
      val store = mk()

      for {
        _ <- store.storeContracts(Seq(contract, contract))
        _ <- store.storeContracts(Seq(contract))
      } yield succeed
    }

    "succeed when storing a different contract for an existing id" must {
      val storedContract2 = contract
      "for created contracts" in {
        val store = mk()

        for {
          _ <- store.storeContract(contract).failOnShutdown
          _ <- store.storeContract(contract).failOnShutdown
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
          .parTraverse(store.storeContract)
          .failOnShutdown
        _ <- store
          .deleteIgnoringUnknown(Seq(contractId, contractId2, contractId3, contractId4))
          .failOnShutdown
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
        notDeleted shouldEqual Right(contract5)
      }
    }

    "purge contract store deletes all contracts" in {
      val store = mk()
      for {
        _ <- store.storeContract(contract).failOnShutdown
        _ <- store.storeContract(contract2).failOnShutdown
        _ <- store.storeContract(contract3).failOnShutdown
        contractsBeforePurge <- store
          .find(
            exactId = None,
            filterPackage = None,
            filterTemplate = None,
            limit = 5,
          )
          .failOnShutdown
        _ <- store.purge().failOnShutdown
        contractsAfterPurge <- store
          .find(
            exactId = None,
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

      val contract2b = ExampleContractFactory.build(
        templateId = Ref.Identifier(pkgId2, QualifiedName.assertFromString("module:template")),
        createdAt = CreationTime.CreatedAt(LfTimestamp.Epoch),
      )

      for {
        _ <- store.storeContract(contract).failOnShutdown
        _ <- store.storeContract(contract2).failOnShutdown
        _ <- store.storeContract(contract3).failOnShutdown
        _ <- store.storeContract(contract4).failOnShutdown
        _ <- store.storeContract(contract2b).failOnShutdown

        resId <- store.find(exactId = Some(contractId.coid), None, None, 100)
        resPkg <- store
          .find(exactId = None, filterPackage = Some(pkgId2), None, 100)
        resPkgLimit <- store
          .find(exactId = None, filterPackage = Some(pkgId2), None, 2)
        resTemplatePkg <- store
          .find(
            exactId = None,
            filterPackage = Some(contract4.templateId.packageId),
            filterTemplate = Some(contract4.templateId.qualifiedName.toString()),
            100,
          )
        resTemplate <- store.find(None, None, Some(templateName3.toString), 100)
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
        _ <- store.storeContract(contract).failOnShutdown
        _ <- store.storeContract(contract2).failOnShutdown
        c1 <- store.lookup(contractId).value
        c1inst <- store.lookup(contractId).value
        c3 <- store.lookup(contractId3).value
      } yield {
        c1 shouldEqual Some(contract)
        c1inst shouldEqual Some(contract)
        c3 shouldEqual None
      }

    }

    "lookup stakeholders when passed existing contract IDs" in {
      val store = mk()

      for {
        _ <- store.storeContract(contract).failOnShutdown
        _ <- store.storeContract(contract2).failOnShutdown
        _ <- store.storeContract(contract3).failOnShutdown
        _ <- store.storeContract(contract4).failOnShutdown
        res <- store.lookupStakeholders(Set(contractId, contractId2, contractId4)).failOnShutdown
      } yield {
        res shouldBe Map(
          contractId -> contract.stakeholders,
          contractId2 -> contract2.stakeholders,
          contractId4 -> contract4.stakeholders,
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

    "fail stakeholder lookup when passed a non-existent contract IDs" in {
      val store = mk()

      for {
        _ <- store.storeContract(contract).failOnShutdown
        _ <- store.storeContract(contract3).failOnShutdown
        _ <- store.storeContract(contract4).failOnShutdown
        res <- store.lookupStakeholders(Set(contractId, contractId2, contractId4)).value
      } yield {
        res shouldBe Left(UnknownContracts(Set(contractId2)))
      }
    }

    "store contracts and retrieve them by internal id" in {
      val store = mk()

      val contracts = Seq(contract, contract2, contract3, contract4)
      val contractIds = contracts.map(_.contractId)

      for {
        _ <- store.storeContracts(contracts)
        internalIdsMap <- store.lookupBatchedNonCachedInternalIds(contractIds)
        persistedMap <- store.lookupBatchedNonCached(internalIdsMap.values)
      } yield {
        internalIdsMap.keys should contain theSameElementsAs contractIds
        internalIdsMap.foreach { case (contractId, internalId) =>
          persistedMap.get(internalId) match {
            case Some(persisted) =>
              persisted.inst.contractId shouldBe contractId
            case None =>
              fail(s"No persisted contract found for internal id $internalId")
          }
        }
        succeed
      }
    }
  }
}
