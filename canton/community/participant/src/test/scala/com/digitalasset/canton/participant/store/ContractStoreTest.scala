// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.QualifiedName
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  packageId,
  transactionId,
}
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  ExampleTransactionFactory,
  SerializableContract,
  WithTransactionId,
}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, LfPartyId, RequestCounter}
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
    val transactionId1 = transactionId(1)
    val transactionId2 = transactionId(2)
    val rc = RequestCounter(0)
    val storedContract = StoredContract.fromCreatedContract(contract, rc, transactionId1)
    val divulgedContract = StoredContract.fromDivulgedContract(contract, rc)

    val rc2 = RequestCounter(1)
    val let2 = CantonTimestamp.Epoch.plusSeconds(5)
    val pkgId2 = Ref.PackageId.assertFromString("different_id")
    val contract2 = asSerializable(
      contractId2,
      contractInstance = contractInstance(
        templateId = Ref.Identifier(pkgId2, QualifiedName.assertFromString("module:template"))
      ),
      ledgerTime = let2,
      agreementText = "text",
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
        agreementText = "instance",
      )

    val contract5 =
      asSerializable(
        contractId5,
        contractInstance = contractInstance(
          templateId = Ref.Identifier(pkgId2, templateName3)
        ),
        agreementText = "instance",
      )

    "store and retrieve a created contract" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(rc, transactionId1, contract)
        c <- store.lookupE(contractId)
        inst <- store.lookupContractE(contractId)
      } yield {
        c shouldEqual storedContract
        inst shouldEqual contract
      }
    }

    "update a created contract with instance size > 32kB (oracle related, see DbContractStore)" in {
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
          agreementText = "A" * 35000,
          metadata = metadata,
        )

      val storedLargeContractUpdated =
        StoredContract.fromCreatedContract(largeContract, rc2, transactionId1)

      for {
        _ <- store.storeCreatedContract(rc, transactionId1, largeContract)
        _ <- store.storeCreatedContract(rc2, transactionId1, largeContract)
        c <- store.lookupE(contractId)
        inst <- store.lookupContractE(contractId)
      } yield {
        c shouldEqual storedLargeContractUpdated
        inst shouldEqual largeContract
      }
    }

    "store and retrieve a divulged contract that does not yet exist" in {
      val store = mk()

      for {
        _ <- store.storeDivulgedContract(rc, contract)
        c <- store.lookupE(contractId)
        inst <- store.lookupContractE(contractId)
      } yield {
        c shouldEqual divulgedContract
        inst shouldEqual contract
      }
    }

    "store the same contract twice for the same id" in {
      val store = mk()

      val element = WithTransactionId(contract, transactionId1)

      for {
        _ <- store.storeCreatedContracts(rc, Seq(element, element))
        _ <- store.storeDivulgedContracts(rc, Seq(contract2, contract2))
      } yield succeed
    }

    "take the second created contract" in {
      val store = mk()
      for {
        _ <- store.storeCreatedContract(rc, transactionId1, contract)
        _ <- store.storeCreatedContract(rc2, transactionId2, contract)
        c <- store.lookupE(contract.contractId)
      } yield c shouldBe StoredContract.fromCreatedContract(
        contract,
        rc2,
        transactionId2,
      )
    }

    "succeed when storing a different contract for an existing id" must {

      val divulgedContract2 =
        StoredContract.fromDivulgedContract(contract, rc2)
      "for divulged contracts" in {
        val store = mk()

        for {
          _ <- store.storeDivulgedContract(rc, contract)
          _ <- store.storeDivulgedContract(rc2, contract)
          c <- store.lookupE(contract.contractId)
        } yield c shouldBe divulgedContract2
      }

      "for divulged contracts reversed" in {
        val store = mk()

        for {
          _ <- store.storeDivulgedContract(rc2, contract)
          _ <- store.storeDivulgedContract(rc, contract)
          c <- store.lookupE(contract.contractId)
        } yield c shouldBe divulgedContract2
      }

      val storedContract2 =
        StoredContract.fromCreatedContract(
          contract,
          rc2,
          transactionId2,
        )
      "for created contracts" in {
        val store = mk()

        for {
          _ <- store.storeCreatedContract(rc, transactionId1, contract)
          _ <- store.storeCreatedContract(
            rc2,
            transactionId2,
            contract,
          )
          c <- store.lookupE(contract.contractId)
        } yield c shouldBe storedContract2
      }

      "for created contracts reversed" in {
        val store = mk()

        for {
          _ <- store.storeCreatedContract(
            rc2,
            transactionId2,
            contract,
          )
          _ <- store.storeCreatedContract(rc, transactionId1, contract)
          c <- store.lookupE(contract.contractId)
        } yield c shouldBe storedContract2
      }

      "for divulged contract with higher request counter than created contract" in {
        val store = mk()

        for {
          _ <- store.storeDivulgedContract(rc2 + 1, contract)
          _ <- store.storeCreatedContract(
            rc2,
            transactionId2,
            contract,
          )
          c <- store.lookupE(contract.contractId)
        } yield c shouldBe storedContract2
      }

      "for divulged contract with higher request counter than created contract reversed" in {
        val store = mk()

        for {
          _ <- store.storeCreatedContract(
            rc2,
            transactionId2,
            contract,
          )
          _ <- store.storeDivulgedContract(rc2 + 1, contract)
          c <- store.lookupE(contract.contractId)
        } yield c shouldBe storedContract2
      }

      "for divulged contract with lower request counter than created contract" in {
        val store = mk()

        for {
          _ <- store.storeDivulgedContract(rc2 - 1, contract)
          _ <- store.storeCreatedContract(
            rc2,
            transactionId2,
            contract,
          )
          c <- store.lookupE(contract.contractId)
        } yield c shouldBe storedContract2
      }

      "for divulged contract with lower request counter than created contract reversed" in {
        val store = mk()

        for {
          _ <- store.storeCreatedContract(
            rc2,
            transactionId2,
            contract,
          )
          _ <- store.storeDivulgedContract(rc2 - 1, contract)
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

    "ignore a divulged contract with a different contract instance" in {
      val store = mk()
      val modifiedContract = contract2.copy(contractId = contractId)

      for {
        _ <- store.storeCreatedContract(rc, transactionId1, contract)
        _ <- store.storeDivulgedContract(rc2, modifiedContract)
        c <- store.lookupE(contract.contractId)
      } yield c shouldBe storedContract
    }

    "delete a contract" in {
      val store = mk()
      for {
        _ <- store.storeCreatedContract(rc, transactionId1, contract)
        res1 <- store.deleteContract(contractId).value
        c <- store.lookupE(contractId).value
      } yield {
        res1 shouldEqual Right(())
        c shouldEqual Left(UnknownContract(contractId))
      }
    }

    "reject deleting an nonexistent contract" in {
      val store = mk()
      for {
        res <- store.deleteContract(contractId).value
      } yield res shouldEqual Left(UnknownContract(contractId))
    }

    "delete a set of contracts" in {
      val store = mk()
      for {
        _ <- List(contract, contract2, contract4, contract5)
          .parTraverse(store.storeCreatedContract(rc, transactionId1, _))
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

    "delete divulged contracts" in {
      val store = mk()
      for {
        _ <- store.storeDivulgedContract(RequestCounter(0), contract)
        _ <- store.storeCreatedContract(RequestCounter(0), transactionId1, contract2)
        _ <- store.storeDivulgedContract(RequestCounter(1), contract3)
        _ <- store.storeCreatedContract(RequestCounter(1), transactionId1, contract4)
        _ <- store.storeDivulgedContract(RequestCounter(2), contract5)
        _ <- store.deleteDivulged(RequestCounter(1))
        c1 <- store.lookupContract(contract.contractId).value
        c2 <- store.lookupContract(contract2.contractId).value
        c3 <- store.lookupContract(contract3.contractId).value
        c4 <- store.lookupContract(contract4.contractId).value
        c5 <- store.lookupContract(contract5.contractId).value
      } yield {
        c1 shouldBe None
        c2 shouldBe Some(contract2)
        c3 shouldBe None
        c4 shouldBe Some(contract4)
        c5 shouldBe Some(contract5)
      }
    }

    "find contracts by filters" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(rc, transactionId1, contract)
        _ <- store.storeCreatedContract(rc, transactionId1, contract2)
        _ <- store.storeCreatedContract(rc, transactionId1, contract3)
        _ <- store.storeCreatedContract(rc, transactionId1, contract4)
        _ <- store.storeCreatedContract(
          rc,
          transactionId2,
          contract2.copy(
            contractId = contractId5,
            ledgerCreateTime = LedgerCreateTime(CantonTimestamp.Epoch),
          ),
        )

        resId <- store.find(filterId = Some(contractId.coid), None, None, 100)
        resPkg <- store
          .find(filterId = None, filterPackage = Some(pkgId2), None, 100)
        resPkgLimit <- store
          .find(filterId = None, filterPackage = Some(pkgId2), None, 2)
        resTemplatePkg <- store.find(
          filterId = None,
          filterPackage = Some(contract4.contractInstance.unversioned.template.packageId),
          filterTemplate =
            Some(contract4.contractInstance.unversioned.template.qualifiedName.toString()),
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
        _ <- store.storeCreatedContract(rc, transactionId1, contract)
        _ <- store.storeCreatedContract(rc, transactionId2, contract2)
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
        _ <- store.storeCreatedContract(rc, transactionId1, contract)
        _ <- store.storeCreatedContract(rc, transactionId1, contract2)
        _ <- store.storeCreatedContract(rc, transactionId1, contract3)
        _ <- store.storeCreatedContract(rc, transactionId1, contract4)
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
        _ <- store.storeCreatedContract(rc, transactionId1, contract)
        _ <- store.storeCreatedContract(rc, transactionId1, contract3)
        _ <- store.storeCreatedContract(rc, transactionId1, contract4)
        res <- store.lookupStakeholders(Set(contractId, contractId2, contractId4)).value
      } yield {
        res shouldBe Left(UnknownContracts(Set(contractId2)))
      }
    }
  }
}
