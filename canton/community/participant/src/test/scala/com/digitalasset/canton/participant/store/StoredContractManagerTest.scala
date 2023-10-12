// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.digitalasset.canton.participant.store.memory.InMemoryContractStore
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  suffixedId,
  transactionId,
}
import com.digitalasset.canton.protocol.WithTransactionId
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, RequestCounter}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class StoredContractManagerTest extends AsyncWordSpec with BaseTest {

  def mk(store: ContractStore): StoredContractManager =
    new StoredContractManager(store, loggerFactory)

  def preload(contracts: Seq[StoredContract] = Seq.empty[StoredContract]): Future[ContractStore] = {
    val store = new InMemoryContractStore(loggerFactory)
    contracts
      .parTraverse_ {
        case StoredContract(contract, rc, Some(txId)) =>
          store.storeCreatedContract(rc, txId, contract)
        case StoredContract(contract, rc, None) => store.storeDivulgedContract(rc, contract)
      }
      .map(_ => store)
  }

  private val contractId0 = suffixedId(0, 0)
  private val contractId1 = suffixedId(1, 0)
  private val contractId2 = suffixedId(2, 0)

  private val contract0 =
    asSerializable(contractId0, contractInstance = contractInstance(), agreementText = "instance0")
  private val contract1 =
    asSerializable(contractId1, contractInstance = contractInstance(), agreementText = "instance1")
  private val contract2 =
    asSerializable(contractId2, contractInstance = contractInstance(), agreementText = "instance2")

  private val rc0 = RequestCounter(0)
  private val rc1 = RequestCounter(1)
  private val rc2 = RequestCounter(2)

  private val transactionId0 = transactionId(0)
  private val transactionId1 = transactionId(1)

  private val createdContract = StoredContract.fromCreatedContract(contract0, rc0, transactionId0)
  private val divulgedContract = StoredContract.fromDivulgedContract(contract1, rc0)

  "lookup" should {
    "find contracts in backing store" in {
      for {
        store <- preload(Seq(createdContract, divulgedContract))
        manager = mk(store)
        created <- manager.lookup(createdContract.contractId).value
        divulged <- manager.lookup(divulgedContract.contractId).value
        notFound <- manager.lookup(contractId2).value
      } yield {
        created shouldBe Some(createdContract)
        divulged shouldBe Some(divulgedContract)
        notFound shouldBe None
      }
    }

    "find pending contracts" in {
      for {
        store <- preload()
        manager = mk(store)
        add <- manager.addPendingContracts(rc0, Seq(WithTransactionId(contract0, transactionId0)))
        found <- manager.lookup(contract0.contractId).value
        notFound <- manager.lookup(contractId1).value
      } yield {
        add shouldBe Set.empty[StoredContract]
        found shouldBe Some(StoredContract.fromCreatedContract(contract0, rc0, transactionId0))
        notFound shouldBe None
      }
    }
  }

  "addPendingContracts" should {
    "not write the contracts to the store" in {
      for {
        store <- preload()
        manager = mk(store)
        add <- manager.addPendingContracts(rc0, Seq(WithTransactionId(contract2, transactionId1)))
        notFound <- store.lookup(contract2.contractId).value
      } yield {
        add shouldBe Set.empty[StoredContract]
        notFound shouldBe None
      }
    }

    "keep only unwritten contracts" in {
      for {
        store <- preload(Seq(createdContract))
        manager = mk(store)
        add <- manager.addPendingContracts(
          rc0,
          Seq(
            WithTransactionId(createdContract.contract, transactionId0),
            WithTransactionId(contract1, transactionId1),
          ),
        )
        delete <- store.deleteContract(createdContract.contractId).value
        notFound <- manager.lookup(createdContract.contractId).value
      } yield {
        add shouldBe Set.empty[StoredContract]
        delete shouldBe Right(())
        notFound shouldBe None
      }
    }

    "report differing contracts" in {
      for {
        store <- preload(Seq(createdContract))
        manager = mk(store)
        add <- manager.addPendingContracts(rc0, Seq(WithTransactionId(contract1, transactionId1)))
        add2 <- manager.addPendingContracts(
          rc1,
          Seq(
            WithTransactionId(
              asSerializable(
                contract1.contractId,
                contractInstance = contractInstance(),
                agreementText = "instance1-modified",
              ),
              transactionId1,
            ),
            WithTransactionId(
              asSerializable(
                createdContract.contractId,
                contractInstance = contractInstance(),
                agreementText = "instance0-modified",
              ),
              transactionId0,
            ),
          ),
        )
        add3 <- manager.addPendingContracts(
          rc1,
          Seq(
            WithTransactionId(
              asSerializable(
                createdContract.contractId,
                contractInstance = contractInstance(),
                agreementText = "instance0",
              ),
              transactionId1,
            )
          ),
        )
      } yield {
        add shouldBe Set.empty[StoredContract]
        add2 shouldBe Set(
          StoredContract.fromCreatedContract(contract1, rc0, transactionId1),
          createdContract,
        )
        add3 shouldBe Set(createdContract)
      }
    }

    "ignore divulged contracts" in {
      for {
        store <- preload()
        manager = mk(store)
        divulged <- manager.storeDivulgedContracts(rc0, Seq(divulgedContract.contract)).value
        modified = asSerializable(
          divulgedContract.contractId,
          contractInstance = contractInstance(),
          agreementText = "divulged-modified",
        )
        add <- manager.addPendingContracts(rc0, Seq(WithTransactionId(modified, transactionId0)))
        lookup <- manager.lookup(divulgedContract.contractId).value
      } yield {
        divulged shouldBe Right(())
        add shouldBe Set.empty[StoredContract]
        lookup shouldBe Some(StoredContract.fromCreatedContract(modified, rc0, transactionId0))
      }
    }
  }

  "storeDivulgedContracts" should {
    "persist the contracts" in {
      for {
        store <- preload()
        manager = mk(store)
        divulged <- manager.storeDivulgedContracts(rc0, Seq(divulgedContract.contract)).value
        lookup <- store.lookup(divulgedContract.contractId).value
      } yield {
        divulged shouldBe Right(())
        lookup shouldBe Some(divulgedContract)
      }
    }

    "report differences" in {
      for {
        store <- preload(Seq(createdContract, divulgedContract))
        manager = mk(store)
        add <- manager.addPendingContracts(rc2, Seq(WithTransactionId(contract2, transactionId0)))
        modifiedC = asSerializable(
          createdContract.contractId,
          contractInstance = contractInstance(),
          agreementText = "instance0-modified",
        )
        modifiedD = asSerializable(
          divulgedContract.contractId,
          contractInstance = contractInstance(),
          agreementText = "instance1-modified",
        )
        modifiedP = asSerializable(
          contract2.contractId,
          contractInstance = contractInstance(),
          agreementText = "instance2-modified",
        )
        divulged <- manager.storeDivulgedContracts(rc2, Seq(modifiedC, modifiedD, modifiedP)).value
      } yield {
        divulged.leftMap(_.toList.toSet) shouldBe Left(
          Set(
            DuplicateContract(
              contract2.contractId,
              StoredContract.fromCreatedContract(contract2, rc2, transactionId0),
              StoredContract.fromDivulgedContract(modifiedP, rc2),
            )
          )
        )
      }
    }
  }

  "commitIfPending" should {
    "persist the pending contract" in {
      for {
        store <- preload()
        manager = mk(store)
        add <- manager.addPendingContracts(
          rc0,
          Seq(WithTransactionId(createdContract.contract, transactionId0)),
        )
        _commit <- manager.commitIfPending(rc0, Map(createdContract.contractId -> true))
        found <- store.lookup(createdContract.contractId).value
      } yield {
        add shouldBe Set.empty[StoredContract]
        found shouldBe Some(createdContract)
      }
    }

    "persist only pending contracts" in {
      val stored = StoredContract.fromCreatedContract(contract0, rc1, transactionId0)
      for {
        store <- preload(Seq(stored))
        manager = mk(store)
        add <- manager.addPendingContracts(
          rc0,
          Seq(
            WithTransactionId(contract0, transactionId0),
            WithTransactionId(contract1, transactionId1),
          ),
        )
        _commit <- manager.commitIfPending(
          rc0,
          Map(contract0.contractId -> true, contract1.contractId -> true),
        )
        lookup0 <- manager.lookup(contract0.contractId).value
        lookup1 <- manager.lookup(contract1.contractId).value
      } yield {
        add shouldBe Set.empty[StoredContract]
        lookup0 shouldBe Some(stored)
        lookup1 shouldBe Some(StoredContract.fromCreatedContract(contract1, rc0, transactionId1))
      }
    }

    "rollback pending contracts" in {
      for {
        store <- preload()
        manager = mk(store)
        add0 <- manager.addPendingContracts(
          rc0,
          Seq(
            WithTransactionId(contract0, transactionId0),
            WithTransactionId(contract1, transactionId1),
          ),
        )
        add1 <- manager.addPendingContracts(rc1, Seq(WithTransactionId(contract0, transactionId0)))
        _commit0 <- manager.commitIfPending(
          rc0,
          Map(contract0.contractId -> false, contract1.contractId -> false),
        )
        lookup0 <- manager.lookup(contract0.contractId).value
        lookup1 <- manager.lookup(contract1.contractId).value
        _commit1 <- manager.commitIfPending(rc1, Map(contract0.contractId -> true))
        written0 <- store.lookup(contract0.contractId).value
      } yield {
        add0 shouldBe Set.empty[StoredContract]
        add1 shouldBe Set.empty[StoredContract]
        lookup0.map(_.contract) shouldBe Some(contract0)
        lookup1 shouldBe None
        written0.map(_.contract) shouldBe Some(contract0)
      }
    }
  }
}
