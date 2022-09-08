// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MaximumLedgerTime
import com.daml.ledger.resources.Resource
import com.daml.lf.crypto.Hash
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.{ContractInstance, ValueRecord, ValueText}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.MutableCacheBackedContractStoreSpec.{cId_5, _}
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.{
  ContractState,
  KeyAssigned,
  KeyUnassigned,
}
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class MutableCacheBackedContractStoreSpec extends AsyncWordSpec with Matchers with MockitoSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "push" should {
    "update the contract state caches" in {
      val contractStateCaches = mock[ContractStateCaches]
      val contractStore = new MutableCacheBackedContractStore(
        metrics = new Metrics(new MetricRegistry),
        contractsReader = mock[LedgerDaoContractsReader],
        contractStateCaches = contractStateCaches,
      )

      val event1 = ContractStateEvent.Archived(
        contractId = ContractId.V1(Hash.hashPrivateKey("cid")),
        globalKey = None,
        stakeholders = Set.empty,
        eventOffset = Offset.beforeBegin,
        eventSequentialId = 1L,
      )
      val event2 = event1.copy(eventSequentialId = 2L)
      val updateBatch = Vector(event1, event2)

      contractStore.push(updateBatch)
      verify(contractStateCaches).push(updateBatch)

      succeed
    }
  }

  "lookupActiveContract" should {
    "read-through the contract state cache" in {
      val spyContractsReader = spy(ContractsReaderFixture())

      for {
        store <- contractStore(cachesSize = 1L, spyContractsReader).asFuture
        _ = store.contractStateCaches.contractState.cacheIndex = offset1
        cId2_lookup <- store.lookupActiveContract(Set(alice), cId_2)
        another_cId2_lookup <- store.lookupActiveContract(Set(alice), cId_2)

        _ = store.contractStateCaches.contractState.cacheIndex = offset2
        cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)
        another_cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)

        _ = store.contractStateCaches.contractState.cacheIndex = offset3
        nonExistentCId = contractId(5)
        nonExistentCId_lookup <- store.lookupActiveContract(Set.empty, nonExistentCId)
        another_nonExistentCId_lookup <- store.lookupActiveContract(Set.empty, nonExistentCId)
      } yield {
        cId2_lookup shouldBe Option.empty
        another_cId2_lookup shouldBe Option.empty

        cId3_lookup shouldBe Some(contract3)
        another_cId3_lookup shouldBe Some(contract3)

        nonExistentCId_lookup shouldBe Option.empty
        another_nonExistentCId_lookup shouldBe Option.empty

        verify(spyContractsReader).lookupContractState(cId_2, offset1)
        verify(spyContractsReader).lookupContractState(cId_3, offset2)
        verify(spyContractsReader).lookupContractState(nonExistentCId, offset3)
        succeed
      }
    }

    "read-through the cache without storing negative lookups" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      for {
        store <- contractStore(cachesSize = 1L, spyContractsReader).asFuture
        _ = store.contractStateCaches.contractState.cacheIndex = offset1
        negativeLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
        positiveLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
      } yield {
        negativeLookup_cId6 shouldBe Option.empty
        positiveLookup_cId6 shouldBe Some(contract6)

        verify(spyContractsReader, times(wantedNumberOfInvocations = 2))
          .lookupContractState(cId_6, offset1)
        succeed
      }
    }

    "resort to resolveDivulgenceLookup on not found" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      for {
        store <- contractStore(cachesSize = 1L, spyContractsReader).asFuture
        _ = store.contractStateCaches.contractState.cacheIndex = offset1
        resolvedLookup_cId7 <- store.lookupActiveContract(Set(bob), cId_7)
      } yield {
        resolvedLookup_cId7 shouldBe Some(contract7)

        verify(spyContractsReader).lookupActiveContractAndLoadArgument(Set(bob), cId_7)
        succeed
      }
    }

    "present the contract state if visible at specific cache offsets (with no cache)" in {
      for {
        store <- contractStore(cachesSize = 0L).asFuture
        cId1_lookup0 <- store.lookupActiveContract(Set(alice), cId_1)
        cId2_lookup0 <- store.lookupActiveContract(Set(bob), cId_2)

        _ = store.contractStateCaches.contractState.cacheIndex = offset1
        cId1_lookup1 <- store.lookupActiveContract(Set(alice), cId_1)
        cid1_lookup1_archivalNotDivulged <- store.lookupActiveContract(Set(charlie), cId_1)

        _ = store.contractStateCaches.contractState.cacheIndex = offset2
        cId2_lookup2 <- store.lookupActiveContract(Set(bob), cId_2)
        cid2_lookup2_divulged <- store.lookupActiveContract(Set(charlie), cId_2)
        cid2_lookup2_nonVisible <- store.lookupActiveContract(Set(alice), cId_2)
      } yield {
        cId1_lookup0 shouldBe Some(contract1)
        cId2_lookup0 shouldBe Option.empty

        cId1_lookup1 shouldBe Option.empty
        cid1_lookup1_archivalNotDivulged shouldBe Some(contract1)

        cId2_lookup2 shouldBe Some(contract2)
        cid2_lookup2_divulged shouldBe Some(contract2)
        cid2_lookup2_nonVisible shouldBe Option.empty
      }
    }
  }

  "lookupContractKey" should {
    "read-through the key state cache" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      val unassignedKey = globalKey("unassigned")

      for {
        store <- contractStore(cachesSize = 1L, spyContractsReader).asFuture
        assigned_firstLookup <- store.lookupContractKey(Set(alice), someKey)
        assigned_secondLookup <- store.lookupContractKey(Set(alice), someKey)

        _ = store.contractStateCaches.keyState.cacheIndex = offset1
        unassigned_firstLookup <- store.lookupContractKey(Set(alice), unassignedKey)
        unassigned_secondLookup <- store.lookupContractKey(Set(alice), unassignedKey)
      } yield {
        assigned_firstLookup shouldBe Some(cId_1)
        assigned_secondLookup shouldBe Some(cId_1)

        unassigned_firstLookup shouldBe Option.empty
        unassigned_secondLookup shouldBe Option.empty

        verify(spyContractsReader).lookupKeyState(someKey, offset0)(loggingContext)
        verify(spyContractsReader).lookupKeyState(unassignedKey, offset1)(loggingContext)
        verifyNoMoreInteractions(spyContractsReader)
        succeed
      }
    }

    "present the key state if visible at specific cache offsets (with no cache)" in {
      for {
        store <- contractStore(cachesSize = 0L).asFuture
        key_lookup0 <- store.lookupContractKey(Set(alice), someKey)

        _ = store.contractStateCaches.keyState.cacheIndex = offset1
        key_lookup1 <- store.lookupContractKey(Set(alice), someKey)

        _ = store.contractStateCaches.keyState.cacheIndex = offset2
        key_lookup2 <- store.lookupContractKey(Set(bob), someKey)
        key_lookup2_notVisible <- store.lookupContractKey(Set(charlie), someKey)

        _ = store.contractStateCaches.keyState.cacheIndex = offset3
        key_lookup3 <- store.lookupContractKey(Set(bob), someKey)
      } yield {
        key_lookup0 shouldBe Some(cId_1)
        key_lookup1 shouldBe Option.empty
        key_lookup2 shouldBe Some(cId_2)
        key_lookup2_notVisible shouldBe Option.empty
        key_lookup3 shouldBe Option.empty
      }
    }
  }

  "lookupMaximumLedgerTime" should {
    "return the maximum ledger time with cached values" in {
      for {
        store <- contractStore(cachesSize = 2L).asFuture
        // populate the cache
        _ <- store.lookupActiveContract(Set(bob), cId_2)
        _ <- store.lookupActiveContract(Set(bob), cId_3)
        maxLedgerTime <- store.lookupMaximumLedgerTimeAfterInterpretation(Set(cId_2, cId_3, cId_4))
      } yield {
        maxLedgerTime shouldBe MaximumLedgerTime.Max(t4)
      }
    }

    "fail if one of the cached contract ids doesn't have an associated active contract" in {
      for {
        store <- contractStore(cachesSize = 1L).asFuture
        // populate the cache
        _ <- store.lookupActiveContract(Set(bob), cId_5)
        maxLedgerTime <- store.lookupMaximumLedgerTimeAfterInterpretation(Set(cId_1, cId_5))
      } yield {
        maxLedgerTime shouldBe MaximumLedgerTime.Archived(Set(cId_5))
      }
    }

    "fail if one of the fetched contract ids doesn't have an associated active contract" in {
      for {
        store <- contractStore(cachesSize = 0L).asFuture
        maxLedgerTime <- store.lookupMaximumLedgerTimeAfterInterpretation(Set(cId_1, cId_5))
      } yield {
        // since with cacheIndex 2L both of them are archived due to set semantics it is accidental which we check first with read-through
        maxLedgerTime shouldBe a[MaximumLedgerTime.Archived]
      }
    }
  }

  "lookupContractAfterInterpretation" should {
    "resolve lookup from cache" in {
      for {
        store <- contractStore(cachesSize = 2L).asFuture
        _ = store.contractStateCaches.contractState.putBatch(
          offset2,
          Map(
            // Populate the cache with an active contract
            cId_4 -> ContractStateValue.Active(
              contract = contract4,
              stakeholders = Set.empty,
              createLedgerEffectiveTime = t4,
            ),
            // Populate the cache with an archived contract
            cId_5 -> ContractStateValue.Archived(Set.empty),
          ),
        )
        activeContractLookupResult <- store.lookupContractForValidation(cId_4)
        archivedContractLookupResult <- store.lookupContractForValidation(cId_5)
      } yield {
        activeContractLookupResult shouldBe Some(contract4 -> t4)
        archivedContractLookupResult shouldBe None
      }
    }

    "resolve lookup from the ContractsReader when not cached" in {
      for {
        store <- contractStore(cachesSize = 0L).asFuture
        activeContractLookupResult <- store.lookupContractForValidation(cId_4)
        archivedContractLookupResult <- store.lookupContractForValidation(cId_5)
      } yield {
        activeContractLookupResult shouldBe Some(contract4 -> t4)
        archivedContractLookupResult shouldBe None
      }
    }
  }
}

object MutableCacheBackedContractStoreSpec {
  private val offset0 = offset(0L)
  private val offset1 = offset(1L)
  private val offset2 = offset(2L)
  private val offset3 = offset(3L)

  private val Seq(alice, bob, charlie) = Seq("alice", "bob", "charlie").map(party)
  private val (
    Seq(cId_1, cId_2, cId_3, cId_4, cId_5, cId_6, cId_7),
    Seq(contract1, contract2, contract3, contract4, _, contract6, contract7),
    Seq(t1, t2, t3, t4, _, t6, _),
  ) =
    (1 to 7).map { id =>
      (contractId(id), contract(s"id$id"), Timestamp.assertFromLong(id.toLong * 1000L))
    }.unzip3

  private val someKey = globalKey("key1")

  private def contractStore(
      cachesSize: Long,
      readerFixture: LedgerDaoContractsReader = ContractsReaderFixture(),
      startIndexExclusive: Offset = offset0,
  )(implicit loggingContext: LoggingContext) = {
    val metrics = new Metrics(new MetricRegistry)
    val contractStore = new MutableCacheBackedContractStore(
      metrics,
      readerFixture,
      contractStateCaches =
        ContractStateCaches.build(startIndexExclusive, cachesSize, cachesSize, metrics)(
          scala.concurrent.ExecutionContext.global,
          loggingContext,
        ),
    )(scala.concurrent.ExecutionContext.global, loggingContext)

    Resource.successful(contractStore)
  }

  case class ContractsReaderFixture() extends LedgerDaoContractsReader {
    @volatile private var initialResultForCid6 = Future.successful(Option.empty[ContractState])

    override def lookupKeyState(key: Key, validAt: Offset)(implicit
        loggingContext: LoggingContext
    ): Future[LedgerDaoContractsReader.KeyState] = (key, validAt) match {
      case (`someKey`, `offset0`) => Future.successful(KeyAssigned(cId_1, Set(alice)))
      case (`someKey`, `offset2`) => Future.successful(KeyAssigned(cId_2, Set(bob)))
      case _ => Future.successful(KeyUnassigned)
    }

    override def lookupContractState(contractId: ContractId, validAt: Offset)(implicit
        loggingContext: LoggingContext
    ): Future[Option[LedgerDaoContractsReader.ContractState]] = {
      (contractId, validAt) match {
        case (`cId_1`, `offset0`) => activeContract(contract1, Set(alice), t1)
        case (`cId_1`, validAt) if validAt > offset0 => archivedContract(Set(alice))
        case (`cId_2`, validAt) if validAt >= offset1 => activeContract(contract2, Set(bob), t2)
        case (`cId_3`, _) => activeContract(contract3, Set(bob), t3)
        case (`cId_4`, _) => activeContract(contract4, Set(bob), t4)
        case (`cId_5`, _) => archivedContract(Set(bob))
        case (`cId_6`, _) =>
          // Simulate store being populated from one query to another
          val result = initialResultForCid6
          initialResultForCid6 = activeContract(contract6, Set(alice), t6)
          result
        case _ => Future.successful(Option.empty)
      }
    }

    override def lookupActiveContractAndLoadArgument(
        forParties: Set[Party],
        contractId: ContractId,
    )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
      (contractId, forParties) match {
        case (`cId_2`, parties) if parties.contains(charlie) =>
          // Purposely return a wrong associated contract than the cId so it can be distinguished upstream
          // that the cached variant of this method was not used
          Future.successful(Some(contract3))
        case (`cId_1`, parties) if parties.contains(charlie) =>
          Future.successful(Some(contract1))
        case (`cId_7`, parties) if parties == Set(bob) => Future.successful(Some(contract7))
        case _ => Future.successful(Option.empty)
      }

    override def lookupActiveContractWithCachedArgument(
        forParties: Set[Party],
        contractId: ContractId,
        createArgument: Value,
    )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
      (contractId, forParties) match {
        case (`cId_2`, parties) if parties.contains(charlie) => Future.successful(Some(contract2))
        case _ => Future.successful(Option.empty)
      }
  }

  private def activeContract(
      contract: Contract,
      parties: Set[Party],
      ledgerEffectiveTime: Timestamp,
  ) =
    Future.successful(
      Some(LedgerDaoContractsReader.ActiveContract(contract, parties, ledgerEffectiveTime))
    )

  private def archivedContract(parties: Set[Party]) =
    Future.successful(Some(LedgerDaoContractsReader.ArchivedContract(parties)))

  private def party(name: String) = Party.assertFromString(name)

  private def contract(templateName: String): Contract = {
    val templateId = Identifier.assertFromString(s"some:template:$templateName")
    val contractArgument = ValueRecord(
      Some(templateId),
      ImmArray.Empty,
    )
    val contractInstance = ContractInstance(
      templateId,
      contractArgument,
      "some agreement",
    )
    TransactionBuilder().versionContract(contractInstance)
  }

  private def contractId(id: Int): ContractId =
    ContractId.V1(Hash.hashPrivateKey(id.toString))

  private def globalKey(desc: String): Key =
    GlobalKey.assertBuild(Identifier.assertFromString(s"some:template:$desc"), ValueText(desc))

  private def offset(idx: Long) = Offset.fromByteArray(BigInt(idx).toByteArray)
}
