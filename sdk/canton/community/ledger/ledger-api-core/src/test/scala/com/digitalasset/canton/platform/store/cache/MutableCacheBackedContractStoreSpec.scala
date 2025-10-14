// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.data.NonEmptyVector
import com.daml.ledger.resources.Resource
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.ExistingContractStatus
import com.digitalasset.canton.ledger.participant.state.index.{ContractState, ContractStateStatus}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.store
import com.digitalasset.canton.participant.store.memory.InMemoryContractStore
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStoreSpec.*
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.protocol.ExampleContractFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{HasExecutionContext, TestEssentials}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.transaction.CreationTime
import com.digitalasset.daml.lf.value.Value.{ContractId, ValueText}
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

class MutableCacheBackedContractStoreSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with TestEssentials
    with HasExecutionContext {

  implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  "push" should {
    "update the contract state caches" in {
      val contractStateCaches = mock[ContractStateCaches]
      val contractStore = new MutableCacheBackedContractStore(
        contractsReader = mock[LedgerDaoContractsReader],
        contractStateCaches = contractStateCaches,
        loggerFactory = loggerFactory,
        contractStore = mock[store.ContractStore],
      )

      val event1 = ContractStateEvent.Archived(
        contractId = ContractId.V1(Hash.hashPrivateKey("cid")),
        globalKey = None,
      )
      val event2 = event1
      val updateBatch = NonEmptyVector.of(event1, event2)

      contractStore.contractStateCaches.push(updateBatch, 10)
      verify(contractStateCaches).push(updateBatch, 10)

      succeed
    }
  }

  "lookupActiveContract" should {
    "read-through the contract state cache" in {
      val spyContractsReader = spy(ContractsReaderFixture())

      for {
        store <- contractStore(
          cachesSize = 1L,
          loggerFactory = loggerFactory,
          spyContractsReader,
        ).asFuture
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId1
        cId2_lookup <- store.lookupActiveContract(Set(charlie), cId_2)
        another_cId2_lookup <- store.lookupActiveContract(Set(charlie), cId_2)

        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId2
        cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)
        another_cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)

        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId3
        nonExistentCId = cId_5
        nonExistentCId_lookup <- store.lookupActiveContract(Set.empty, nonExistentCId)
        another_nonExistentCId_lookup <- store.lookupActiveContract(Set.empty, nonExistentCId)
      } yield {
        cId2_lookup shouldBe Option.empty
        another_cId2_lookup shouldBe Option.empty

        cId3_lookup.map(_.templateId) shouldBe Some(contract3.inst.templateId)
        another_cId3_lookup.map(_.templateId) shouldBe Some(contract3.inst.templateId)

        nonExistentCId_lookup shouldBe Option.empty
        another_nonExistentCId_lookup shouldBe Option.empty

        // The cache is evicted BOTH on the number of entries AND memory pressure
        // So even though a read-through populates missing entries,
        // they can be immediately evicted by GCs and lead to subsequent misses.
        // Hence, verify atLeastOnce for LedgerDaoContractsReader.lookupContractState
        verify(spyContractsReader, atLeastOnce).lookupContractState(cId_2, eventSeqId1)
        verify(spyContractsReader, atLeastOnce).lookupContractState(cId_3, eventSeqId2)
        verify(spyContractsReader, atLeastOnce).lookupContractState(nonExistentCId, eventSeqId3)
        succeed
      }
    }

    "read-through the cache without storing negative lookups" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      for {
        store <- contractStore(
          cachesSize = 1L,
          loggerFactory = loggerFactory,
          spyContractsReader,
        ).asFuture
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId1
        negativeLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
        positiveLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
      } yield {
        negativeLookup_cId6 shouldBe Option.empty
        positiveLookup_cId6 shouldBe Option.empty

        verify(spyContractsReader, times(wantedNumberOfInvocations = 1))
          .lookupContractState(cId_6, eventSeqId1)
        succeed
      }
    }

    "present the contract state if visible at specific cache offsets (with no cache)" in {
      for {
        store <- contractStore(cachesSize = 0L, loggerFactory).asFuture
        cId1_lookup0 <- store.lookupActiveContract(Set(alice), cId_1)
        cId2_lookup0 <- store.lookupActiveContract(Set(bob), cId_2)

        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId1
        cId1_lookup1 <- store.lookupActiveContract(Set(alice), cId_1)
        cid1_lookup1_archivalNotDivulged <- store.lookupActiveContract(Set(charlie), cId_1)

        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId2
        cId2_lookup2 <- store.lookupActiveContract(Set(bob), cId_2)
        cid2_lookup2_divulged <- store.lookupActiveContract(Set(charlie), cId_2)
        cid2_lookup2_nonVisible <- store.lookupActiveContract(Set(charlie), cId_2)
      } yield {
        cId1_lookup0.map(_.templateId) shouldBe Some(contract1.inst.templateId)
        cId2_lookup0 shouldBe Option.empty

        cId1_lookup1 shouldBe Option.empty
        cid1_lookup1_archivalNotDivulged shouldBe None

        cId2_lookup2.map(_.templateId) shouldBe Some(contract2.inst.templateId)
        cid2_lookup2_divulged shouldBe None
        cid2_lookup2_nonVisible shouldBe Option.empty
      }
    }
  }

  "lookupContractKey" should {
    "read-through the key state cache" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      val unassignedKey = globalKey("unassigned")

      for {
        store <- contractStore(
          cachesSize = 1L,
          loggerFactory = loggerFactory,
          spyContractsReader,
        ).asFuture
        assigned_firstLookup <- store.lookupContractKey(Set(alice), someKey)
        assigned_secondLookup <- store.lookupContractKey(Set(alice), someKey)

        _ = store.contractStateCaches.keyState.cacheEventSeqIdIndex = eventSeqId1
        unassigned_firstLookup <- store.lookupContractKey(Set(alice), unassignedKey)
        unassigned_secondLookup <- store.lookupContractKey(Set(alice), unassignedKey)
      } yield {
        assigned_firstLookup shouldBe Some(cId_1)
        assigned_secondLookup shouldBe Some(cId_1)

        unassigned_firstLookup shouldBe Option.empty
        unassigned_secondLookup shouldBe Option.empty

        verify(spyContractsReader).lookupKeyState(someKey, eventSeqId0)(loggingContext)
        // looking up the key state will prefetch and use the contract state
        verify(spyContractsReader).lookupContractState(cId_1, eventSeqId0)(loggingContext)
        verify(spyContractsReader).lookupContractState(cId_1, eventSeqId0)(loggingContext)
        verify(spyContractsReader).lookupKeyState(unassignedKey, eventSeqId1)(loggingContext)
        verifyNoMoreInteractions(spyContractsReader)
        succeed
      }
    }

    "present the key state if visible at specific cache offsets (with no cache)" in {
      for {
        store <- contractStore(cachesSize = 0L, loggerFactory).asFuture
        key_lookup0 <- store.lookupContractKey(Set(alice), someKey)

        _ = store.contractStateCaches.keyState.cacheEventSeqIdIndex = eventSeqId1
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId1
        key_lookup1 <- store.lookupContractKey(Set(alice), someKey)

        _ = store.contractStateCaches.keyState.cacheEventSeqIdIndex = eventSeqId2
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId2
        key_lookup2 <- store.lookupContractKey(Set(bob), someKey)
        key_lookup2_notVisible <- store.lookupContractKey(Set(charlie), someKey)

        _ = store.contractStateCaches.keyState.cacheEventSeqIdIndex = eventSeqId3
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId3
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

  "lookupContractStateWithoutDivulgence" should {

    "resolve lookup from cache" in {
      for {
        store <- contractStore(cachesSize = 2L, loggerFactory).asFuture
        _ = store.contractStateCaches.contractState.putBatch(
          eventSeqId2,
          Map(
            // Populate the cache with an active contract
            cId_4 -> ContractStateStatus.Active,
            // Populate the cache with an archived contract
            cId_5 -> ContractStateStatus.Archived,
          ),
        )
        activeContractLookupResult <- store.lookupContractState(cId_4)
        archivedContractLookupResult <- store.lookupContractState(cId_5)
        nonExistentContractLookupResult <- store.lookupContractState(cId_7)
      } yield {
        activeContractLookupResult shouldBe ContractState.Active(contract4.inst)
        archivedContractLookupResult shouldBe ContractState.Archived
        nonExistentContractLookupResult shouldBe ContractState.NotFound
      }
    }

    "resolve lookup from the ContractsReader when not cached" in {
      for {
        store <- contractStore(cachesSize = 0L, loggerFactory).asFuture
        activeContractLookupResult <- store.lookupContractState(cId_4)
        archivedContractLookupResult <- store.lookupContractState(cId_5)
        nonExistentContractLookupResult <- store.lookupContractState(cId_7)
      } yield {
        activeContractLookupResult shouldBe ContractState.Active(contract4.inst)
        archivedContractLookupResult shouldBe ContractState.Archived
        nonExistentContractLookupResult shouldBe ContractState.NotFound
      }
    }
  }
}

@nowarn("msg=match may not be exhaustive")
object MutableCacheBackedContractStoreSpec {
  private val eventSeqId0 = 1L
  private val eventSeqId1 = 2L
  private val eventSeqId2 = 3L
  private val eventSeqId3 = 4L

  private val Seq(alice, bob, charlie) = Seq("alice", "bob", "charlie").map(party)

  private val someKey = globalKey("key1")

  private val exStakeholders = Set(bob, alice)
  private val exSignatories = Set(alice)
  private val exMaintainers = Set(alice)
  private val someKeyWithMaintainers = KeyWithMaintainers(someKey, exMaintainers)

  private val timeouts = ProcessingTimeout()

  private val Seq(t1, t2, t3, t4, t5, t6, t7) = (1 to 7).map { id =>
    Time.Timestamp.assertFromLong(id.toLong * 1000L)
  }

  private val (
    Seq(cId_1, cId_2, cId_3, cId_4, cId_5, cId_6, cId_7),
    Seq(contract1, contract2, contract3, contract4, _, contract6, _),
  ) =
    Seq(
      contract(Set(alice), t1),
      contract(exStakeholders, t2),
      contract(exStakeholders, t3),
      contract(exStakeholders, t4),
      contract(exStakeholders, t5),
      contract(Set(alice), t6),
      contract(exStakeholders, t7),
    ).map(c => c.contractId -> c).unzip

  private def contractStore(
      cachesSize: Long,
      loggerFactory: NamedLoggerFactory,
      readerFixture: LedgerDaoContractsReader = ContractsReaderFixture(),
  )(implicit ec: ExecutionContext, traceContext: TraceContext) = {
    val metrics = LedgerApiServerMetrics.ForTesting
    val startIndexExclusive = eventSeqId0
    val contractStore = new MutableCacheBackedContractStore(
      readerFixture,
      contractStateCaches = ContractStateCaches
        .build(startIndexExclusive, cachesSize, cachesSize, metrics, loggerFactory),
      loggerFactory = loggerFactory,
      contractStore = inMemoryContractStore(loggerFactory),
    )

    Resource.successful(contractStore)
  }

  @SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is spied in tests
  case class ContractsReaderFixture() extends LedgerDaoContractsReader {
    @volatile private var initialResultForCid6 =
      Future.successful(Option.empty[ExistingContractStatus])

    override def lookupKeyState(key: Key, notEarlierThanEventSeqId: Long)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[LedgerDaoContractsReader.KeyState] = (key, notEarlierThanEventSeqId) match {
      case (`someKey`, `eventSeqId0`) => Future.successful(KeyAssigned(cId_1))
      case (`someKey`, `eventSeqId2`) => Future.successful(KeyAssigned(cId_2))
      case _ => Future.successful(KeyUnassigned)
    }

    override def lookupKeyStatesFromDb(keys: Seq[Key], notEarlierThanOffset: Long)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Map[Key, KeyState]] = ??? // not used in this test

    override def lookupContractState(contractId: ContractId, notEarlierThanEventSeqId: Long)(
        implicit loggingContext: LoggingContextWithTrace
    ): Future[Option[ExistingContractStatus]] =
      (contractId, notEarlierThanEventSeqId) match {
        case (`cId_1`, `eventSeqId0`) => activeContract
        case (`cId_1`, validAt) if validAt > eventSeqId0 => archivedContract
        case (`cId_2`, validAt) if validAt >= eventSeqId1 =>
          activeContract
        case (`cId_3`, _) => activeContract
        case (`cId_4`, _) => activeContract
        case (`cId_5`, _) => archivedContract
        case (`cId_6`, _) =>
          // Simulate store being populated from one query to another
          val result = initialResultForCid6
          initialResultForCid6 = activeContract
          result
        case _ => Future.successful(Option.empty)
      }
  }

  def inMemoryContractStore(
      loggerFactory: NamedLoggerFactory
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): InMemoryContractStore = {
    val store = new InMemoryContractStore(timeouts, loggerFactory)
    val contracts = Seq(
      contract1,
      contract2,
      contract3,
      contract4,
      contract6,
    )
    store.storeContracts(contracts).discard
    store
  }

  private def contract(
      stakeholders: Set[Party],
      ledgerEffectiveTime: Time.Timestamp,
      key: Option[KeyWithMaintainers] = Some(someKeyWithMaintainers),
  ) =
    ExampleContractFactory.build(
      createdAt = CreationTime.CreatedAt(ledgerEffectiveTime),
      signatories = exSignatories,
      stakeholders = stakeholders,
      keyOpt = key,
    )

  private val activeContract: Future[Option[ExistingContractStatus]] =
    Future.successful(Some(ContractStateStatus.Active))

  private val archivedContract: Future[Option[ExistingContractStatus]] =
    Future.successful(Some(ContractStateStatus.Archived))

  private def party(name: String): Party = Party.assertFromString(name)

  private def globalKey(desc: String): Key =
    Key.assertBuild(
      Identifier.assertFromString("some:template:name"),
      ValueText(desc),
      Ref.PackageName.assertFromString("pkg-name"),
    )
}
