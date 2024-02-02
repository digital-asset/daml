// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.ledger.resources.Resource
import com.daml.lf.crypto.Hash
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.IdString
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{GlobalKey, TransactionVersion, Versioned}
import com.daml.lf.value.Value.{ContractInstance, ValueRecord, ValueText}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.ContractState
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStoreSpec.*
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyUnassigned,
}
import com.digitalasset.canton.{HasExecutionContext, TestEssentials}
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
        metrics = Metrics.ForTesting,
        contractsReader = mock[LedgerDaoContractsReader],
        contractStateCaches = contractStateCaches,
        loggerFactory = loggerFactory,
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

      contractStore.contractStateCaches.push(updateBatch)
      verify(contractStateCaches).push(updateBatch)

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

        // The cache is evicted BOTH on the number of entries AND memory pressure
        // So even though a read-through populates missing entries,
        // they can be immediately evicted by GCs and lead to subsequent misses.
        // Hence, verify atLeastOnce for LedgerDaoContractsReader.lookupContractState
        verify(spyContractsReader, atLeastOnce).lookupContractState(cId_2, offset1)
        verify(spyContractsReader, atLeastOnce).lookupContractState(cId_3, offset2)
        verify(spyContractsReader, atLeastOnce).lookupContractState(nonExistentCId, offset3)
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
        _ = store.contractStateCaches.contractState.cacheIndex = offset1
        negativeLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
        positiveLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
      } yield {
        negativeLookup_cId6 shouldBe Option.empty
        positiveLookup_cId6 shouldBe Option.empty

        verify(spyContractsReader, times(wantedNumberOfInvocations = 1))
          .lookupContractState(cId_6, offset1)
        succeed
      }
    }

    "present the contract state if visible at specific cache offsets (with no cache)" in {
      for {
        store <- contractStore(cachesSize = 0L, loggerFactory).asFuture
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
        cid1_lookup1_archivalNotDivulged shouldBe None

        cId2_lookup2 shouldBe Some(contract2)
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
        store <- contractStore(cachesSize = 0L, loggerFactory).asFuture
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

  "lookupContractStateWithoutDivulgence" should {

    val stateValueActive = ContractStateValue.Active(
      contract = contract4,
      createLedgerEffectiveTime = t4,
      stakeholders = exStakeholders,
      agreementText = exAgreementText,
      signatories = exSignatories,
      globalKey = Some(someKey),
      keyMaintainers = exMaintainers,
      driverMetadata = exDriverMetadata,
    )

    val stateActive = ContractState.Active(
      contractInstance = contract4,
      ledgerEffectiveTime = t4,
      stakeholders = exStakeholders,
      agreementText = exAgreementText,
      signatories = exSignatories,
      globalKey = Some(someKey),
      maintainers = exMaintainers,
      driverMetadata = exDriverMetadata,
    )

    "resolve lookup from cache" in {
      for {
        store <- contractStore(cachesSize = 2L, loggerFactory).asFuture
        _ = store.contractStateCaches.contractState.putBatch(
          offset2,
          Map(
            // Populate the cache with an active contract
            cId_4 -> stateValueActive,
            // Populate the cache with an archived contract
            cId_5 -> ContractStateValue.Archived(Set.empty),
          ),
        )
        activeContractLookupResult <- store.lookupContractState(cId_4)
        archivedContractLookupResult <- store.lookupContractState(cId_5)
        nonExistentContractLookupResult <- store.lookupContractState(cId_7)
      } yield {
        activeContractLookupResult shouldBe stateActive
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
        activeContractLookupResult shouldBe stateActive
        archivedContractLookupResult shouldBe ContractState.Archived
        nonExistentContractLookupResult shouldBe ContractState.NotFound
      }
    }
  }
}

@nowarn("msg=match may not be exhaustive")
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

  private val exAgreementText = Some("agreement")
  private val exStakeholders = Set(bob)
  private val exSignatories = Set(alice)
  private val exMaintainers = Some(Set(bob))
  private val exDriverMetadata = Some("meta".getBytes)

  private def contractStore(
      cachesSize: Long,
      loggerFactory: NamedLoggerFactory,
      readerFixture: LedgerDaoContractsReader = ContractsReaderFixture(),
  )(implicit ec: ExecutionContext) = {
    val metrics = Metrics.ForTesting
    val startIndexExclusive: Offset = offset0
    val contractStore = new MutableCacheBackedContractStore(
      metrics,
      readerFixture,
      contractStateCaches = ContractStateCaches
        .build(startIndexExclusive, cachesSize, cachesSize, metrics, loggerFactory),
      loggerFactory = loggerFactory,
    )

    Resource.successful(contractStore)
  }

  @SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is spied in tests
  case class ContractsReaderFixture() extends LedgerDaoContractsReader {
    @volatile private var initialResultForCid6 =
      Future.successful(Option.empty[LedgerDaoContractsReader.ContractState])

    override def lookupKeyState(key: Key, validAt: Offset)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[LedgerDaoContractsReader.KeyState] = (key, validAt) match {
      case (`someKey`, `offset0`) => Future.successful(KeyAssigned(cId_1, Set(alice)))
      case (`someKey`, `offset2`) => Future.successful(KeyAssigned(cId_2, Set(bob)))
      case _ => Future.successful(KeyUnassigned)
    }

    override def lookupContractState(contractId: ContractId, validAt: Offset)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[LedgerDaoContractsReader.ContractState]] = {
      (contractId, validAt) match {
        case (`cId_1`, `offset0`) => activeContract(contract1, Set(alice), t1)
        case (`cId_1`, validAt) if validAt > offset0 => archivedContract(Set(alice))
        case (`cId_2`, validAt) if validAt >= offset1 =>
          activeContract(contract2, exStakeholders, t2)
        case (`cId_3`, _) => activeContract(contract3, exStakeholders, t3)
        case (`cId_4`, _) => activeContract(contract4, exStakeholders, t4)
        case (`cId_5`, _) => archivedContract(Set(bob))
        case (`cId_6`, _) =>
          // Simulate store being populated from one query to another
          val result = initialResultForCid6
          initialResultForCid6 = activeContract(contract6, Set(alice), t6)
          result
        case _ => Future.successful(Option.empty)
      }
    }
  }

  private def activeContract(
      contract: Contract,
      stakeholders: Set[Party],
      ledgerEffectiveTime: Timestamp,
      agreementText: Option[String] = exAgreementText,
      signatories: Set[Party] = exSignatories,
      globalKey: Option[GlobalKey] = Some(someKey),
      maintainers: Option[Set[Party]] = exMaintainers,
      driverMetadata: Option[Array[Byte]] = exDriverMetadata,
  ): Future[Option[LedgerDaoContractsReader.ActiveContract]] =
    Future.successful(
      Some(
        LedgerDaoContractsReader.ActiveContract(
          contract = contract,
          stakeholders = stakeholders,
          ledgerEffectiveTime = ledgerEffectiveTime,
          agreementText = agreementText,
          signatories = signatories,
          globalKey = globalKey,
          keyMaintainers = maintainers,
          driverMetadata = driverMetadata,
        )
      )
    )

  private def archivedContract(
      parties: Set[Party]
  ): Future[Option[LedgerDaoContractsReader.ArchivedContract]] =
    Future.successful(Some(LedgerDaoContractsReader.ArchivedContract(parties)))

  private def party(name: String): IdString.Party = Party.assertFromString(name)

  private def contract(templateName: String): Contract = {
    val templateId = Identifier.assertFromString(s"some:template:$templateName")
    val contractArgument = ValueRecord(
      Some(templateId),
      ImmArray.Empty,
    )
    val contractInstance = ContractInstance(template = templateId, arg = contractArgument)
    Versioned(TransactionVersion.StableVersions.max, contractInstance)
  }

  private def contractId(id: Int): ContractId =
    ContractId.V1(Hash.hashPrivateKey(id.toString))

  private def globalKey(desc: String): Key =
    GlobalKey.assertBuild(ValueText(desc))

  private def offset(idx: Long) = Offset.fromByteArray(BigInt(idx).toByteArray)
}
