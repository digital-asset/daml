// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.data.NonEmptyVector
import com.daml.ledger.resources.Resource
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.ContractState
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStoreSpec.*
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.{HasExecutionContext, TestEssentials}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.transaction.{CreationTime, Node, Versioned}
import com.digitalasset.daml.lf.value.Value.{ValueInt64, ValueRecord, ValueText}
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
      )

      val event1 = ContractStateEvent.Archived(
        contractId = ContractId.V1(Hash.hashPrivateKey("cid")),
        globalKey = None,
        stakeholders = Set.empty,
        eventOffset = Offset.firstOffset,
      )
      val event2 = event1
      val updateBatch = NonEmptyVector.of(event1, event2)

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
        _ = store.contractStateCaches.contractState.cacheIndex = Some(offset1)
        cId2_lookup <- store.lookupActiveContract(Set(charlie), cId_2)
        another_cId2_lookup <- store.lookupActiveContract(Set(charlie), cId_2)

        _ = store.contractStateCaches.contractState.cacheIndex = Some(offset2)
        cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)
        another_cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)

        _ = store.contractStateCaches.contractState.cacheIndex = Some(offset3)
        nonExistentCId = contractId(5)
        nonExistentCId_lookup <- store.lookupActiveContract(Set.empty, nonExistentCId)
        another_nonExistentCId_lookup <- store.lookupActiveContract(Set.empty, nonExistentCId)
      } yield {
        cId2_lookup shouldBe Option.empty
        another_cId2_lookup shouldBe Option.empty

        cId3_lookup.map(_.templateId) shouldBe Some(contract3.unversioned.template)
        another_cId3_lookup.map(_.templateId) shouldBe Some(contract3.unversioned.template)

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
        _ = store.contractStateCaches.contractState.cacheIndex = Some(offset1)
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

        _ = store.contractStateCaches.contractState.cacheIndex = Some(offset1)
        cId1_lookup1 <- store.lookupActiveContract(Set(alice), cId_1)
        cid1_lookup1_archivalNotDivulged <- store.lookupActiveContract(Set(charlie), cId_1)

        _ = store.contractStateCaches.contractState.cacheIndex = Some(offset2)
        cId2_lookup2 <- store.lookupActiveContract(Set(bob), cId_2)
        cid2_lookup2_divulged <- store.lookupActiveContract(Set(charlie), cId_2)
        cid2_lookup2_nonVisible <- store.lookupActiveContract(Set(charlie), cId_2)
      } yield {
        cId1_lookup0.map(_.templateId) shouldBe Some(contract1.unversioned.template)
        cId2_lookup0 shouldBe Option.empty

        cId1_lookup1 shouldBe Option.empty
        cid1_lookup1_archivalNotDivulged shouldBe None

        cId2_lookup2.map(_.templateId) shouldBe Some(contract2.unversioned.template)
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

        _ = store.contractStateCaches.keyState.cacheIndex = Some(offset1)
        unassigned_firstLookup <- store.lookupContractKey(Set(alice), unassignedKey)
        unassigned_secondLookup <- store.lookupContractKey(Set(alice), unassignedKey)
      } yield {
        assigned_firstLookup shouldBe Some(cId_1)
        assigned_secondLookup shouldBe Some(cId_1)

        unassigned_firstLookup shouldBe Option.empty
        unassigned_secondLookup shouldBe Option.empty

        verify(spyContractsReader).lookupKeyState(someKey, offset0)(loggingContext)
        // looking up the key state will not prefetch the contract state
        verify(spyContractsReader).lookupKeyState(unassignedKey, offset1)(loggingContext)
        verifyNoMoreInteractions(spyContractsReader)
        succeed
      }
    }

    "present the key state if visible at specific cache offsets (with no cache)" in {
      for {
        store <- contractStore(cachesSize = 0L, loggerFactory).asFuture
        key_lookup0 <- store.lookupContractKey(Set(alice), someKey)

        _ = store.contractStateCaches.keyState.cacheIndex = Some(offset1)
        key_lookup1 <- store.lookupContractKey(Set(alice), someKey)

        _ = store.contractStateCaches.keyState.cacheIndex = Some(offset2)
        key_lookup2 <- store.lookupContractKey(Set(bob), someKey)
        key_lookup2_notVisible <- store.lookupContractKey(Set(charlie), someKey)

        _ = store.contractStateCaches.keyState.cacheIndex = Some(offset3)
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

    lazy val contract = fatContract(
      contractId = cId_4,
      thinContract = contract4,
      createLedgerEffectiveTime = t4,
      stakeholders = exStakeholders,
      signatories = exSignatories,
      key = Some(KeyWithMaintainers(someKey, exMaintainers)),
      authenticationData = exAuthenticationData,
    )

    val stateValueActive = ContractStateValue.Active(contract)
    val stateActive = ContractState.Active(contract)

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
  private val offset0 = Offset.tryFromLong(1L)
  private val offset1 = Offset.tryFromLong(2L)
  private val offset2 = Offset.tryFromLong(3L)
  private val offset3 = Offset.tryFromLong(4L)

  private val Seq(alice, bob, charlie) = Seq("alice", "bob", "charlie").map(party)
  private val (
    Seq(cId_1, cId_2, cId_3, cId_4, cId_5, cId_6, cId_7),
    Seq(contract1, contract2, contract3, contract4, _, contract6, _),
    Seq(t1, t2, t3, t4, _, t6, _),
  ) =
    (1 to 7).map { id =>
      (contractId(id), thinContract(id), Time.Timestamp.assertFromLong(id.toLong * 1000L))
    }.unzip3

  private val someKey = globalKey("key1")

  private val exStakeholders = Set(bob, alice)
  private val exSignatories = Set(alice)
  private val exMaintainers = Set(alice)
  private val exAuthenticationData = Bytes.fromByteArray("meta".getBytes)
  private val someKeyWithMaintainers = KeyWithMaintainers(someKey, exMaintainers)

  private def contractStore(
      cachesSize: Long,
      loggerFactory: NamedLoggerFactory,
      readerFixture: LedgerDaoContractsReader = ContractsReaderFixture(),
  )(implicit ec: ExecutionContext) = {
    val metrics = LedgerApiServerMetrics.ForTesting
    val startIndexExclusive = Some(offset0)
    val contractStore = new MutableCacheBackedContractStore(
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

    override def lookupKeyStatesFromDb(keys: Seq[Key], notEarlierThanOffset: Offset)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Map[Key, KeyState]] = ??? // not used in this test

    override def lookupContractState(contractId: ContractId, validAt: Offset)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[LedgerDaoContractsReader.ContractState]] =
      (contractId, validAt) match {
        case (`cId_1`, `offset0`) => activeContract(cId_1, contract1, Set(alice), t1)
        case (`cId_1`, validAt) if validAt > offset0 => archivedContract(Set(alice))
        case (`cId_2`, validAt) if validAt >= offset1 =>
          activeContract(cId_2, contract2, exStakeholders, t2)
        case (`cId_3`, _) => activeContract(cId_3, contract3, exStakeholders, t3)
        case (`cId_4`, _) => activeContract(cId_4, contract4, exStakeholders, t4)
        case (`cId_5`, _) => archivedContract(Set(bob))
        case (`cId_6`, _) =>
          // Simulate store being populated from one query to another
          val result = initialResultForCid6
          initialResultForCid6 = activeContract(cId_6, contract6, Set(alice), t6)
          result
        case _ => Future.successful(Option.empty)
      }
  }

  private def activeContract(
      contractId: ContractId,
      contract: ThinContract,
      stakeholders: Set[Party],
      ledgerEffectiveTime: Time.Timestamp,
      signatories: Set[Party] = exSignatories,
      key: Option[KeyWithMaintainers] = Some(someKeyWithMaintainers),
      authenticationData: Bytes = exAuthenticationData,
  ): Future[Option[LedgerDaoContractsReader.ActiveContract]] =
    Future.successful(
      Some(
        LedgerDaoContractsReader.ActiveContract(
          contract = fatContract(
            contractId = contractId,
            thinContract = contract,
            createLedgerEffectiveTime = ledgerEffectiveTime,
            stakeholders = stakeholders,
            signatories = signatories,
            key = key,
            authenticationData = authenticationData,
          )
        )
      )
    )

  private def archivedContract(
      parties: Set[Party]
  ): Future[Option[LedgerDaoContractsReader.ArchivedContract]] =
    Future.successful(Some(LedgerDaoContractsReader.ArchivedContract(parties)))

  private def party(name: String): Party = Party.assertFromString(name)

  private def thinContract(idx: Int): ThinContract = {
    val templateId = Identifier.assertFromString("some:template:name")
    val packageName = Ref.PackageName.assertFromString("pkg-name")

    val contractArgument = ValueRecord(
      Some(templateId),
      ImmArray(None -> ValueInt64(idx.toLong)),
    )
    ThinContract(
      packageName = packageName,
      template = templateId,
      arg = Versioned(LanguageMajorVersion.V2.maxStableVersion, contractArgument),
    )
  }

  private def fatContract(
      contractId: ContractId,
      thinContract: ThinContract,
      createLedgerEffectiveTime: Time.Timestamp,
      stakeholders: Set[Party],
      signatories: Set[Party],
      key: Option[KeyWithMaintainers],
      authenticationData: Bytes,
  ) =
    FatContract.fromCreateNode(
      Node.Create(
        coid = contractId,
        packageName = thinContract.unversioned.packageName,
        templateId = thinContract.unversioned.template,
        arg = thinContract.unversioned.arg,
        signatories = signatories,
        stakeholders = stakeholders,
        keyOpt = key,
        version = thinContract.version,
      ),
      createTime = CreationTime.CreatedAt(createLedgerEffectiveTime),
      cantonData = authenticationData,
    )

  private def contractId(id: Int): ContractId =
    ContractId.V1(Hash.hashPrivateKey(id.toString))

  private def globalKey(desc: String): Key =
    Key.assertBuild(
      Identifier.assertFromString("some:template:name"),
      ValueText(desc),
      Ref.PackageName.assertFromString("pkg-name"),
    )
}
