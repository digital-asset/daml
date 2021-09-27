// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.scaladsl.Source
import akka.stream.{BoundedSourceQueue, Materializer}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.{ContractInst, ValueRecord, ValueText}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.cache.ContractKeyStateValue.{Assigned, Unassigned}
import com.daml.platform.store.cache.ContractStateValue.{Active, Archived}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.{
  ContractNotFound,
  EmptyContractIds,
  EventSequentialId,
  SubscribeToContractStateEvents,
}
import com.daml.platform.store.cache.MutableCacheBackedContractStoreSpec.{
  ContractsReaderFixture,
  contractStore,
  _,
}
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.{
  ContractState,
  KeyAssigned,
  KeyUnassigned,
}
import org.mockito.MockitoSugar
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.math.BigInt.long2bigInt

class MutableCacheBackedContractStoreSpec
    extends AsyncWordSpec
    with Matchers
    with Eventually
    with MockitoSugar
    with BeforeAndAfterAll {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val actorSystem = ActorSystem("test")
  private implicit val materializer: Materializer = Materializer(actorSystem)

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2000, Millis)), interval = scaled(Span(50, Millis)))

  "cache initialization" should {
    "set the cache index to the initialization index" in {
      val cacheInitializationIndex = Offset.fromByteArray(1337.toByteArray) -> 1337L
      contractStore(cachesSize = 0L, startIndexExclusive = cacheInitializationIndex).asFuture
        .map(_.cacheIndex.get shouldBe cacheInitializationIndex)
    }
  }

  "event stream consumption" should {
    "populate the caches from the contract state event stream" in {

      val lastLedgerHead = new AtomicReference[Offset](Offset.beforeBegin)
      val capture_signalLedgerHead = lastLedgerHead.set _

      implicit val (
        queue: BoundedSourceQueue[ContractStateEvent],
        source: Source[ContractStateEvent, NotUsed],
      ) = Source
        .queue[ContractStateEvent](16)
        .preMaterialize()

      for {
        store <- contractStore(
          cachesSize = 2L,
          ContractsReaderFixture(),
          capture_signalLedgerHead,
          _ => source,
        ).asFuture
        c1 <- createdEvent(cId_1, contract1, Some(someKey), Set(charlie), 1L, t1)
        _ <- eventually {
          store.contractsCache.get(cId_1) shouldBe Some(Active(contract1, Set(charlie), t1))
          store.keyCache.get(someKey) shouldBe Some(Assigned(cId_1, Set(charlie)))
          store.cacheIndex.getEventSequentialId shouldBe 1L
        }

        _ <- archivedEvent(c1, eventSequentialId = 2L)
        _ <- eventually {
          store.contractsCache.get(cId_1) shouldBe Some(Archived(Set(charlie)))
          store.keyCache.get(someKey) shouldBe Some(Unassigned)
          store.cacheIndex.getEventSequentialId shouldBe 2L
        }

        someOffset = Offset.fromByteArray(1337.toByteArray)
        _ <- ledgerEnd(someOffset, 3L)
        _ <- eventually {
          store.cacheIndex.getEventSequentialId shouldBe 3L
          lastLedgerHead.get() shouldBe someOffset
        }
      } yield succeed
    }

    "resubscribe from the last ingested offset (on failure)" in {
      val offsetBase = BigInt(1) << 32

      val offset1 = Offset.beforeBegin
      val offset2 = Offset.fromByteArray((offsetBase + 1L).toByteArray)
      val offset3 = Offset.fromByteArray((offsetBase + 2L).toByteArray)

      val created = ContractStateEvent.Created(
        contractId = cId_1,
        contract = contract1,
        globalKey = Some(someKey),
        ledgerEffectiveTime = t1,
        stakeholders = Set(charlie),
        eventOffset = offset1,
        eventSequentialId = 1L,
      )

      val dummy = created.copy(eventSequentialId = 7L)

      val archived = ContractStateEvent.Archived(
        contractId = created.contractId,
        globalKey = created.globalKey,
        stakeholders = created.stakeholders,
        eventOffset = offset2,
        eventSequentialId = 2L,
      )

      val anotherCreate = ContractStateEvent.Created(
        contractId = cId_2,
        contract = contract2,
        globalKey = Some(someKey),
        ledgerEffectiveTime = t2,
        stakeholders = Set(alice),
        eventOffset = offset3,
        eventSequentialId = 3L,
      )

      val sourceSubscriptionFixture: SubscribeToContractStateEvents = {
        case (Offset.beforeBegin, EventSequentialId.beforeBegin) =>
          Source
            .fromIterator(() => Iterator(created, archived, dummy))
            // Simulate the source failure at the last event
            .map(x => if (x == dummy) throw new RuntimeException("some transient failure") else x)
        case (_, 2L) =>
          Source.fromIterator(() => Iterator(anotherCreate))
        case subscriptionOffset =>
          fail(s"Unexpected subscription offsets $subscriptionOffset")
      }

      for {
        store <- contractStore(
          cachesSize = 2L,
          ContractsReaderFixture(),
          _ => (),
          sourceSubscriptionFixture,
        ).asFuture
        _ <- eventually {
          store.contractsCache.get(cId_1) shouldBe Some(Archived(Set(charlie)))
          store.contractsCache.get(cId_2) shouldBe Some(Active(contract2, Set(alice), t2))
          store.keyCache.get(someKey) shouldBe Some(Assigned(cId_2, Set(alice)))
          store.cacheIndex.get shouldBe offset3 -> 3L
        }
      } yield succeed
    }
  }

  "lookupActiveContract" should {
    "read-through the contract state cache" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      for {
        store <- contractStore(cachesSize = 1L, spyContractsReader).asFuture
        _ = store.cacheIndex.set(unusedOffset, 1L)
        cId2_lookup <- store.lookupActiveContract(Set(alice), cId_2)
        another_cId2_lookup <- store.lookupActiveContract(Set(alice), cId_2)

        _ = store.cacheIndex.set(unusedOffset, 2L)
        cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)
        another_cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)

        _ = store.cacheIndex.set(unusedOffset, 3L)
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

        verify(spyContractsReader).lookupContractState(cId_2, 1L)
        verify(spyContractsReader).lookupContractState(cId_3, 2L)
        verify(spyContractsReader).lookupContractState(nonExistentCId, 3L)
        succeed
      }
    }

    "read-through the cache without storing negative lookups" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      for {
        store <- contractStore(cachesSize = 1L, spyContractsReader).asFuture
        _ = store.cacheIndex.set(unusedOffset, 1L)
        negativeLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
        positiveLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
      } yield {
        negativeLookup_cId6 shouldBe Option.empty
        positiveLookup_cId6 shouldBe Some(contract6)

        verify(spyContractsReader, times(wantedNumberOfInvocations = 2))
          .lookupContractState(cId_6, 1L)
        succeed
      }
    }

    "resort to resolveDivulgenceLookup on not found" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      for {
        store <- contractStore(cachesSize = 1L, spyContractsReader).asFuture
        _ = store.cacheIndex.set(unusedOffset, 1L)
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

        _ = store.cacheIndex.set(unusedOffset, 1L)
        cId1_lookup1 <- store.lookupActiveContract(Set(alice), cId_1)
        cid1_lookup1_archivalNotDivulged <- store.lookupActiveContract(Set(charlie), cId_1)

        _ = store.cacheIndex.set(unusedOffset, 2L)
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

        _ = store.cacheIndex.set(unusedOffset, 1L)
        unassigned_firstLookup <- store.lookupContractKey(Set(alice), unassignedKey)
        unassigned_secondLookup <- store.lookupContractKey(Set(alice), unassignedKey)
      } yield {
        assigned_firstLookup shouldBe Some(cId_1)
        assigned_secondLookup shouldBe Some(cId_1)

        unassigned_firstLookup shouldBe Option.empty
        unassigned_secondLookup shouldBe Option.empty

        verify(spyContractsReader).lookupKeyState(someKey, 0L)(loggingContext)
        verify(spyContractsReader).lookupKeyState(unassignedKey, 1L)(loggingContext)
        verifyNoMoreInteractions(spyContractsReader)
        succeed
      }
    }

    "present the key state if visible at specific cache offsets (with no cache)" in {
      for {
        store <- contractStore(cachesSize = 0L).asFuture
        key_lookup0 <- store.lookupContractKey(Set(alice), someKey)

        _ = store.cacheIndex.set(unusedOffset, 1L)
        key_lookup1 <- store.lookupContractKey(Set(alice), someKey)

        _ = store.cacheIndex.set(unusedOffset, 2L)
        key_lookup2 <- store.lookupContractKey(Set(bob), someKey)
        key_lookup2_notVisible <- store.lookupContractKey(Set(charlie), someKey)

        _ = store.cacheIndex.set(unusedOffset, 3L)
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
        _ = store.cacheIndex.set(unusedOffset, 2L)
        // populate the cache
        _ <- store.lookupActiveContract(Set(bob), cId_2)
        _ <- store.lookupActiveContract(Set(bob), cId_3)
        maxLedgerTime <- store.lookupMaximumLedgerTime(Set(cId_2, cId_3, cId_4))
      } yield {
        maxLedgerTime shouldBe Some(t4)
      }
    }

    "fail if one of the cached contract ids doesn't have an associated active contract" in {
      for {
        store <- contractStore(cachesSize = 1L).asFuture
        _ = store.cacheIndex.set(unusedOffset, 2L)
        // populate the cache
        _ <- store.lookupActiveContract(Set(bob), cId_5)
        assertion <- recoverToSucceededIf[ContractNotFound](
          store.lookupMaximumLedgerTime(Set(cId_1, cId_5))
        )
      } yield assertion
    }

    "fail if one of the fetched contract ids doesn't have an associated active contract" in {
      for {
        store <- contractStore(cachesSize = 0L).asFuture
        _ = store.cacheIndex.set(unusedOffset, 2L)
        assertion <- recoverToSucceededIf[IllegalArgumentException](
          store.lookupMaximumLedgerTime(Set(cId_1, cId_5))
        )
      } yield assertion
    }

    "fail if the requested contract id set is empty" in {
      for {
        store <- contractStore(cachesSize = 0L).asFuture
        _ <- recoverToSucceededIf[EmptyContractIds](store.lookupMaximumLedgerTime(Set.empty))
      } yield succeed
    }
  }

  private def createdEvent(
      contractId: ContractId,
      contract: Contract,
      maybeKey: Option[GlobalKey],
      stakeholders: Set[Party],
      eventSequentialId: EventSequentialId,
      createLedgerEffectiveTime: Instant,
  )(implicit queue: BoundedSourceQueue[ContractStateEvent]): Future[ContractStateEvent.Created] = {
    val created = ContractStateEvent.Created(
      contractId = contractId,
      contract = contract,
      globalKey = maybeKey,
      ledgerEffectiveTime = createLedgerEffectiveTime,
      stakeholders = stakeholders,
      eventOffset = Offset.beforeBegin, // Not used
      eventSequentialId = eventSequentialId,
    )
    Future.successful {
      queue.offer(created) shouldBe Enqueued
      created
    }
  }

  private def archivedEvent(
      created: ContractStateEvent.Created,
      eventSequentialId: EventSequentialId,
  )(implicit queue: BoundedSourceQueue[ContractStateEvent]): Future[Assertion] =
    Future {
      queue.offer(
        ContractStateEvent.Archived(
          contractId = created.contractId,
          globalKey = created.globalKey,
          stakeholders = created.stakeholders,
          eventOffset = Offset.beforeBegin, // Not used
          eventSequentialId = eventSequentialId,
        )
      ) shouldBe Enqueued
    }

  private def ledgerEnd(offset: Offset, eventSequentialId: EventSequentialId)(implicit
      queue: BoundedSourceQueue[ContractStateEvent]
  ) =
    Future {
      queue.offer(ContractStateEvent.LedgerEndMarker(offset, eventSequentialId)) shouldBe Enqueued
    }

  override def afterAll(): Unit = {
    materializer.shutdown()
    val _ = actorSystem.terminate()
  }
}

object MutableCacheBackedContractStoreSpec {
  private val unusedOffset = Offset.beforeBegin
  private val Seq(alice, bob, charlie) = Seq("alice", "bob", "charlie").map(party)
  private val (
    Seq(cId_1, cId_2, cId_3, cId_4, cId_5, cId_6, cId_7),
    Seq(contract1, contract2, contract3, contract4, _, contract6, contract7),
    Seq(t1, t2, t3, t4, _, t6, _),
  ) =
    (1 to 7).map { id =>
      (contractId(id), contract(s"id$id"), Instant.ofEpochSecond(id.toLong))
    }.unzip3

  private val someKey = globalKey("key1")

  private def contractStore(
      cachesSize: Long,
      readerFixture: LedgerDaoContractsReader = ContractsReaderFixture(),
      signalNewLedgerHead: Offset => Unit = _ => (),
      sourceSubscriber: SubscribeToContractStateEvents = _ => Source.empty,
      startIndexExclusive: (Offset, EventSequentialId) =
        Offset.beforeBegin -> EventSequentialId.beforeBegin,
  )(implicit loggingContext: LoggingContext, materializer: Materializer) =
    new MutableCacheBackedContractStore.OwnerWithSubscription(
      subscribeToContractStateEvents = sourceSubscriber,
      startIndexExclusive = startIndexExclusive,
      minBackoffStreamRestart = 10.millis,
      contractsReader = readerFixture,
      signalNewLedgerHead = signalNewLedgerHead,
      metrics = new Metrics(new MetricRegistry),
      maxContractsCacheSize = cachesSize,
      maxKeyCacheSize = cachesSize,
      executionContext = scala.concurrent.ExecutionContext.global,
    )
      .acquire()(ResourceContext(scala.concurrent.ExecutionContext.global))

  case class ContractsReaderFixture() extends LedgerDaoContractsReader {
    @volatile private var initialResultForCid6 = Future.successful(Option.empty[ContractState])

    override def lookupKeyState(key: Key, validAt: Long)(implicit
        loggingContext: LoggingContext
    ): Future[LedgerDaoContractsReader.KeyState] = (key, validAt) match {
      case (`someKey`, 0L) => Future.successful(KeyAssigned(cId_1, Set(alice)))
      case (`someKey`, 2L) => Future.successful(KeyAssigned(cId_2, Set(bob)))
      case _ => Future.successful(KeyUnassigned)
    }

    override def lookupContractState(contractId: ContractId, validAt: Long)(implicit
        loggingContext: LoggingContext
    ): Future[Option[LedgerDaoContractsReader.ContractState]] = {
      (contractId, validAt) match {
        case (`cId_1`, 0L) => activeContract(contract1, Set(alice), t1)
        case (`cId_1`, validAt) if validAt > 0L => archivedContract(Set(alice))
        case (`cId_2`, validAt) if validAt >= 1L => activeContract(contract2, Set(bob), t2)
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

    override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
        loggingContext: LoggingContext
    ): Future[Option[Instant]] = ids match {
      case setIds if setIds == Set(cId_4) =>
        Future.successful(Some(t4))
      case set if set.isEmpty =>
        Future.failed(EmptyContractIds())
      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"The following contracts have not been found: ${ids.map(_.coid).mkString(", ")}"
          )
        )
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

    override def lookupContractKey(key: Key, forParties: Set[Party])(implicit
        loggingContext: LoggingContext
    ): Future[Option[ContractId]] = throw new RuntimeException("This method should not be called")
  }

  private def activeContract(
      contract: Contract,
      parties: Set[Party],
      ledgerEffectiveTime: Instant,
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
    val contractInstance = ContractInst(
      templateId,
      contractArgument,
      "some agreement",
    )
    TransactionBuilder().versionContract(contractInstance)
  }

  private def contractId(id: Int): ContractId =
    ContractId.assertFromString(s"#contract-$id")

  private def globalKey(desc: String): Key =
    GlobalKey.assertBuild(Identifier.assertFromString(s"some:template:$desc"), ValueText(desc))
}
