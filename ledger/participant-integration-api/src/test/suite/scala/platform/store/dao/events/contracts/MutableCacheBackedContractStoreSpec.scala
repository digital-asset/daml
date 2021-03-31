// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events.contracts

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{BoundedSourceQueue, Materializer}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractInst, ValueText, VersionedValue}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.ContractsStateCache.ContractStateValue.{Active, Archived}
import com.daml.platform.store.cache.KeyStateCache.KeyStateValue.{Assigned, Unassigned}
import com.daml.platform.store.dao.events._
import com.daml.platform.store.dao.events.contracts.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyUnassigned,
}
import com.daml.platform.store.dao.events.contracts.MutableCacheBackedContractStore.{
  ContractNotFound,
  EmptyContractIds,
  EventSequentialId,
}
import com.daml.platform.store.dao.events.contracts.MutableCacheBackedContractStoreSpec._
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.math.BigInt.long2bigInt

class MutableCacheBackedContractStoreSpec
    extends AsyncWordSpec
    with Matchers
    with Eventually
    with MockitoSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "consumeFrom" should {
    "populate the caches from the contract state event stream" in {
      val actorSystem = ActorSystem("test")
      implicit val materializer: Materializer = Materializer(actorSystem)

      val lastLedgerHead = new AtomicReference[Offset](Offset.beforeBegin)
      val capture_signalLedgerHead = lastLedgerHead.set _
      val store = contractStore(cachesSize = 2L, ContractsReaderFixture(), capture_signalLedgerHead)

      implicit val eventsStreamQueue: BoundedSourceQueue[ContractStateEvent] = Source
        .queue[ContractStateEvent](16)
        .via(store.consumeFrom)
        .toMat(Sink.ignore)(Keep.left)
        .run()

      for {
        c1 <- createdEvent(cId_1, contract1, Some(someKey), Set(charlie), 1L, t1)
        _ <- eventually {
          store.contractsCache.get(cId_1) shouldBe Some(Active(contract1, Set(charlie), t1))
          store.keyCache.get(someKey) shouldBe Some(Assigned(cId_1, Set(charlie)))
          store.cacheOffset.get() shouldBe 1L
        }
        _ <- archivedEvent(c1, eventSequentialId = 2L)
        _ <- eventually {
          store.contractsCache.get(cId_1) shouldBe Some(Archived(Set(charlie)))
          store.keyCache.get(someKey) shouldBe Some(Unassigned)
          store.cacheOffset.get() shouldBe 2L
        }
        offset1 = Offset.fromByteArray(1337.toByteArray)
        _ <- ledgerEnd(offset1, 3L)
        _ <- eventually {
          store.cacheOffset.get() shouldBe 3L
          lastLedgerHead.get() shouldBe offset1
        }
      } yield succeed
    }
  }

  "lookupActiveContract" should {
    "read-through the contract state cache" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      val store = contractStore(cachesSize = 1L, spyContractsReader)
      store.cacheOffset.set(1L)

      for {
        cId2_lookup <- store.lookupActiveContract(Set(alice), cId_2)
        another_cId2_lookup <- store.lookupActiveContract(Set(alice), cId_2)
        _ = store.cacheOffset.incrementAndGet()
        cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)
        another_cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)
        _ = store.cacheOffset.incrementAndGet()
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

    "present the contract state if visible at specific cache offsets (with no cache)" in {
      val store = contractStore(cachesSize = 0L)

      for {
        cId1_lookup0 <- store.lookupActiveContract(Set(alice), cId_1)
        cId2_lookup0 <- store.lookupActiveContract(Set(bob), cId_2)
        _ = store.cacheOffset.incrementAndGet()
        cId1_lookup1 <- store.lookupActiveContract(Set(alice), cId_1)
        cid1_lookup1_archivalNotDivulged <- store.lookupActiveContract(Set(charlie), cId_1)
        _ = store.cacheOffset.incrementAndGet()
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
      val store = contractStore(cachesSize = 1L, spyContractsReader)

      for {
        assigned_firstLookup <- store.lookupContractKey(Set(alice), someKey)
        assigned_secondLookup <- store.lookupContractKey(Set(alice), someKey)
        unassignedKey = globalKey("unassigned")
        _ = store.cacheOffset.incrementAndGet()
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
      val store = contractStore(cachesSize = 0L)
      for {
        key_lookup0 <- store.lookupContractKey(Set(alice), someKey)
        _ = store.cacheOffset.incrementAndGet()
        key_lookup1 <- store.lookupContractKey(Set(alice), someKey)
        _ = store.cacheOffset.incrementAndGet()
        key_lookup2 <- store.lookupContractKey(Set(bob), someKey)
        key_lookup2_notVisible <- store.lookupContractKey(Set(charlie), someKey)
        _ = store.cacheOffset.incrementAndGet()
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
      val store = contractStore(cachesSize = 2L)
      store.cacheOffset.set(2L)
      for {
        // populate the cache
        _ <- store.lookupActiveContract(Set(bob), cId_2)
        _ <- store.lookupActiveContract(Set(bob), cId_3)
        maxLedgerTime <- store.lookupMaximumLedgerTime(Set(cId_2, cId_3, cId_4))
      } yield {
        maxLedgerTime shouldBe Some(t4)
      }
    }

    "fail if one of the contract ids doesn't have an associated active contract" in {
      val store = contractStore(cachesSize = 0L)
      store.cacheOffset.set(2L)
      recoverToSucceededIf[ContractNotFound] {
        for {
          _ <- store.lookupMaximumLedgerTime(Set(cId_1, cId_2))
        } yield succeed
      }
    }

    "fail if the requested contract id set is empty" in {
      val store = contractStore(cachesSize = 0L)
      recoverToSucceededIf[EmptyContractIds] {
        for {
          _ <- store.lookupMaximumLedgerTime(Set.empty)
        } yield succeed
      }
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
      createLedgerEffectiveTime = createLedgerEffectiveTime,
      globalKey = maybeKey,
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
}

object MutableCacheBackedContractStoreSpec {
  private val Seq(alice, bob, charlie) = Seq("alice", "bob", "charlie").map(party)
  private val (
    Seq(cId_1, cId_2, cId_3, cId_4),
    Seq(contract1, contract2, contract3, contract4),
    Seq(t1, t2, t3, t4),
  ) =
    (1 to 4).map { id =>
      (contractId(id), contract(s"id$id"), Instant.ofEpochSecond(id.toLong))
    }.unzip3

  private val someKey = globalKey("key1")

  private def contractStore(
      cachesSize: Long,
      readerFixture: LedgerDaoContractsReader = ContractsReaderFixture(),
      signalNewLedgerHead: Offset => Unit = _ => (),
  ) =
    MutableCacheBackedContractStore(
      readerFixture,
      signalNewLedgerHead,
      new Metrics(new MetricRegistry),
      contractsCacheSize = cachesSize,
      keyCacheSize = cachesSize,
    )(scala.concurrent.ExecutionContext.global)

  case class ContractsReaderFixture() extends LedgerDaoContractsReader {
    override def lookupKeyState(key: Key, validAt: Long)(implicit
        loggingContext: LoggingContext
    ): Future[LedgerDaoContractsReader.KeyState] = (key, validAt) match {
      case (`someKey`, 0L) => Future.successful(KeyAssigned(cId_1, Set(alice)))
      case (`someKey`, 2L) => Future.successful(KeyAssigned(cId_2, Set(bob)))
      case _ => Future.successful(KeyUnassigned)
    }

    override def lookupContractState(contractId: ContractId, validAt: Long)(implicit
        loggingContext: LoggingContext
    ): Future[Option[LedgerDaoContractsReader.ContractState]] = (contractId, validAt) match {
      case (`cId_1`, 0L) => activeContract(contract1, Set(alice), t1)
      case (`cId_1`, validAt) if validAt > 0L => archivedContract(Set(alice))
      case (`cId_2`, validAt) if validAt >= 1L => activeContract(contract2, Set(bob), t2)
      case (`cId_3`, _) => activeContract(contract3, Set(bob), t3)
      case (`cId_4`, _) => activeContract(contract4, Set(bob), t4)
      case _ => Future.successful(Option.empty)
    }

    override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
        loggingContext: LoggingContext
    ): Future[Option[Instant]] = ids match {
      case setIds if setIds == Set(cId_4) =>
        Future.successful(Some(t4))
      case set if set.isEmpty =>
        Future.failed(EmptyContractIds())
      case _ => Future.failed(ContractNotFound(ids))
    }

    override def lookupActiveContractAndLoadArgument(
        contractId: ContractId,
        forParties: Set[Party],
    )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
      (contractId, forParties) match {
        case (`cId_2`, parties) if parties.contains(charlie) =>
          // Purposely return a wrong associated contract than the cId so it can be distinguished upstream
          // that the cached variant of this method was not used
          Future.successful(Some(contract3))
        case (`cId_1`, parties) if parties.contains(charlie) =>
          Future.successful(Some(contract1))
        case _ => Future.successful(Option.empty)
      }

    override def lookupActiveContractWithCachedArgument(
        contractId: ContractId,
        forParties: Set[Party],
        createArgument: VersionedValue[ContractId],
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
    val contractArgument = Value.ValueRecord(
      Some(templateId),
      ImmArray.empty,
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

  private def globalKey(desc: String): GlobalKey =
    GlobalKey.assertBuild(Identifier.assertFromString(s"some:template:$desc"), ValueText(desc))
}
