// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.scaladsl.Source
import akka.stream.{BoundedSourceQueue, Materializer}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MaximumLedgerTime
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.crypto.Hash
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.{ContractInstance, ValueRecord, ValueText}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.cache.ContractKeyStateValue.{Assigned, Unassigned}
import com.daml.platform.store.cache.ContractStateValue.{Active, Archived}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.{
  EventSequentialId,
  SignalNewLedgerHead,
  SubscribeToContractStateEvents,
}
import com.daml.platform.store.cache.MutableCacheBackedContractStoreSpec.{
  ContractsReaderFixture,
  contractStore,
  _,
}
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

// TODO LLP: Extract unit test for [[com.daml.platform.store.cache.ContractStateCaches]] in own unit test
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
      val cacheInitializationOffset = offset(1337L)
      contractStore(cachesSize = 0L, startIndexExclusive = cacheInitializationOffset).asFuture
        .map { mutableCacheBackedContractStore =>
          mutableCacheBackedContractStore.contractStateCaches.keyState.cacheIndex shouldBe cacheInitializationOffset
          mutableCacheBackedContractStore.contractStateCaches.contractState.cacheIndex shouldBe cacheInitializationOffset
        }
    }
  }

  "event stream consumption" should {
    "populate the caches from the contract state event stream" in {
      val lastLedgerHead =
        new AtomicReference[(Offset, EventSequentialId)]((offset0, EventSequentialId.beforeBegin))
      val capture_signalLedgerHead: SignalNewLedgerHead =
        (offset, seqId) => lastLedgerHead.set((offset, seqId))

      implicit val (
        queue: BoundedSourceQueue[TransactionEvents],
        source: Source[TransactionEvents, NotUsed],
      ) = Source
        .queue[TransactionEvents](16)
        .preMaterialize()

      for {
        store <- contractStore(
          cachesSize = 2L,
          ContractsReaderFixture(),
          capture_signalLedgerHead,
          () => source,
        ).asFuture
        created1 <- createdEvent(cId_1, contract1, Some(someKey), Set(charlie), offset1, t1)
        _ <- eventually {
          store.contractStateCaches.contractState.get(cId_1) shouldBe Some(
            Active(contract1, Set(charlie), t1)
          )
          store.contractStateCaches.keyState.get(someKey) shouldBe Some(
            Assigned(cId_1, Set(charlie))
          )

          store.contractStateCaches.contractState.cacheIndex shouldBe offset1
          store.contractStateCaches.keyState.cacheIndex shouldBe offset1
        }

        created2 = ContractStateEvent.Created(
          contractId = cId_2,
          contract = contract2,
          globalKey = Some(someKey),
          ledgerEffectiveTime = t3,
          stakeholders = Set(alice),
          eventOffset = offset3,
          eventSequentialId = 0L, // Not used
        )

        _ <- batchUpdate(
          ContractStateEvent.Archived(
            contractId = created1.contractId,
            globalKey = created1.globalKey,
            stakeholders = created1.stakeholders,
            eventOffset = offset2,
            eventSequentialId = 0L, // Not used
          ),
          created2,
        )

        _ <- eventually {
          store.contractStateCaches.contractState.get(cId_1) shouldBe Some(Archived(Set(charlie)))
          store.contractStateCaches.contractState.get(cId_2) shouldBe Some(
            Active(contract2, Set(alice), t3)
          )

          store.contractStateCaches.keyState.get(someKey) shouldBe Some(Assigned(cId_2, Set(alice)))

          store.contractStateCaches.contractState.cacheIndex shouldBe offset3
          store.contractStateCaches.keyState.cacheIndex shouldBe offset3
        }

        _ <- archivedEvent(created2, offset4)

        _ <- eventually {
          store.contractStateCaches.contractState.get(cId_2) shouldBe Some(Archived(Set(alice)))
          store.contractStateCaches.keyState.get(someKey) shouldBe Some(Unassigned)

          store.contractStateCaches.contractState.cacheIndex shouldBe offset4
          store.contractStateCaches.keyState.cacheIndex shouldBe offset4
        }

        someOffset = Offset.fromByteArray(1337.toByteArray)
        _ <- ledgerEnd(someOffset, 3L)
        _ <- eventually {
          lastLedgerHead.get() shouldBe (someOffset -> 3L)
        }
      } yield succeed
    }

    "resubscribe from the last ingested offset (on failure)" in {
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
        @volatile var firstSource = true
        () =>
          if (firstSource) {
            firstSource = false
            Source
              .fromIterator(() => Iterator(created, archived, dummy).map(Vector(_)))
              // Simulate the source failure at the last event
              .map(x => if (x == dummy) throw new RuntimeException("some transient failure") else x)
          } else {
            Source.fromIterator(() => Iterator(Vector(anotherCreate)))
          }
      }

      for {
        store <- contractStore(
          cachesSize = 2L,
          ContractsReaderFixture(),
          (_, _) => (),
          sourceSubscriptionFixture,
        ).asFuture
        _ <- eventually {
          store.contractStateCaches.contractState.get(cId_1) shouldBe Some(Archived(Set(charlie)))
          store.contractStateCaches.contractState.get(cId_2) shouldBe Some(
            Active(contract2, Set(alice), t2)
          )
          store.contractStateCaches.keyState.get(someKey) shouldBe Some(Assigned(cId_2, Set(alice)))
        }
      } yield succeed
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

  private def createdEvent(
      contractId: ContractId,
      contract: Contract,
      maybeKey: Option[GlobalKey],
      stakeholders: Set[Party],
      offset: Offset,
      createLedgerEffectiveTime: Timestamp,
  )(implicit queue: BoundedSourceQueue[TransactionEvents]): Future[ContractStateEvent.Created] = {
    val created = ContractStateEvent.Created(
      contractId = contractId,
      contract = contract,
      globalKey = maybeKey,
      ledgerEffectiveTime = createLedgerEffectiveTime,
      stakeholders = stakeholders,
      eventOffset = offset,
      eventSequentialId = 0L, // Not used
    )
    Future.successful {
      queue.offer(Vector(created)) shouldBe Enqueued
      created
    }
  }

  private def archivedEvent(
      created: ContractStateEvent.Created,
      offset: Offset,
  )(implicit queue: BoundedSourceQueue[TransactionEvents]): Future[Assertion] =
    Future {
      queue.offer(
        Vector(
          ContractStateEvent.Archived(
            contractId = created.contractId,
            globalKey = created.globalKey,
            stakeholders = created.stakeholders,
            eventOffset = offset,
            eventSequentialId = 0L, // Not used
          )
        )
      ) shouldBe Enqueued
    }

  private def batchUpdate(
      batch: ContractStateEvent*
  )(implicit queue: BoundedSourceQueue[TransactionEvents]): Future[Unit] = Future.successful {
    queue.offer(batch.toVector) shouldBe Enqueued
    ()
  }

  private def ledgerEnd(offset: Offset, eventSequentialId: EventSequentialId)(implicit
      queue: BoundedSourceQueue[TransactionEvents]
  ) =
    Future {
      queue.offer(
        Vector(ContractStateEvent.LedgerEndMarker(offset, eventSequentialId))
      ) shouldBe Enqueued
    }

  override def afterAll(): Unit = {
    materializer.shutdown()
    val _ = actorSystem.terminate()
  }
}

object MutableCacheBackedContractStoreSpec {
  private type TransactionEvents = Vector[ContractStateEvent]
  private val offset0 = offset(0L)
  private val offset1 = offset(1L)
  private val offset2 = offset(2L)
  private val offset3 = offset(3L)
  private val offset4 = offset(4L)

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
      signalNewLedgerHead: (Offset, EventSequentialId) => Unit = (_, _) => (),
      sourceSubscriber: SubscribeToContractStateEvents = () => Source.empty,
      startIndexExclusive: Offset = offset0,
  )(implicit loggingContext: LoggingContext, materializer: Materializer) = {
    implicit val resourceContext: ResourceContext = ResourceContext(
      scala.concurrent.ExecutionContext.global
    )

    val metrics = new Metrics(new MetricRegistry)
    val contractStore = new MutableCacheBackedContractStore(
      metrics,
      readerFixture,
      signalNewLedgerHead,
      contractStateCaches =
        ContractStateCaches.build(startIndexExclusive, cachesSize, cachesSize, metrics)(
          scala.concurrent.ExecutionContext.global,
          loggingContext,
        ),
    )(scala.concurrent.ExecutionContext.global, loggingContext)

    new MutableCacheBackedContractStore.CacheUpdateSubscription(
      contractStore = contractStore,
      subscribeToContractStateEvents = sourceSubscriber,
      minBackoffStreamRestart = 10.millis,
    ).acquire()
      .map(_ => contractStore)
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
