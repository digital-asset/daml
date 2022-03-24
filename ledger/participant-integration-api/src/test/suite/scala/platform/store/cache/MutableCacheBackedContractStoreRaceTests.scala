// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.{ContractInstance, ValueInt64, VersionedValue}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.appendonlydao.events.ContractStateEvent
import com.daml.platform.store.cache.EventsBuffer.SearchableByVector
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.cache.MutableCacheBackedContractStoreRaceTests.{
  IndexViewContractsReader,
  assert_sync_vs_async_race_contract,
  assert_sync_vs_async_race_key,
  buildContractStore,
  generateWorkload,
  test,
}
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader._
import org.mockito.MockitoSugar
import org.scalatest.Assertions.fail
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.collection.Searching
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

class MutableCacheBackedContractStoreRaceTests
    extends AsyncFlatSpec
    with Matchers
    with Eventually
    with MockitoSugar
    with BeforeAndAfterAll {
  behavior of "Mutable state cache updates"

  private val unboundedExecutionContext =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  private val actorSystem = ActorSystem()
  private implicit val materializer: Materializer = Materializer(actorSystem)

  it should "preserve causal monotonicity under contention for key state" in {
    val workload = generateWorkload(keysCount = 10L, contractsCount = 1000L)
    val indexViewContractsReader = IndexViewContractsReader()(unboundedExecutionContext)
    val contractStore = buildContractStore(indexViewContractsReader, unboundedExecutionContext)

    for {
      _ <- test(indexViewContractsReader, workload, unboundedExecutionContext) { ec => event =>
        assert_sync_vs_async_race_key(contractStore)(event)(ec)
      }
    } yield succeed
  }

  it should "preserve causal monotonicity under contention for contract state" in {
    val workload = generateWorkload(keysCount = 10L, contractsCount = 1000L)
    val indexViewContractsReader = IndexViewContractsReader()(unboundedExecutionContext)
    val contractStore = buildContractStore(indexViewContractsReader, unboundedExecutionContext)

    for {
      _ <- test(indexViewContractsReader, workload, unboundedExecutionContext) { ec => event =>
        assert_sync_vs_async_race_contract(contractStore)(event)(ec)
      }
    } yield succeed
  }

  override def afterAll(): Unit = {
    Await.ready(actorSystem.terminate(), 10.seconds)
    materializer.shutdown()
  }
}

private object MutableCacheBackedContractStoreRaceTests {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val stakeholders = Set(Ref.Party.assertFromString("some-stakeholder"))

  private def test(
      indexViewContractsReader: IndexViewContractsReader,
      workload: Seq[EventSequentialId => SimplifiedContractStateEvent],
      unboundedExecutionContext: ExecutionContext,
  )(
      assert: ExecutionContext => SimplifiedContractStateEvent => Future[Unit]
  )(implicit materializer: Materializer): Future[Done] =
    Source
      .fromIterator(() => workload.iterator)
      .statefulMapConcat { () =>
        var eventSequentialId = 0L

        eventCtor => {
          eventSequentialId += 1
          Iterator(eventCtor(eventSequentialId))
        }
      }
      .map(event => {
        indexViewContractsReader.update(event)
        event
      })
      .mapAsync(1)(
        // Validate the view's contents (test sanity-check)
        assertIndexState(indexViewContractsReader, _)(unboundedExecutionContext)
      )
      .mapAsync(1)(assert(unboundedExecutionContext))
      .run()

  private def assert_sync_vs_async_race_key(
      contractStore: MutableCacheBackedContractStore
  )(event: SimplifiedContractStateEvent)(implicit ec: ExecutionContext): Future[Unit] = {
    val contractStateEvent = toContractStateEvent(event)

    // Start async key lookup
    // Use Future.delegate here to ensure immediate control handover to the next statement
    val keyLookupF =
      Future.unit.flatMap(_ => contractStore.lookupContractKey(stakeholders, event.key))
    // Update the mutable contract state cache synchronously
    contractStore.push(contractStateEvent)

    for {
      // Lookup after synchronous update
      firstAsyncLookupResult <- contractStore.lookupContractKey(stakeholders, event.key)
      _ <- keyLookupF
      // Lookup after asynchronous update
      secondAsyncLookupResult <- contractStore.lookupContractKey(stakeholders, event.key)
    } yield {
      assertKeyAssignmentAfterAppliedEvent(firstAsyncLookupResult)(event)
      assertKeyAssignmentAfterAppliedEvent(secondAsyncLookupResult)(event)
    }
  }

  private def assert_sync_vs_async_race_contract(
      contractStore: MutableCacheBackedContractStore
  )(event: SimplifiedContractStateEvent)(implicit ec: ExecutionContext): Future[Unit] = {
    val contractStateEvent = toContractStateEvent(event)

    // Start async contract lookup
    // Use Future.delegate here to ensure immediate control handover to the next statement
    val keyLookupF =
      Future.unit.flatMap(_ => contractStore.lookupActiveContract(stakeholders, event.contractId))
    // Update the mutable contract state cache synchronously
    contractStore.push(contractStateEvent)

    for {
      // Lookup after synchronous update
      firstAsyncLookupResult <- contractStore.lookupActiveContract(stakeholders, event.contractId)
      _ <- keyLookupF
      // Lookup after asynchronous update
      secondAsyncLookupResult <- contractStore.lookupActiveContract(stakeholders, event.contractId)
    } yield {
      assertContractIdAssignmentAfterAppliedEvent(firstAsyncLookupResult)(event)
      assertContractIdAssignmentAfterAppliedEvent(secondAsyncLookupResult)(event)
    }
  }

  private def assertKeyAssignmentAfterAppliedEvent(
      assignment: Option[ContractId]
  )(event: SimplifiedContractStateEvent): Unit =
    assignment match {
      case Some(contractId) if (event.contractId != contractId) || !event.created =>
        fail(message =
          s"Key state corruption for ${event.key}: " +
            s"expected ${if (event.created) s"assignment to ${event.contractId} -> ${event.contract}"
            else "unassigned"}, " +
            s"but got assignment to $contractId"
        )
      case None if event.created =>
        fail(message =
          s"Key state corruption for ${event.key}: expected assignment to ${event.contractId} -> ${event.contract} " +
            "but got unassigned instead"
        )
      case _ => ()
    }

  private def assertContractIdAssignmentAfterAppliedEvent(
      assignment: Option[Contract]
  )(event: SimplifiedContractStateEvent): Unit =
    assignment match {
      case Some(actualContract) if (event.contract != actualContract) || !event.created =>
        fail(message =
          s"Contract state corruption for ${event.contractId}: " +
            s"expected ${if (event.created) s"active contract (${event.contract})"
            else "non-active contract"}, but got assignment to $actualContract"
        )
      case None if event.created =>
        fail(message =
          s"Contract state corruption for ${event.contractId}: expected active contract ${event.contract} " +
            "but got non-active contract"
        )
      case _ => ()
    }

  private def assertIndexState(
      indexViewContractsReader: IndexViewContractsReader,
      event: SimplifiedContractStateEvent,
  )(implicit ec: ExecutionContext) =
    for {
      _ <- indexViewContractsReader
        .lookupKeyState(event.key, event.eventSequentialId)
        .map {
          case KeyAssigned(contractId, _) if contractId == event.contractId && event.created =>
          case KeyUnassigned if !event.created =>
          case actual =>
            fail(
              s"Test bug: actual $actual after event $event: index view: ${indexViewContractsReader.keyStateStore
                .get(event.key)}"
            )
        }
      _ <- indexViewContractsReader
        .lookupContractState(event.contractId, event.eventSequentialId)
        .map {
          case Some(ActiveContract(actualContract, _, _))
              if event.created && event.contract == actualContract =>
          case Some(ArchivedContract(_)) if !event.created =>
          case actual =>
            fail(
              s"Test bug: actual $actual after event $event: index view: ${indexViewContractsReader.contractStateStore
                .get(event.contractId)}"
            )
        }
    } yield event

  private def generateWorkload(
      keysCount: Long,
      contractsCount: Long,
  ): Seq[EventSequentialId => SimplifiedContractStateEvent] = {
    val keys = (0L until keysCount).map { keyIdx =>
      keyIdx -> GlobalKey(Identifier.assertFromString("pkgId:module:entity"), ValueInt64(keyIdx))
    }.toMap

    val keysToContracts = keys.map { case (keyIdx, key) =>
      val contractLifecyclesForKey = contractsCount / keysCount
      key -> (0L until contractLifecyclesForKey)
        .map { contractIdx =>
          val globalContractIdx = keyIdx * contractLifecyclesForKey + contractIdx
          val contractId = ContractId.V1(Hash.hashPrivateKey(globalContractIdx.toString))
          val contractRef = contract(globalContractIdx)
          (contractId, contractRef)
        }
        .foldLeft(Vector.empty[(ContractId, Contract)]) { case (r, (k, v)) =>
          r :+ k -> v
        }
    }

    val updates =
      keysToContracts.map { case (key, contracts) =>
        contracts.flatMap { case (contractId, contractRef) =>
          Vector(
            (eventSeqId: EventSequentialId) =>
              SimplifiedContractStateEvent(
                eventSequentialId = eventSeqId,
                contractId = contractId,
                contract = contractRef,
                created = true,
                key = key,
              ),
            (eventSeqId: EventSequentialId) =>
              SimplifiedContractStateEvent(
                eventSequentialId = eventSeqId,
                contractId = contractId,
                contract = contractRef,
                created = false,
                key = key,
              ),
          )
        }
      }

    interleaveRandom(updates)
  }

  private def interleaveRandom(
      indexContractsUpdates: Iterable[Iterable[EventSequentialId => SimplifiedContractStateEvent]]
  ): Seq[EventSequentialId => SimplifiedContractStateEvent] = {
    @tailrec
    def interleaveIteratorsRandom[T](acc: Vector[T], col: Set[Iterator[T]]): Vector[T] =
      if (col.isEmpty) acc
      else {
        val vCol = col.toVector
        val randomIteratorIndex = Random.nextInt(vCol.size)
        val targetIterator = vCol(randomIteratorIndex)
        if (targetIterator.hasNext) interleaveIteratorsRandom(acc :+ targetIterator.next(), col)
        else interleaveIteratorsRandom(acc, col - targetIterator)
      }

    interleaveIteratorsRandom(
      Vector.empty[EventSequentialId => SimplifiedContractStateEvent],
      indexContractsUpdates.map(_.iterator).toSet,
    )
  }

  final case class SimplifiedContractStateEvent(
      eventSequentialId: EventSequentialId,
      contractId: ContractId,
      contract: Contract,
      created: Boolean,
      key: GlobalKey,
  )

  private def contract(idx: Long): Contract = {
    val templateId = Identifier.assertFromString(s"somePackage:someModule:someEntity")
    val contractArgument = com.daml.lf.value.Value.ValueInt64(idx)
    val contractInstance = ContractInstance(templateId, contractArgument, "some agreement")
    TransactionBuilder().versionContract(contractInstance)
  }

  private def buildContractStore(
      indexViewContractsReader: IndexViewContractsReader,
      ec: ExecutionContext,
  ) = MutableCacheBackedContractStore(
    contractsReader = indexViewContractsReader,
    signalNewLedgerHead = (_, _) => (),
    startIndexExclusive = EventSequentialId.beforeBegin,
    metrics = new Metrics(new MetricRegistry),
    maxContractsCacheSize = 1L,
    maxKeyCacheSize = 1L,
  )(ec, loggingContext)

  private val toContractStateEvent: SimplifiedContractStateEvent => ContractStateEvent = {
    case SimplifiedContractStateEvent(eventSequentialId, contractId, contract, created, key) =>
      if (created)
        ContractStateEvent.Created(
          contractId = contractId,
          contract = contract,
          globalKey = Some(key),
          ledgerEffectiveTime = Time.Timestamp.MinValue, // Not used
          stakeholders = stakeholders, // Not used
          eventOffset = Offset.beforeBegin, // Not used
          eventSequentialId = eventSequentialId,
        )
      else
        ContractStateEvent.Archived(
          contractId = contractId,
          globalKey = Some(key),
          stakeholders = stakeholders, // Not used
          eventOffset = Offset.beforeBegin, // Not used
          eventSequentialId = eventSequentialId,
        )
  }

  final case class ContractLifecycle(
      contractId: ContractId,
      contract: Contract,
      createdAt: Long,
      archivedAt: Option[Long],
  )

  // Simplified view of the index which models the evolution of the key and contracts state
  private case class IndexViewContractsReader()(implicit ec: ExecutionContext)
      extends LedgerDaoContractsReader {
    private type CreatedAt = Long
    @volatile private[cache] var contractStateStore = Map.empty[ContractId, ContractLifecycle]
    @volatile private[cache] var keyStateStore = Map.empty[Key, Vector[(CreatedAt, ContractId)]]

    // Evolves the index state
    // Non-thread safe
    def update(event: SimplifiedContractStateEvent): Unit =
      if (event.created) {
        // On create
        contractStateStore = contractStateStore.get(event.contractId) match {
          case None =>
            contractStateStore.updated(
              event.contractId,
              ContractLifecycle(
                contractId = event.contractId,
                contract = event.contract,
                createdAt = event.eventSequentialId,
                archivedAt = None,
              ),
            )
          case lastState @ Some(_) =>
            fail(s"Contract state update conflict: last state $lastState vs even $event")
        }

        keyStateStore = keyStateStore.get(event.key) match {
          case None =>
            keyStateStore.updated(event.key, Vector(event.eventSequentialId -> event.contractId))
          case Some(assignments) =>
            val (lastContractAssignedAt, currentContractId) = assignments.last
            val lastContract = contractStateStore(currentContractId)
            val createdAt = event.eventSequentialId
            if (lastContractAssignedAt < createdAt && lastContract.archivedAt.exists(_ < createdAt))
              keyStateStore.updated(event.key, assignments :+ (createdAt -> event.contractId))
            else fail(s"Key state update conflict: last state $lastContract vs event $event")
        }

      } else {
        // On archive
        contractStateStore = contractStateStore.get(event.contractId) match {
          case Some(contractLifecycle @ ContractLifecycle(contractId, _, createdAt, None))
              if event.eventSequentialId > createdAt && event.contractId == contractId =>
            contractStateStore.updated(
              event.contractId,
              contractLifecycle.copy(archivedAt = Some(event.eventSequentialId)),
            )
          case lastState =>
            fail(s"Contract state update conflict: last state $lastState vs even $event")
        }
      }

    override def lookupContractState(contractId: ContractId, validAt: Long)(implicit
        loggingContext: LoggingContext
    ): Future[Option[ContractState]] =
      Future {
        val _ = loggingContext
        contractStateStore
          .get(contractId)
          .flatMap { case ContractLifecycle(_, contract, createdAt, maybeArchivedAt) =>
            if (validAt < createdAt) None
            else if (maybeArchivedAt.forall(_ > validAt))
              Some(ActiveContract(contract, stakeholders, Time.Timestamp.MinValue))
            else Some(ArchivedContract(stakeholders))
          }
      }(ec)

    override def lookupKeyState(key: Key, validAt: Long)(implicit
        loggingContext: LoggingContext
    ): Future[KeyState] = Future {
      val _ = loggingContext
      keyStateStore
        .get(key)
        .map { vector =>
          val index = vector.searchBy(validAt, _._1) match {
            case Searching.Found(foundIndex) => foundIndex
            case Searching.InsertionPoint(insertionPoint) =>
              if (insertionPoint == 0) 0 else insertionPoint - 1
          }
          vector(index) match {
            case (_, contractId) =>
              contractStateStore(contractId).archivedAt match {
                case Some(archivedAt) if archivedAt <= validAt => KeyUnassigned
                case _ => KeyAssigned(contractId, stakeholders)
              }
          }
        }
        .getOrElse(KeyUnassigned)
    }(ec)

    override def lookupActiveContractAndLoadArgument(readers: Set[Party], contractId: ContractId)(
        implicit loggingContext: LoggingContext
    ): Future[Option[Contract]] = {
      val _ = (loggingContext, readers, contractId)
      // Needs to return None for divulgence lookups
      Future.successful(None)
    }

    override def lookupActiveContractWithCachedArgument(
        readers: Set[Party],
        contractId: ContractId,
        createArgument: VersionedValue,
    )(implicit loggingContext: LoggingContext): Future[Option[Contract]] = {
      val _ = (loggingContext, readers, contractId, createArgument)
      // Needs to return None for divulgence lookups
      Future.successful(None)
    }

    override def lookupMaximumLedgerTime(
        ids: Set[ContractId]
    )(implicit loggingContext: LoggingContext): Future[Option[Time.Timestamp]] = {
      val _ = (ids, loggingContext)
      throw new NotImplementedError("lookupMaximumLedgerTime")
    }

    override def lookupContractKey(key: Key, forParties: Set[Party])(implicit
        loggingContext: LoggingContext
    ): Future[Option[ContractId]] = {
      val _ = (key, forParties, loggingContext)
      throw new NotImplementedError("lookupContractKey")
    }
  }
}
