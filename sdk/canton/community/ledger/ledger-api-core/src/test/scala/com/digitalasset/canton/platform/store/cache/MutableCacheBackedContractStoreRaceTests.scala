// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.data.NonEmptyVector
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.{
  Active,
  Archived,
  ExistingContractStatus,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.store.memory.InMemoryContractStore
import com.digitalasset.canton.participant.store.{ContractStore, PersistedContractInstance}
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStoreRaceTests.{
  IndexViewContractsReader,
  assert_sync_vs_async_race_contract,
  assert_sync_vs_async_race_key,
  buildContractStore,
  generateWorkload,
  test,
}
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.*
import com.digitalasset.canton.protocol.{ExampleContractFactory, ExampleTransactionFactory}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ValueInt64
import org.apache.pekko.Done
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.Assertions.fail
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.flatspec.AsyncFlatSpec

import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.collection.immutable.{TreeMap, VectorMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class MutableCacheBackedContractStoreRaceTests
    extends AsyncFlatSpec
    with PekkoBeforeAndAfterAll
    with TestEssentials {
  behavior of "Mutable state cache updates"

  private val unboundedExecutionContext =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  it should "preserve causal monotonicity under contention for key state" in {
    val workload = generateWorkload(keysCount = 10L, contractsCount = 1000L)
    val indexViewContractsReader = IndexViewContractsReader()(unboundedExecutionContext)
    val participantContractStore = new InMemoryContractStore(
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )(unboundedExecutionContext)
    val contractStore =
      buildContractStore(
        indexViewContractsReader,
        unboundedExecutionContext,
        loggerFactory,
        participantContractStore,
      )

    for {
      _ <- test(
        indexViewContractsReader,
        participantContractStore,
        workload,
        unboundedExecutionContext,
      ) { ec => event =>
        assert_sync_vs_async_race_key(contractStore)(event)(ec)
      }
    } yield succeed
  }

  it should "preserve causal monotonicity under contention for contract state" in {
    val workload = generateWorkload(keysCount = 10L, contractsCount = 1000L)
    val indexViewContractsReader = IndexViewContractsReader()(unboundedExecutionContext)
    val participantContractStore = new InMemoryContractStore(
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )(unboundedExecutionContext)
    val contractStore =
      buildContractStore(
        indexViewContractsReader,
        unboundedExecutionContext,
        loggerFactory,
        participantContractStore,
      )

    for {
      _ <- test(
        indexViewContractsReader,
        participantContractStore,
        workload,
        unboundedExecutionContext,
      ) { ec => event =>
        assert_sync_vs_async_race_contract(contractStore)(event)(ec)
      }
    } yield succeed
  }
}

private object MutableCacheBackedContractStoreRaceTests {
  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting
  private val stakeholders = Set(Ref.Party.assertFromString("some-stakeholder"))

  private def test(
      indexViewContractsReader: IndexViewContractsReader,
      participantContractStore: ContractStore,
      workload: Seq[Long => SimplifiedContractStateEvent],
      unboundedExecutionContext: ExecutionContext,
  )(
      assert: ExecutionContext => SimplifiedContractStateEvent => Future[Unit]
  )(implicit materializer: Materializer): Future[Done] =
    Source
      .fromIterator(() => workload.iterator)
      .statefulMapConcat { () =>
        var counter = 0L

        eventCtor => {
          counter += 1
          Iterator(eventCtor(counter))
        }
      }
      .map { event =>
        indexViewContractsReader.update(event)
        update(participantContractStore, event)(unboundedExecutionContext).futureValue
        event
      }
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
    val keyLookupF = Future.delegate(contractStore.lookupContractKey(stakeholders, event.key))
    // Update the mutable contract state cache synchronously
    contractStore.contractStateCaches.push(NonEmptyVector.of(contractStateEvent), event.eventSeqId)

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
      Future.delegate(contractStore.lookupActiveContract(stakeholders, event.contractId))
    // Update the mutable contract state cache synchronously
    contractStore.contractStateCaches.push(NonEmptyVector.of(contractStateEvent), event.eventSeqId)

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
      assignment: Option[FatContract]
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
        .lookupKeyState(event.key, event.eventSeqId)
        .map {
          case KeyAssigned(contractId) if contractId == event.contractId && event.created =>
          case KeyUnassigned if !event.created =>
          case actual =>
            fail(
              s"Test bug: actual $actual after event $event: index view: ${indexViewContractsReader.keyStateStore
                  .get(event.key)}"
            )
        }
      _ <- indexViewContractsReader
        .lookupContractState(event.contractId, event.eventSeqId)
        .map {
          case Some(Active) if event.created =>
          case Some(Archived) if !event.created =>
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
  ): Seq[Long => SimplifiedContractStateEvent] = {
    val keys = (0L until keysCount).map { keyIdx =>
      keyIdx -> Key.assertBuild(
        Identifier.assertFromString("pkgId:module:entity"),
        ValueInt64(keyIdx),
        Ref.PackageName.assertFromString("pkg-name"),
      )
    }.toMap

    val keysToContracts = keys.map { case (keyIdx, key) =>
      val contractLifecyclesForKey = contractsCount / keysCount
      key -> (0L until contractLifecyclesForKey)
        .map { contractIdx =>
          val globalContractIdx = keyIdx * contractLifecyclesForKey + contractIdx
          val contractRef = contract(globalContractIdx, key)
          (contractRef.contractId, contractRef)
        }
        .foldLeft(VectorMap.empty[ContractId, FatContract]) { case (r, (k, v)) =>
          r.updated(k, v)
        }
    }

    val updates =
      keysToContracts.map { case (key, contracts) =>
        contracts.flatMap { case (_, contractRef) =>
          Vector(
            (eventSeqId: Long) =>
              SimplifiedContractStateEvent(
                eventSeqId = eventSeqId,
                contract = contractRef,
                created = true,
                key = key,
              ),
            (eventSeqId: Long) =>
              SimplifiedContractStateEvent(
                eventSeqId = eventSeqId,
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
      indexContractsUpdates: Iterable[Iterable[Long => SimplifiedContractStateEvent]]
  ): Seq[Long => SimplifiedContractStateEvent] = {
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
      Vector.empty[Long => SimplifiedContractStateEvent],
      indexContractsUpdates.map(_.iterator).toSet,
    )
  }

  final case class SimplifiedContractStateEvent(
      eventSeqId: Long,
      contract: FatContract,
      created: Boolean,
      key: Key,
  ) {
    val contractId: ContractId = contract.contractId
  }

  private def contract(idx: Long, key: GlobalKey): FatContract = {
    val templateId = Identifier.assertFromString("pkgId:module:entity")
    val packageName = Ref.PackageName.assertFromString("pkg-name")
    val contractArgument = Value.ValueInt64(idx)
    ExampleContractFactory
      .build(
        packageName = packageName,
        templateId = templateId,
        argument = contractArgument,
        signatories = stakeholders,
        stakeholders = stakeholders,
        keyOpt = Some(KeyWithMaintainers(key, Set.empty)),
        overrideContractId = Some(ExampleTransactionFactory.suffixedId(idx.toInt, 0)),
      )
      .inst
  }

  private def buildContractStore(
      indexViewContractsReader: IndexViewContractsReader,
      ec: ExecutionContext,
      loggerFactory: NamedLoggerFactory,
      participantContractStore: ContractStore,
  ) = {
    val metrics = LedgerApiServerMetrics.ForTesting
    new MutableCacheBackedContractStore(
      contractsReader = indexViewContractsReader,
      contractStateCaches = ContractStateCaches.build(
        initialCacheEventSeqIdIndex = 0L,
        maxContractsCacheSize = 1L,
        maxKeyCacheSize = 1L,
        metrics = metrics,
        loggerFactory = loggerFactory,
      )(ec),
      contractStore = participantContractStore,
      loggerFactory = loggerFactory,
    )(ec)
  }

  private val toContractStateEvent: SimplifiedContractStateEvent => ContractStateEvent = {
    case SimplifiedContractStateEvent(_eventSeqId, contract, created, key) =>
      if (created)
        ContractStateEvent.Created(contract.contractId, Some(key))
      else
        ContractStateEvent.Archived(contract.contractId, Some(key))
  }

  final case class ContractLifecycle(
      contract: FatContract,
      createdAt: Long,
      archivedAt: Option[Long],
  )

  // Simplified view of the index which models the evolution of the key and contracts state
  private final case class IndexViewContractsReader()(implicit ec: ExecutionContext)
      extends LedgerDaoContractsReader {
    private type CreatedAt = Long
    @volatile private[cache] var contractStateStore = Map.empty[ContractId, ContractLifecycle]
    @volatile private[cache] var keyStateStore = Map.empty[Key, TreeMap[CreatedAt, ContractId]]

    // Evolves the index state
    // Non-thread safe
    def update(event: SimplifiedContractStateEvent): Unit =
      if (event.created) {
        // On create
        contractStateStore = contractStateStore.updatedWith(event.contractId) {
          case None =>
            Some(
              ContractLifecycle(
                contract = event.contract,
                createdAt = event.eventSeqId,
                archivedAt = None,
              )
            )
          case lastState @ Some(_) =>
            fail(s"Contract state update conflict: last state $lastState vs even $event")
        }

        keyStateStore = keyStateStore.updatedWith(event.key) {
          case None => Some(TreeMap(event.eventSeqId -> event.contractId))
          case Some(assignments) =>
            val (lastContractAssignedAt, currentContractId) = assignments.last
            val lastContract = contractStateStore(currentContractId)
            val createdAt = event.eventSeqId
            if (lastContractAssignedAt < createdAt && lastContract.archivedAt.exists(_ < createdAt))
              Some(assignments + (createdAt -> event.contractId))
            else fail(s"Key state update conflict: last state $lastContract vs event $event")
        }
      } else {
        // On archive
        contractStateStore = contractStateStore.updatedWith(event.contractId) {
          case Some(contractLifecycle @ ContractLifecycle(contract, createdAt, None))
              if event.eventSeqId > createdAt && event.contractId == contract.contractId =>
            Some(contractLifecycle.copy(archivedAt = Some(event.eventSeqId)))
          case lastState =>
            fail(s"Contract state update conflict: last state $lastState vs even $event")
        }

        keyStateStore = keyStateStore.updatedWith(event.key) {
          case Some(assignments) =>
            val (currentCreatedAt, currentContractId) = assignments.last
            val lastContractAssignment = contractStateStore(currentContractId)
            val archivedAt = event.eventSeqId
            if (currentCreatedAt < archivedAt && lastContractAssignment.archivedAt.nonEmpty)
              Some(assignments + (archivedAt -> event.contractId))
            else
              fail(s"Key state update conflict: last state $lastContractAssignment vs event $event")
          case faultyState =>
            fail(s"Key state update conflict: $faultyState vs event $event")
        }
      }

    override def lookupContractState(contractId: ContractId, notEarlierThanEventSeqId: Long)(
        implicit loggingContext: LoggingContextWithTrace
    ): Future[Option[ExistingContractStatus]] =
      Future {
        val _ = loggingContext
        contractStateStore
          .get(contractId)
          .flatMap { case ContractLifecycle(_, createdAt, maybeArchivedAt) =>
            if (notEarlierThanEventSeqId < createdAt) None
            else if (maybeArchivedAt.forall(_ > notEarlierThanEventSeqId))
              Some(ContractStateStatus.Active)
            else Some(ContractStateStatus.Archived)
          }
      }(ec)

    override def lookupKeyState(key: Key, notEarlierThanEventSeqId: Long)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[KeyState] = Future {
      val _ = loggingContext
      keyStateStore
        .get(key)
        .map(_.maxBefore(notEarlierThanEventSeqId + 1) match {
          case Some((_, contractId)) =>
            contractStateStore(contractId).archivedAt match {
              case Some(archivedAt) if archivedAt <= notEarlierThanEventSeqId => KeyUnassigned
              case _ => KeyAssigned(contractId)
            }
          case None => KeyUnassigned
        })
        .getOrElse(KeyUnassigned)
    }(ec)

    override def lookupKeyStatesFromDb(keys: Seq[Key], notEarlierThanEventSeqId: Long)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Map[Key, KeyState]] = ??? // not used in this test
  }

  def update(contractStore: ContractStore, event: SimplifiedContractStateEvent)(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    if (event.created) {
      val contract = PersistedContractInstance(event.contract)
      contractStore
        .storeContracts(Seq(contract.asContractInstance))
        .map(_ => ())
        .failOnShutdownToAbortException("test")
    } else Future.unit
}
