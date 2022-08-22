// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractInstance, ValueInt64, VersionedValue}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.MutableCacheBackedContractStoreRaceTests.{
  IndexViewContractsReader,
  assert_sync_vs_async_race_contract,
  assert_sync_vs_async_race_key,
  buildContractStore,
  generateWorkload,
  test,
}
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader._
import org.scalatest.Assertions.fail
import org.scalatest.flatspec.AsyncFlatSpec

import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.collection.immutable.{TreeMap, VectorMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class MutableCacheBackedContractStoreRaceTests extends AsyncFlatSpec with AkkaBeforeAndAfterAll {
  behavior of "Mutable state cache updates"

  private val unboundedExecutionContext =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

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
}

private object MutableCacheBackedContractStoreRaceTests {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val stakeholders = Set(Ref.Party.assertFromString("some-stakeholder"))

  private def test(
      indexViewContractsReader: IndexViewContractsReader,
      workload: Seq[Offset => SimplifiedContractStateEvent],
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
          Iterator(eventCtor(offset(counter)))
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
    val keyLookupF = Future.delegate(contractStore.lookupContractKey(stakeholders, event.key))
    // Update the mutable contract state cache synchronously
    contractStore.push(Vector(contractStateEvent))

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
    contractStore.push(Vector(contractStateEvent))

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
        .lookupKeyState(event.key, event.offset)
        .map {
          case Vector(contractId) if contractId == event.contractId && event.created =>
          case Vector() if !event.created =>
          case actual =>
            fail(
              s"Test bug: actual $actual after event $event: index view: ${indexViewContractsReader.keyStateStore
                  .get(event.key)}"
            )
        }
      _ <- indexViewContractsReader
        .lookupContractState(event.contractId, event.offset)
        .map {
          case Some(ActiveContract(actualContract, _, _, _))
              if event.created && event.contract == actualContract =>
          case Some(ArchivedContract(_, _)) if !event.created =>
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
  ): Seq[Offset => SimplifiedContractStateEvent] = {
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
        .foldLeft(VectorMap.empty[ContractId, Contract]) { case (r, (k, v)) =>
          r.updated(k, v)
        }
    }

    val updates =
      keysToContracts.map { case (key, contracts) =>
        contracts.flatMap { case (contractId, contractRef) =>
          Vector(
            (offset: Offset) =>
              SimplifiedContractStateEvent(
                offset = offset,
                contractId = contractId,
                contract = contractRef,
                created = true,
                key = key,
              ),
            (offset: Offset) =>
              SimplifiedContractStateEvent(
                offset = offset,
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
      indexContractsUpdates: Iterable[Iterable[Offset => SimplifiedContractStateEvent]]
  ): Seq[Offset => SimplifiedContractStateEvent] = {
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
      Vector.empty[Offset => SimplifiedContractStateEvent],
      indexContractsUpdates.map(_.iterator).toSet,
    )
  }

  final case class SimplifiedContractStateEvent(
      offset: Offset,
      contractId: ContractId,
      contract: Contract,
      created: Boolean,
      key: GlobalKey,
  )

  private def contract(idx: Long): Contract = {
    val templateId = Identifier.assertFromString(s"somePackage:someModule:someEntity")
    val contractArgument = Value.ValueInt64(idx)
    val contractInstance = ContractInstance(templateId, contractArgument, "some agreement")
    TransactionBuilder().versionContract(contractInstance)
  }

  private def buildContractStore(
      indexViewContractsReader: IndexViewContractsReader,
      ec: ExecutionContext,
  ) = {
    val metrics = new Metrics(new MetricRegistry)
    new MutableCacheBackedContractStore(
      contractsReader = indexViewContractsReader,
      metrics = metrics,
      contractStateCaches = ContractStateCaches.build(
        initialCacheIndex = Offset.beforeBegin,
        maxContractsCacheSize = 1L,
        maxKeyCacheSize = 1L,
        metrics = metrics,
      )(ec, loggingContext),
    )(ec, loggingContext)
  }

  private val toContractStateEvent: SimplifiedContractStateEvent => ContractStateEvent = {
    case SimplifiedContractStateEvent(offset, contractId, contract, created, key) =>
      if (created)
        ContractStateEvent.Created(
          contractId = contractId,
          contract = contract,
          globalKey = Some(key),
          ledgerEffectiveTime = Time.Timestamp.MinValue, // Not used
          stakeholders = stakeholders, // Not used
          eventOffset = offset,
          eventSequentialId = 0L, // Not used
        )
      else
        ContractStateEvent.Archived(
          contractId = contractId,
          globalKey = Some(key),
          stakeholders = stakeholders, // Not used
          eventOffset = offset,
          eventSequentialId = 0L, // Not used
        )
  }

  final case class ContractLifecycle(
      contractId: ContractId,
      contract: Contract,
      createdAt: Offset,
      archivedAt: Option[Offset],
      keyHash: Option[String],
  )

  // Simplified view of the index which models the evolution of the key and contracts state
  private case class IndexViewContractsReader()(implicit ec: ExecutionContext)
      extends LedgerDaoContractsReader {
    private type CreatedAt = Offset
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
                contractId = event.contractId,
                contract = event.contract,
                createdAt = event.offset,
                archivedAt = None,
                keyHash = Some(event.key.hash.bytes.toHexString),
              )
            )
          case lastState @ Some(_) =>
            fail(s"Contract state update conflict: last state $lastState vs even $event")
        }

        keyStateStore = keyStateStore.updatedWith(event.key) {
          case None => Some(TreeMap(event.offset -> event.contractId))
          case Some(assignments) =>
            val (lastContractAssignedAt, currentContractId) = assignments.last
            val lastContract = contractStateStore(currentContractId)
            val createdAt = event.offset
            if (lastContractAssignedAt < createdAt && lastContract.archivedAt.exists(_ < createdAt))
              Some(assignments + (createdAt -> event.contractId))
            else fail(s"Key state update conflict: last state $lastContract vs event $event")
        }
      } else {
        // On archive
        contractStateStore = contractStateStore.updatedWith(event.contractId) {
          case Some(contractLifecycle @ ContractLifecycle(contractId, _, createdAt, None, _))
              if event.offset > createdAt && event.contractId == contractId =>
            Some(contractLifecycle.copy(archivedAt = Some(event.offset)))
          case lastState =>
            fail(s"Contract state update conflict: last state $lastState vs even $event")
        }

        keyStateStore = keyStateStore.updatedWith(event.key) {
          case Some(assignments) =>
            val (currentCreatedAt, currentContractId) = assignments.last
            val lastContractAssignment = contractStateStore(currentContractId)
            val archivedAt = event.offset
            if (currentCreatedAt < archivedAt && lastContractAssignment.archivedAt.nonEmpty)
              Some(assignments + (archivedAt -> event.contractId))
            else
              fail(s"Key state update conflict: last state $lastContractAssignment vs event $event")
          case faultyState =>
            fail(s"Key state update conflict: $faultyState vs event $event")
        }
      }

    override def lookupContractState(contractId: ContractId, validAt: Offset)(implicit
        loggingContext: LoggingContext
    ): Future[Option[ContractState]] =
      Future {
        val _ = loggingContext
        contractStateStore
          .get(contractId)
          .flatMap { case ContractLifecycle(_, contract, createdAt, maybeArchivedAt, keyHash) =>
            if (validAt < createdAt) None
            else if (maybeArchivedAt.forall(_ > validAt))
              Some(ActiveContract(contract, stakeholders, Time.Timestamp.MinValue, keyHash))
            else Some(ArchivedContract(stakeholders, keyHash))
          }
      }(ec)

    override def lookupKeyState(key: Key, validAt: Offset)(implicit
        loggingContext: LoggingContext
    ): Future[Vector[ContractId]] = Future {
      val _ = loggingContext
      keyStateStore
        .get(key)
        .map(_.maxBefore(nextAfter(validAt)) match {
          case Some((_, contractId)) =>
            contractStateStore(contractId).archivedAt match {
              case Some(archivedAt) if archivedAt <= validAt => Vector.empty
              case _ => Vector(contractId)
            }
          case None => Vector.empty
        })
        .getOrElse(Vector.empty)
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
  }

  private def offset(idx: Long) = {
    val base = BigInt(1L) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }

  private def nextAfter(currentOffset: Offset) = {
    val offsetBytes = currentOffset.toByteArray
    if (offsetBytes.length == 0) {
      offset(0L)
    } else {
      Offset.fromByteArray((BigInt(currentOffset.toByteArray) + 1).toByteArray)
    }
  }
}
