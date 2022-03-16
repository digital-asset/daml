// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractInstance, ValueInt64, VersionedValue}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.appendonlydao.events.ContractStateEvent
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.cache.MutableCacheBackedContractStoreRaceConditionsTest.{
  IndexViewContractsReader,
  MonotonicityProbe,
  contract,
  test,
}
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import scala.collection.{Searching, immutable}
import scala.collection.immutable.VectorMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class MutableCacheBackedContractStoreRaceConditionsTest
    extends AsyncFlatSpec
    with Matchers
    with Eventually
    with MockitoSugar
    with BeforeAndAfterAll {
  behavior of "updates"
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  it should "preserve causal monotonicity under contention" in {
    val keysCount = 100L
    val contractsCount = 100000L
    val cacheSizeDivider = 2L

    val keys = (0L until keysCount).map { keyIdx =>
      keyIdx -> GlobalKey(Identifier.assertFromString("pkgId:module:entity"), ValueInt64(keyIdx))
    }.toMap

    val keysAndContracts: Map[Key, VectorMap[ContractId, Contract]] = keys.map {
      case (keyIdx, key) =>
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

    val contractIdMapping = keysAndContracts.flatMap { _._2 }

    val unboundedExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    val indexViewContractsReader = IndexViewContractsReader()(unboundedExecutionContext)

    val contractStore = MutableCacheBackedContractStore(
      contractsReader = indexViewContractsReader,
      signalNewLedgerHead = (_, _) => (),
      startIndexExclusive = Offset.beforeBegin -> EventSequentialId.beforeBegin,
      metrics = new Metrics(new MetricRegistry),
      maxContractsCacheSize = contractsCount / cacheSizeDivider,
      maxKeyCacheSize = keysCount / cacheSizeDivider,
    )(scala.concurrent.ExecutionContext.global, loggingContext)

    val monotonicityProbe = MonotonicityProbe(contractIdMapping, contractStore)

    val loadF =
      test(indexViewContractsReader, contractStore, monotonicityProbe, keysAndContracts)(
        unboundedExecutionContext
      )

    loadF
      .map { _ =>
        indexViewContractsReader.keyStateStore.size shouldBe keysCount
        indexViewContractsReader.contractStateStore.size shouldBe contractsCount
        succeed
      }
  }
}

private object MutableCacheBackedContractStoreRaceConditionsTest {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val delayMaxMillis = 10L

  case class MonotonicityProbe(
      contractMapping: Map[ContractId, Contract],
      contractStore: MutableCacheBackedContractStore,
  )(implicit ec: ExecutionContext) {
    private var keyQueries = Map.empty[Key, (ContractId, Instant)]

    def sample(key: Key): Future[Unit] = {
      val currentSampleTime = Instant.now()

      contractStore
        .lookupContractKey(stakeholders, key)
        .map {
          case Some(nextContractId) =>
            keyQueries.synchronized {
              val nextContract = contractMapping(nextContractId)
              keyQueries.get(key) match {
                case Some((prevContractId, previousSampleTime)) =>
                  val prevContract = contractMapping(prevContractId)
                  val prevIndex = prevContract.unversioned.arg.asInstanceOf[ValueInt64]
                  val nextIndex = nextContract.unversioned.arg.asInstanceOf[ValueInt64]
                  assert(
                    !previousSampleTime.isAfter(currentSampleTime),
                    s"Sample time evolved backwards for key $key: Previous sample time $previousSampleTime vs current sample time $currentSampleTime",
                  )
                  assert(
                    prevIndex.value <= nextIndex.value,
                    s"Key cache corrupted - key $key evolved backwards: Previous assignment ($prevContractId -> $prevContract) vs current assignment ($nextContractId -> $nextContract).",
                  )
                case None => ()
              }
              keyQueries = keyQueries.updated(key, nextContractId -> currentSampleTime)
            }
          case None => ()
        }
    }
  }

  private def test(
      indexViewContractsReader: IndexViewContractsReader,
      contractStore: MutableCacheBackedContractStore,
      monotonicityProbe: MonotonicityProbe,
      keysAndContracts: Map[Key, VectorMap[ContractId, Contract]],
  )(unboundedExecutionContext: ExecutionContext) = {
    implicit val ec: ExecutionContext = unboundedExecutionContext
    val eventSequentialIdRef = new AtomicLong(0L)
    def nextEventSequentialId() = eventSequentialIdRef.incrementAndGet()

    val indexContractsUpdates =
      keysAndContracts.map { case (key, contractsForKey) =>
        key -> contractsForKey.flatMap { case (contractId, contractRef) =>
          Vector(
            (eventSeqId: EventSequentialId) =>
              SimplifiedContractStateEvent(
                eventSequentialId = eventSeqId,
                contractId = contractId,
                contract = contractRef,
                created = true,
                key = key,
              ),
            // Add some delay here
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

    Future
      .sequence(
        indexContractsUpdates.map { case (key, keyFlow) =>
          updateAndQueryFlowForKey(
            indexViewContractsReader,
            contractStore,
            key,
            keyFlow,
            nextEventSequentialId _,
            monotonicityProbe.sample,
          )
        }
      )
      .map(_ => ())
  }

  private def updateAndQueryFlowForKey(
      indexViewContractsReader: IndexViewContractsReader,
      contractStore: MutableCacheBackedContractStore,
      key: Key,
      keyFlow: immutable.Iterable[EventSequentialId => SimplifiedContractStateEvent],
      nextEventSequentialId: () => Long,
      sample: Key => Future[Unit],
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val updateStreamF = keyFlow.foldLeft(Future.unit) { case (f, event) =>
      f.flatMap { _ =>
        Thread.sleep(Random.nextLong(delayMaxMillis))
        val updateEvent = event(nextEventSequentialId())
        indexViewContractsReader.update(updateEvent)
        contractStore.push(toContractStoreEvent(updateEvent))
        Future.unit
      }
    }

    val queryF = (1L to 10L * keyFlow.size.toLong)
      .foldLeft(Future.unit) { case (f, _) =>
        f.flatMap { _ => sample(key) }
      }

    updateStreamF zip queryF map (_ => ())
  }

  private val stakeholders = Set(Ref.Party.assertFromString("some-stakeholder"))

  final case class ContractLifecycle(
      contractId: ContractId,
      contract: Contract,
      createdAt: Long,
      archivedAt: Option[Long],
  )

  sealed trait TestAction extends Product with Serializable

  final case class SimplifiedContractStateEvent(
      eventSequentialId: EventSequentialId,
      contractId: ContractId,
      contract: Contract,
      created: Boolean,
      key: GlobalKey,
  ) extends TestAction

  private def contract(idx: Long): Contract = {
    val templateId = Identifier.assertFromString(s"somePackage:someModule:someEntity")
    val contractArgument = Value.ValueInt64(idx)
    val contractInstance = ContractInstance(
      templateId,
      contractArgument,
      "some agreement",
    )
    TransactionBuilder().versionContract(contractInstance)
  }

  private val toContractStoreEvent: SimplifiedContractStateEvent => ContractStateEvent = {
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

  case class InvalidUpdateException(cause: String) extends RuntimeException(cause)

  case class IndexViewContractsReader()(implicit ec: ExecutionContext)
      extends LedgerDaoContractsReader {
    var contractStateStore = Map.empty[ContractId, ContractLifecycle]
    var keyStateStore = Map.empty[Key, Vector[ContractLifecycle]]

    private def delay(): Unit = Thread.sleep(Random.nextLong(delayMaxMillis))

    def update(event: SimplifiedContractStateEvent): Unit = synchronized {
      if (event.created) {
        // On create
        val newContractLifecycle = ContractLifecycle(
          contractId = event.contractId,
          contract = event.contract,
          createdAt = event.eventSequentialId,
          archivedAt = None,
        )
        contractStateStore = contractStateStore.updatedWith(event.contractId) {
          case None => Some(newContractLifecycle)
          case Some(_) =>
            throw InvalidUpdateException(s"Already created for contract id: ${event.contractId}")
        }
        keyStateStore = keyStateStore.updatedWith(event.key) {
          case None =>
            Some(Vector(newContractLifecycle))
          case Some(stateTransitions) =>
            stateTransitions.last match {
              case ContractLifecycle(_, _, _, Some(archivedAt)) =>
                if (archivedAt < event.eventSequentialId)
                  Some(stateTransitions :+ newContractLifecycle)
                else
                  throw InvalidUpdateException(
                    s"Key state span conflict: $archivedAt vs ${event.eventSequentialId}"
                  )
              case lastState @ ContractLifecycle(_, _, createdAt, None) =>
                if (createdAt < event.eventSequentialId) {
                  Some(
                    stateTransitions.init :+ lastState.copy(archivedAt =
                      Some(event.eventSequentialId)
                    )
                  )
                } else
                  throw InvalidUpdateException(
                    s"Key state span conflict: $createdAt vs ${event.eventSequentialId}"
                  )
            }
        }
      } else {
        // On archive
        contractStateStore = contractStateStore.updatedWith(event.contractId) {
          case None =>
            throw InvalidUpdateException(s"You cannot archive a non-existing contract")
          case Some(ContractLifecycle(_, _, _, Some(_))) =>
            throw InvalidUpdateException(s"You cannot archive an archived contract")
          case Some(ContractLifecycle(_, _, createdAt, None))
              if createdAt >= event.eventSequentialId =>
            throw InvalidUpdateException("You cannot archive before a create")
          case Some(contractLifecycle @ ContractLifecycle(_, _, _, None)) =>
            Some(contractLifecycle.copy(archivedAt = Some(event.eventSequentialId)))
        }
        keyStateStore = keyStateStore.updatedWith(event.key) {
          case None => throw InvalidUpdateException("You cannot un-assign a non-existing key")
          case Some(stateTransitions) =>
            stateTransitions.last match {
              case ContractLifecycle(_, _, _, Some(_)) =>
                throw InvalidUpdateException(s"You cannot un-assign an unassigned key")
              case ContractLifecycle(_, _, createdAt, None)
                  if createdAt >= event.eventSequentialId =>
                throw InvalidUpdateException(
                  s"You cannot un-assign a key for a contract at or before its create"
                )
              case contractLifecycle @ ContractLifecycle(_, _, _, None) =>
                Some(
                  stateTransitions.init :+ contractLifecycle.copy(archivedAt =
                    Some(event.eventSequentialId)
                  )
                )
            }
        }
      }
    }

    override def lookupContractState(contractId: ContractId, validAt: Long)(implicit
        loggingContext: LoggingContext
    ): Future[Option[ContractState]] =
      Future {
        val _ = loggingContext
        delay()
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
      delay()
      keyStateStore
        .get(key)
        .map { stateTransitionsVector =>
          // We can search since we guarantee order at update
          stateTransitionsVector.view.map(_.createdAt).search(validAt) match {
            case Searching.Found(foundIndex) =>
              val state = stateTransitionsVector(foundIndex)
              KeyAssigned(state.contractId, stakeholders)
            case Searching.InsertionPoint(insertionPoint) =>
              if (insertionPoint == 0) KeyUnassigned
              else {
                val state = stateTransitionsVector(insertionPoint - 1)
                state.archivedAt match {
                  case Some(archivedAt) if archivedAt <= validAt => KeyUnassigned
                  case Some(_) => KeyAssigned(state.contractId, stakeholders)
                  case None => KeyUnassigned
                }
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
  }
}
