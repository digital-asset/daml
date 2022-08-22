// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{ContractStore, MaximumLedgerTime}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.VersionedContractInstance
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.backend.PersistentContractKeyHash
import com.daml.platform.store.cache.ContractStateValue._
import com.daml.platform.store.cache.MutableCacheBackedContractStore._
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.{
  ActiveContract,
  ArchivedContract,
  ContractState,
}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

private[platform] class MutableCacheBackedContractStore(
    metrics: Metrics,
    contractsReader: LedgerDaoContractsReader,
    private[cache] val contractStateCaches: ContractStateCaches,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends ContractStore {

  private val logger = ContextualizedLogger.get(getClass)

  def push(eventsBatch: Vector[ContractStateEvent]): Unit = {
    debugEvents(eventsBatch)
    contractStateCaches.push(eventsBatch)
  }

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    contractStateCaches.contractState
      .get(contractId)
      .map(Future.successful)
      .getOrElse(readThroughContractsCache(contractId))
      .flatMap(contractStateToResponse(readers, contractId))

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    contractStateCaches.keyState
      .get(PersistentContractKeyHash(key))
      .map(Future.successful)
      .getOrElse(readThroughKeyCache(key))
      .flatMap(keyStateToResponse(_, readers, key))

  override def lookupMaximumLedgerTimeAfterInterpretation(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[MaximumLedgerTime] =
    Future
      .successful(partitionCached(ids))
      .flatMap {
        case Left(archivedContracts) =>
          Future.successful(MaximumLedgerTime.Archived(archivedContracts))

        case Right((cached, toBeFetched)) if toBeFetched.isEmpty =>
          Future.successful(MaximumLedgerTime.from(cached.maxOption))

        case Right((cached, toBeFetched)) =>
          readThroughMaximumLedgerTime(toBeFetched.toList, cached.maxOption)
      }

  override def lookupContractForValidation(
      contractId: ContractId
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[(VersionedContractInstance, Timestamp)]] =
    contractStateCaches.contractState
      .get(contractId)
      .map(Future.successful)
      .getOrElse(readThroughContractsCache(contractId))
      .map {
        case NotFound => None
        case Active(contract, _, let, _) => Some(contract -> let)
        case Archived(_, _) => None
      }

  private def readThroughMaximumLedgerTime(
      missing: List[ContractId],
      acc: Option[Timestamp],
  ): Future[MaximumLedgerTime] =
    missing match {
      case contractId :: restOfMissing =>
        readThroughContractsCache(contractId).flatMap {
          case active: Active =>
            val newMaximumLedgerTime = Some(
              (active.createLedgerEffectiveTime :: acc.toList).max
            )
            readThroughMaximumLedgerTime(restOfMissing, newMaximumLedgerTime)

          case _: Archived =>
            Future.successful(MaximumLedgerTime.Archived(Set(contractId)))

          case NotFound =>
            // If cannot be found: no create or archive event for the contract.
            // Since this contract is part of the input, it was able to be looked up once.
            // So this is the case of a divulged contract, which was not archived.
            // Divulged contract does not change maximumLedgerTime

            readThroughMaximumLedgerTime(restOfMissing, acc)
        }

      case _ => Future.successful(MaximumLedgerTime.from(acc))
    }

  private def partitionCached(
      ids: Set[ContractId]
  )(implicit
      loggingContext: LoggingContext
  ): Either[Set[ContractId], (Set[Timestamp], Set[ContractId])] = {
    val cacheQueried = ids.view.map(id => id -> contractStateCaches.contractState.get(id)).toVector

    val cached = cacheQueried
      .foldLeft[Either[Set[ContractId], Set[Timestamp]]](Right(Set.empty[Timestamp])) {
        // successful lookups
        case (Right(timestamps), (_, Some(active: Active))) =>
          Right(timestamps + active.createLedgerEffectiveTime)
        case (Right(timestamps), (_, None)) => Right(timestamps)

        // failure cases
        case (acc, (cid, Some(Archived(_, _) | NotFound))) =>
          val missingContracts = acc.left.getOrElse(Set.empty) + cid
          Left(missingContracts)
        case (acc @ Left(_), _) => acc
      }

    cached
      .map { cached =>
        val missing = cacheQueried.view.collect { case (id, None) => id }.toSet
        (cached, missing)
      }
  }

  private def readThroughContractsCache(contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[ContractStateValue] = {
    val readThroughRequest =
      (validAt: Offset) =>
        contractsReader
          .lookupContractState(contractId, validAt)
          .map(toContractCacheValue)
          .transformWith {
            case Success(NotFound) =>
              metrics.daml.execution.cache.readThroughNotFound.inc()
              // We must not cache negative lookups by contract-id, as they can be invalidated by later divulgence events.
              // This is OK from a performance perspective, as we do not expect uses-cases that require
              // caching of contract absence or the results of looking up divulged contracts.
              Future.failed(ContractReadThroughNotFound(contractId))
            case result => Future.fromTry(result)
          }

    contractStateCaches.contractState
      .putAsync(contractId, readThroughRequest)
      .transformWith {
        case Failure(_: ContractReadThroughNotFound) => Future.successful(NotFound)
        case other => Future.fromTry(other)
      }
  }

  private def keyStateToResponse(
      mappings: Vector[ContractId],
      readers: Set[Party],
      key: GlobalKey,
  ): Future[Option[ContractId]] = {
    mappings.headOption match {
      case None => Future.successful(None)
      case Some(latestContractId) =>
        contractStateCaches.contractState
          .get(latestContractId)
          .map(Future.successful)
          .getOrElse(readThroughContractsCache(latestContractId))
          .transformWith {
            // not found cases...we just continue, should not happen (except archival and pruning happened silently)
            case Failure(ContractReadThroughNotFound(_)) =>
              keyStateToResponse(mappings.tail, readers, key)
            case Success(ContractStateValue.NotFound) =>
              keyStateToResponse(mappings.tail, readers, key)

            // if contract hash does not match, let's keep looking (this we only need for resolving collisions, with nonUCK engine might resolve this)
            case Success(active: ContractStateValue.Active)
                if active.contractKeyHash.get != key.hash.bytes.toHexString =>
              keyStateToResponse(mappings.tail, readers, key)
            case Success(archived: ContractStateValue.Archived)
                if archived.contractKeyHash.get != key.hash.bytes.toHexString =>
              keyStateToResponse(mappings.tail, readers, key)

            // latest active contract is accessible means assigned if UCK
            case Success(active: ContractStateValue.Active)
                if nonEmptyIntersection(readers, active.stakeholders) =>
              Future.successful(Some(latestContractId))

            // latest active contract is not accessible means unassigned if UCK
            case Success(_: ContractStateValue.Active) =>
              Future.successful(None)

            // latest contract is archived means unassigned if UCK
            case Success(_: ContractStateValue.Archived) =>
              Future.successful(None)

            // the rest of the errors will be bubble up
            case Failure(throwable) =>
              Future.failed(throwable)
          }
    }
  }

  private def contractStateToResponse(readers: Set[Party], contractId: ContractId)(
      value: ContractStateValue
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    value match {
      case Active(contract, stakeholders, _, _) if nonEmptyIntersection(stakeholders, readers) =>
        Future.successful(Some(contract))
      case Archived(stakeholders, _) if nonEmptyIntersection(stakeholders, readers) =>
        Future.successful(Option.empty)
      case contractStateValue =>
        // This flow is exercised when the readers are not stakeholders of the contract
        // (the contract might have been divulged to the readers)
        // OR the contract was not found in the index
        //
        logger.debug(s"Checking divulgence for contractId=${contractId.coid} and readers=$readers")
        resolveDivulgenceLookup(contractStateValue, contractId, readers)
    }

  private def resolveDivulgenceLookup(
      contractStateValue: ContractStateValue,
      contractId: ContractId,
      forParties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    contractStateValue match {
      case Active(contract, _, _, _) =>
        metrics.daml.execution.cache.resolveDivulgenceLookup.inc()
        contractsReader.lookupActiveContractWithCachedArgument(
          forParties,
          contractId,
          contract.map(_.arg),
        )
      case _: Archived | NotFound =>
        // We need to fetch the contract here since the contract creation or archival
        // may have not been divulged to the readers
        metrics.daml.execution.cache.resolveFullLookup.inc()
        contractsReader.lookupActiveContractAndLoadArgument(
          forParties,
          contractId,
        )
    }

  private val toContractCacheValue: Option[ContractState] => ContractStateValue = {
    case Some(ActiveContract(contract, stakeholders, ledgerEffectiveTime, contractKeyHash)) =>
      ContractStateValue.Active(contract, stakeholders, ledgerEffectiveTime, contractKeyHash)
    case Some(ArchivedContract(stakeholders, contractKeyHash)) =>
      ContractStateValue.Archived(stakeholders, contractKeyHash)
    case None => ContractStateValue.NotFound
  }

  private def readThroughKeyCache(
      key: GlobalKey
  )(implicit loggingContext: LoggingContext): Future[Vector[ContractId]] = {
    val readThroughRequest = (validAt: Offset) => contractsReader.lookupKeyState(key, validAt)
    contractStateCaches.keyState.putAsync(PersistentContractKeyHash(key), readThroughRequest)
  }

  private def nonEmptyIntersection[T](one: Set[T], other: Set[T]): Boolean =
    one.intersect(other).nonEmpty

  private def debugEvents(
      eventsBatch: Seq[ContractStateEvent]
  )(implicit loggingContext: LoggingContext): Unit =
    eventsBatch.foreach {
      case ContractStateEvent.Created(
            contractId,
            _,
            globalKey,
            _,
            _,
            eventOffset,
            eventSequentialId,
          ) =>
        logger.debug(
          s"State events update: Created(contractId=${contractId.coid}, globalKey=$globalKey, offset=$eventOffset, eventSequentialId=$eventSequentialId)"
        )
      case ContractStateEvent.Archived(
            contractId,
            globalKey,
            _,
            eventOffset,
            eventSequentialId,
          ) =>
        logger.debug(
          s"State events update: Archived(contractId=${contractId.coid}, globalKey=$globalKey, offset=$eventOffset, eventSequentialId=$eventSequentialId)"
        )
    }
}

private[platform] object MutableCacheBackedContractStore {
  type EventSequentialId = Long

  final case class ContractReadThroughNotFound(contractId: ContractId) extends NoStackTrace {
    override def getMessage: String =
      s"Contract not found for contract id ${contractId.coid}. Hint: this could be due racing with a concurrent archival."
  }
}
