// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.ContractKeyStateValue._
import com.daml.platform.store.cache.ContractStateValue._
import com.daml.platform.store.cache.MutableCacheBackedContractStore._
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.{
  ActiveContract,
  ArchivedContract,
  ContractState,
  KeyState,
}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

private[platform] class MutableCacheBackedContractStore(
    metrics: Metrics,
    contractsReader: LedgerDaoContractsReader,
    private[cache] val contractStateCaches: ContractStateCaches,
)(implicit executionContext: ExecutionContext)
    extends ContractStore {

  private val logger = ContextualizedLogger.get(getClass)

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    lookupContractStateValue(contractId)
      .flatMap(contractStateToResponse(readers, contractId))

  override def lookupContractStateWithoutDivulgence(
      contractId: ContractId
  )(implicit loggingContext: LoggingContext): Future[v2.ContractState] =
    lookupContractStateValue(contractId)
      .map {
        case active: Active =>
          v2.ContractState.Active(active.contract, active.createLedgerEffectiveTime)
        case _: Archived => v2.ContractState.Archived
        case NotFound => v2.ContractState.NotFound
      }

  private def lookupContractStateValue(
      contractId: ContractId
  )(implicit loggingContext: LoggingContext): Future[ContractStateValue] =
    contractStateCaches.contractState
      .get(contractId)
      .map(Future.successful)
      .getOrElse(readThroughContractsCache(contractId))

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    contractStateCaches.keyState
      .get(key)
      .map(Future.successful)
      .getOrElse(readThroughKeyCache(key))
      .map(keyStateToResponse(_, readers))

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
      value: ContractKeyStateValue,
      readers: Set[Party],
  ): Option[ContractId] = value match {
    case Assigned(contractId, createWitnesses) if nonEmptyIntersection(readers, createWitnesses) =>
      Some(contractId)
    case _: Assigned | Unassigned => Option.empty
  }

  private def contractStateToResponse(readers: Set[Party], contractId: ContractId)(
      value: ContractStateValue
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    value match {
      case Active(contract, stakeholders, _) if nonEmptyIntersection(stakeholders, readers) =>
        Future.successful(Some(contract))
      case Archived(stakeholders) if nonEmptyIntersection(stakeholders, readers) =>
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
      case Active(contract, _, _) =>
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
    case Some(ActiveContract(contract, stakeholders, ledgerEffectiveTime)) =>
      ContractStateValue.Active(contract, stakeholders, ledgerEffectiveTime)
    case Some(ArchivedContract(stakeholders)) =>
      ContractStateValue.Archived(stakeholders)
    case None => ContractStateValue.NotFound
  }

  private val toKeyCacheValue: KeyState => ContractKeyStateValue = {
    case LedgerDaoContractsReader.KeyAssigned(contractId, stakeholders) =>
      Assigned(contractId, stakeholders)
    case LedgerDaoContractsReader.KeyUnassigned =>
      Unassigned
  }

  private def readThroughKeyCache(
      key: GlobalKey
  )(implicit loggingContext: LoggingContext): Future[ContractKeyStateValue] = {
    val readThroughRequest = (validAt: Offset) =>
      contractsReader.lookupKeyState(key, validAt).map(toKeyCacheValue)
    contractStateCaches.keyState.putAsync(key, readThroughRequest)
  }

  private def nonEmptyIntersection[T](one: Set[T], other: Set[T]): Boolean =
    one.intersect(other).nonEmpty
}

private[platform] object MutableCacheBackedContractStore {
  type EventSequentialId = Long

  final case class ContractReadThroughNotFound(contractId: ContractId) extends NoStackTrace {
    override def getMessage: String =
      s"Contract not found for contract id ${contractId.coid}. Hint: this could be due racing with a concurrent archival."
  }
}
