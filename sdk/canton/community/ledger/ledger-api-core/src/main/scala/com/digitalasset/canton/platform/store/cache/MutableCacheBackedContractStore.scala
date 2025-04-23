// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.ledger.participant.state.index
import com.digitalasset.canton.ledger.participant.state.index.ContractStore
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.cache.ContractKeyStateValue.*
import com.digitalasset.canton.platform.store.cache.ContractStateValue.*
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  ActiveContract,
  ArchivedContract,
  ContractState,
  KeyState,
}
import com.digitalasset.daml.lf.transaction.GlobalKey

import scala.concurrent.{ExecutionContext, Future}

private[platform] class MutableCacheBackedContractStore(
    contractsReader: LedgerDaoContractsReader,
    val loggerFactory: NamedLoggerFactory,
    private[cache] val contractStateCaches: ContractStateCaches,
)(implicit executionContext: ExecutionContext)
    extends ContractStore
    with NamedLogging {

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ThinContract]] =
    lookupContractStateValue(contractId)
      .flatMap(contractStateToResponse(readers))

  override def lookupContractState(
      contractId: ContractId
  )(implicit loggingContext: LoggingContextWithTrace): Future[index.ContractState] =
    lookupContractStateValue(contractId)
      .map {
        case active: Active =>
          index.ContractState.Active(
            contractInstance = active.contract,
            ledgerEffectiveTime = active.createLedgerEffectiveTime,
            stakeholders = active.stakeholders,
            signatories = active.signatories,
            globalKey = active.globalKey,
            maintainers = active.keyMaintainers,
            driverMetadata = active.driverMetadata,
          )
        case _: Archived => index.ContractState.Archived
        case NotFound => index.ContractState.NotFound
      }

  private def lookupContractStateValue(
      contractId: ContractId
  )(implicit loggingContext: LoggingContextWithTrace): Future[ContractStateValue] =
    contractStateCaches.contractState
      .get(contractId)
      .map(Future.successful)
      .getOrElse(readThroughContractsCache(contractId))

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractId]] =
    contractStateCaches.keyState
      .get(key)
      .map(Future.successful)
      .getOrElse(readThroughKeyCache(key))
      .map(keyStateToResponse(_, readers))

  private def readThroughContractsCache(contractId: ContractId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[ContractStateValue] =
    contractStateCaches.contractState
      .putAsync(
        contractId,
        contractsReader.lookupContractState(contractId, _).map(toContractCacheValue),
      )

  private def keyStateToResponse(
      value: ContractKeyStateValue,
      readers: Set[Party],
  ): Option[ContractId] = value match {
    case Assigned(contractId, createWitnesses) if nonEmptyIntersection(readers, createWitnesses) =>
      Some(contractId)
    case _: Assigned | Unassigned => Option.empty
  }

  private def contractStateToResponse(readers: Set[Party])(
      value: ContractStateValue
  ): Future[Option[ThinContract]] =
    value match {
      case Active(contract, stakeholders, _, _, _, _, _)
          if nonEmptyIntersection(stakeholders, readers) =>
        Future.successful(Some(contract))
      case _ =>
        Future.successful(Option.empty)
    }

  private val toContractCacheValue: Option[ContractState] => ContractStateValue = {
    case Some(active: ActiveContract) =>
      ContractStateValue.Active(
        contract = active.contract,
        stakeholders = active.stakeholders,
        createLedgerEffectiveTime = active.ledgerEffectiveTime,
        signatories = active.signatories,
        globalKey = active.globalKey,
        keyMaintainers = active.keyMaintainers,
        driverMetadata = active.driverMetadata,
      )
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
  )(implicit loggingContext: LoggingContextWithTrace): Future[ContractKeyStateValue] =
    // Even if we have a contract id, we do not automatically trigger the loading of the contract here.
    // For prefetching at the start of command interpretation, this is done explicitly there.
    // For contract key lookups during interpretation, there is no benefit in doing so,
    // because contract prefetching blocks in the current architecture.
    // So when Daml engine does not need the contract after all, we avoid loading it.
    // Conversely, when Daml engine does need the contract, then this will trigger the loading of the contract
    // with the same parallelization and batching opportunities.
    contractStateCaches.keyState
      .putAsync(
        key,
        contractsReader.lookupKeyState(key, _).map(toKeyCacheValue),
      )

  private def nonEmptyIntersection[T](one: Set[T], other: Set[T]): Boolean =
    one.intersect(other).nonEmpty
}

private[platform] object MutableCacheBackedContractStore {
  type EventSequentialId = Long
}
