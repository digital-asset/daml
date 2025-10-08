// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.ledger.participant.state.index
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.ExistingContractStatus
import com.digitalasset.canton.ledger.participant.state.index.{
  ContractState,
  ContractStateStatus,
  ContractStore,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.store
import com.digitalasset.canton.platform.store.cache.ContractKeyStateValue.*
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import com.digitalasset.daml.lf.transaction.GlobalKey

import scala.concurrent.{ExecutionContext, Future}

private[platform] class MutableCacheBackedContractStore(
    contractsReader: LedgerDaoContractsReader,
    val loggerFactory: NamedLoggerFactory,
    private[cache] val contractStateCaches: ContractStateCaches,
    contractStore: store.ContractStore,
)(implicit executionContext: ExecutionContext)
    extends ContractStore
    with NamedLogging {

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[FatContract]] =
    lookupContractState(contractId)
      .map(contractStateToResponse(readers))

  override def lookupContractState(
      contractId: ContractId
  )(implicit loggingContext: LoggingContextWithTrace): Future[ContractState] =
    contractStateCaches.contractState
      .get(contractId)
      .map(Future.successful)
      .getOrElse(readThroughContractsCache(contractId))
      .flatMap {
        case ContractStateStatus.Active =>
          contractStore
            .lookupPersisted(contractId)
            .failOnShutdownTo(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)
            .map {
              case Some(persistedContract) => ContractState.Active(persistedContract.inst)
              case None =>
                logger.error(
                  s"Contract $contractId marked as active in index (db or cache) but not found in participant's contract store"
                )
                ContractState.NotFound
            }
        case ContractStateStatus.Archived => Future.successful(ContractState.Archived)
        case ContractStateStatus.NotFound => Future.successful(ContractState.NotFound)
      }

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractId]] =
    contractStateCaches.keyState
      .get(key)
      .map(Future.successful)
      .getOrElse(readThroughKeyCache(key))
      .flatMap(keyStateToResponse(_, readers))

  private def readThroughContractsCache(contractId: ContractId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[ContractStateStatus] =
    contractStateCaches.contractState
      .putAsync(
        contractId,
        contractsReader.lookupContractState(contractId, _).map(toContractCacheValue),
      )

  private def keyStateToResponse(
      value: ContractKeyStateValue,
      readers: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[ContractId]] = value match {
    case Assigned(contractId) =>
      lookupContractState(contractId).map(
        contractStateToResponse(readers)(_).map(_.contractId)
      )

    case _: Assigned | Unassigned => Future.successful(None)
  }

  private def contractStateToResponse(readers: Set[Party])(
      value: index.ContractState
  ): Option[FatContract] =
    value match {
      case ContractState.Active(contract) if nonEmptyIntersection(contract.stakeholders, readers) =>
        Some(contract)
      case _ =>
        None
    }

  private val toContractCacheValue: Option[ExistingContractStatus] => ContractStateStatus =
    _.getOrElse(ContractStateStatus.NotFound)

  private val toKeyCacheValue: KeyState => ContractKeyStateValue = {
    case LedgerDaoContractsReader.KeyAssigned(contractId) =>
      Assigned(contractId)
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
