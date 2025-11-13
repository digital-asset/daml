// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

// this is a wrapper trait around ContractStore to be used from Ledger API layer and handle exceptions and shutdowns uniformly
trait LedgerApiContractStore {

  def lookupPersisted(id: LfContractId)(implicit
      traceContext: TraceContext
  ): Future[Option[PersistedContractInstance]]

  def lookupBatchedNonCached(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, PersistedContractInstance]]

  def lookupBatchedNonCachedInternalIds(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, Long]]

  def lookupBatchedNonCachedContractIds(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, LfContractId]]

  @VisibleForTesting
  def storeContracts(contracts: Seq[ContractInstance])(implicit
      traceContext: TraceContext
  ): Future[Unit]
}

final case class LedgerApiContractStoreImpl(
    participantContractStore: ContractStore,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LedgerApiContractStore
    with NamedLogging {

  def lookupPersisted(id: LfContractId)(implicit
      traceContext: TraceContext
  ): Future[Option[PersistedContractInstance]] =
    failOnShutdown(
      participantContractStore
        .lookupPersisted(id)
    )

  def lookupBatchedNonCached(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, PersistedContractInstance]] =
    failOnShutdown(
      participantContractStore
        .lookupBatchedNonCached(internalContractIds)
    )

  def lookupBatchedNonCachedInternalIds(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, Long]] =
    failOnShutdown(
      participantContractStore
        .lookupBatchedNonCachedInternalIds(contractIds)
    )

  def lookupBatchedNonCachedContractIds(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, LfContractId]] =
    failOnShutdown(
      participantContractStore
        .lookupBatchedNonCachedContractIds(internalContractIds)
    )

  def storeContracts(contracts: Seq[ContractInstance])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    failOnShutdown(
      participantContractStore
        .storeContracts(contracts)
    )

  private def failOnShutdown[T](f: FutureUnlessShutdown[T])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[T] =
    f.failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)

}
