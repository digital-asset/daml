// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.metrics.Timed
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.store.{ContractStore, PersistedContractInstance}
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
  ): Future[Map[LfContractId, Long]]
}

final case class LedgerApiContractStoreImpl(
    participantContractStore: ContractStore,
    loggerFactory: NamedLoggerFactory,
    metrics: LedgerApiServerMetrics,
)(implicit ec: ExecutionContext)
    extends LedgerApiContractStore
    with NamedLogging {

  def lookupPersisted(id: LfContractId)(implicit
      traceContext: TraceContext
  ): Future[Option[PersistedContractInstance]] =
    Timed
      .future(
        metrics.contractStore.lookupPersisted,
        failOnShutdown(
          participantContractStore
            .lookupPersisted(id)
        ),
      )

  def lookupBatchedNonCached(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, PersistedContractInstance]] =
    Timed.future(
      metrics.contractStore.lookupBatched,
      failOnShutdown(
        participantContractStore
          .lookupBatchedNonCached(internalContractIds)
      ),
    )

  def lookupBatchedNonCachedInternalIds(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, Long]] =
    Timed.future(
      metrics.contractStore.lookupBatchedInternalIds,
      failOnShutdown(
        participantContractStore
          .lookupBatchedNonCachedInternalIds(contractIds)
      ),
    )

  def lookupBatchedNonCachedContractIds(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, LfContractId]] =
    Timed
      .future(
        metrics.contractStore.lookupBatchedContractIds,
        failOnShutdown(
          participantContractStore
            .lookupBatchedNonCachedContractIds(internalContractIds)
        ),
      )

  def storeContracts(contracts: Seq[ContractInstance])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, Long]] =
    failOnShutdown(
      participantContractStore
        .storeContracts(contracts)
    )

  private def failOnShutdown[T](f: FutureUnlessShutdown[T])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[T] =
    f.failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)

}
