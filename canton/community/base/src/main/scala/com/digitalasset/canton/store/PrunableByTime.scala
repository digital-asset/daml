// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

/** Interface for a store that allows pruning and keeps track of when pruning has started and finished. */
trait PrunableByTime {

  protected implicit val ec: ExecutionContext

  /** Prune all unnecessary data relating to events before the given timestamp.
    *
    * The meaning of "unnecessary", and whether the limit is inclusive or exclusive both depend on the particular store.
    * The store must implement the actual pruning logic in the [[doPrune]] method.
    */
  final def prune(
      limit: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      lastTs <- getLastPruningTs
      _ <- advancePruningTimestamp(PruningPhase.Started, limit)
      _ <- doPrune(limit, lastTs)
      _ <- advancePruningTimestamp(PruningPhase.Completed, limit)
    } yield ()

  private def getLastPruningTs(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = pruningStatus.map(_.flatMap(_.lastSuccess))

  /** Returns the latest timestamp at which pruning was started or completed.
    * For [[com.digitalasset.canton.pruning.PruningPhase.Started]], it is guaranteed
    * that no pruning has been run on the store after the returned timestamp.
    * For [[com.digitalasset.canton.pruning.PruningPhase.Completed]], it is guaranteed
    * that the store is pruned at least up to the returned timestamp (inclusive).
    * That is, another pruning with the returned timestamp (or earlier) has no effect on the store.
    * Returns [[scala.None$]] if no pruning has ever been started on the store.
    */
  def pruningStatus(implicit traceContext: TraceContext): Future[Option[PruningStatus]]

  @VisibleForTesting
  protected[canton] def advancePruningTimestamp(phase: PruningPhase, timestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Future[Unit]

  @VisibleForTesting
  protected[canton] def doPrune(limit: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): Future[Unit]

}
