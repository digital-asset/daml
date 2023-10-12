// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.participant.util.StateChange
import com.digitalasset.canton.store.PrunableByTime
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Common interface for stores used by conflict detection */
trait ConflictDetectionStore[K, A <: PrettyPrinting] extends PrunableByTime {

  /** Short-hand for [[fetchStates]] for a single identifier */
  def fetchState(id: K)(implicit traceContext: TraceContext): Future[Option[StateChange[A]]] =
    fetchStates(Seq(id)).map(_.get(id))

  /** Fetches the latest states for the given identifiers from the store.
    * @return The map from identifiers in `ids` in the store to their latest state.
    *         Nonexistent identifiers are excluded from the map.
    */
  def fetchStates(ids: Iterable[K])(implicit
      traceContext: TraceContext
  ): Future[Map[K, StateChange[A]]]

  /** Separate method for fetching states for invariant checking
    * so that we can distinguish them from regular calls to [[fetchStates]] in tests.
    */
  private[participant] def fetchStatesForInvariantChecking(ids: Iterable[K])(implicit
      traceContext: TraceContext
  ): Future[Map[K, StateChange[A]]] = fetchStates(ids)
}
