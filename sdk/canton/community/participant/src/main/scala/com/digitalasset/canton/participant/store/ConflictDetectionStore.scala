// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.participant.util.StateChange
import com.digitalasset.canton.store.{PrunableByTime, Purgeable}
import com.digitalasset.canton.tracing.TraceContext

/** Common interface for stores used by conflict detection */
trait ConflictDetectionStore[K, A <: PrettyPrinting] extends PrunableByTime with Purgeable {

  /** Fetches the latest states for the given identifiers from the store.
    * @return The map from identifiers in `ids` in the store to their latest state.
    *         Nonexistent identifiers are excluded from the map.
    */
  def fetchStates(ids: Iterable[K])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[K, StateChange[A]]]

  /** Separate method for fetching states for invariant checking
    * so that we can distinguish them from regular calls to [[fetchStates]] in tests.
    */
  private[participant] def fetchStatesForInvariantChecking(ids: Iterable[K])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[K, StateChange[A]]] = fetchStates(ids)
}
