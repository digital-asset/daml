// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.integrations.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.synchronizer.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InFlightAggregations,
}
import com.digitalasset.canton.tracing.TraceContext

/** Backing store for the [[com.digitalasset.canton.synchronizer.block.BlockSequencerStateManager]] used for sequencer integrations to persist some sequencer
  * data into a database.
  */
trait SequencerStateManagerStore {

  /** Rehydrate the sequencer state from the backing persisted store
    *
    * @param timestamp The timestamp for which the state is computed
    */
  def readInFlightAggregations(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[InFlightAggregations]

  /** Updates the in-flight aggregations for the given aggregation IDs.
    * Only adds or updates aggregations, but never removes them.
    *
    * @see expireInFlightAggregations for removing in-flight aggregations
    */
  def addInFlightAggregationUpdates(updates: InFlightAggregationUpdates)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Removes all in-flight aggregations that have expired before or at the given timestamp */
  def pruneExpiredInFlightAggregations(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

}
