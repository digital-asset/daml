// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InFlightAggregations,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Backing store for the [[com.digitalasset.canton.domain.block.BlockSequencerStateManager]] used for sequencer integrations to persist some sequencer
  * data into a database.
  */
trait SequencerStateManagerStore {

  /** Rehydrate the sequencer state from the backing persisted store
    *
    * @param timestamp The timestamp for which the state is computed
    */
  def readInFlightAggregations(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[InFlightAggregations]

  /** Updates the in-flight aggregations for the given aggregation IDs.
    * Only adds or updates aggregations, but never removes them.
    *
    * @see expireInFlightAggregations for removing in-flight aggregations
    */
  def addInFlightAggregationUpdates(updates: InFlightAggregationUpdates)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Removes all in-flight aggregations that have expired before or at the given timestamp */
  def pruneExpiredInFlightAggregations(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

}
