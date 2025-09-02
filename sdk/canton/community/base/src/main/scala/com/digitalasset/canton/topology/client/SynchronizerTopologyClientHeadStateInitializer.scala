// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.data.SynchronizerPredecessor
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Responsible for calling [[SynchronizerTopologyClientWithInit.updateHead]] for the first time. */
trait SynchronizerTopologyClientHeadStateInitializer {

  def initialize(
      client: SynchronizerTopologyClientWithInit,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit]
}

object SynchronizerTopologyClientHeadStateInitializer {

  /** Compute the initial timestamps to update head
    * @param maxTimestamp
    *   Max timestamp found in the store
    * @param synchronizerPredecessor
    *   Predecessor of the synchronizer, if known
    * @return
    */
  def computeInitialHeadUpdate(
      maxTimestamp: Option[(SequencedTime, EffectiveTime)],
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  ): Option[(SequencedTime, EffectiveTime)] = {
    val upgradeTimestamps: Option[(SequencedTime, EffectiveTime)] = synchronizerPredecessor
      .map(_.upgradeTime)
      .map(ts => (SequencedTime(ts), EffectiveTime(ts)))

    /*
    On the successor (so if the predecessor is defined), then the topology is known until the upgrade time.
     */
    (maxTimestamp.toList ++ upgradeTimestamps.toList)
      .maxByOption { case (sequencedTime, _) => sequencedTime }
  }
}

/** A topology client head initializer implementation relying solely on maximum timestamps from the
  * topology store.
  */
final class DefaultHeadStateInitializer(store: TopologyStore[TopologyStoreId.SynchronizerStore])
    extends SynchronizerTopologyClientHeadStateInitializer {

  override def initialize(
      client: SynchronizerTopologyClientWithInit,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] =
    store
      .maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
      .map { maxTimestamp =>
        SynchronizerTopologyClientHeadStateInitializer
          .computeInitialHeadUpdate(
            maxTimestamp,
            synchronizerPredecessor,
          )
          .foreach { case (sequenced, effective) =>
            client.updateHead(
              sequenced,
              effective,
              effective.toApproximate,
              potentialTopologyChange = true,
            )
          }
        client
      }
}
