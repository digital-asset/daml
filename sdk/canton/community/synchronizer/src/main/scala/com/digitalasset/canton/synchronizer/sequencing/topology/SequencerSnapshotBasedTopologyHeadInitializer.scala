// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.topology

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.synchronizer.sequencer.SequencerSnapshot
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientHeadStateInitializer,
  SynchronizerTopologyClientWithInit,
}
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** For a provided sequencer snapshot, updates the topology client's head state up to
  * [[sequencer.SequencerSnapshot.lastTs]], because this is up to where the topology store is
  * queried for the onboarding state. See
  * [[com.digitalasset.canton.synchronizer.sequencing.service.GrpcSequencerAdministrationService.onboardingState]]
  * for details.
  */
final class SequencerSnapshotBasedTopologyHeadInitializer(
    snapshot: SequencerSnapshot,
    topologyStore: TopologyStore[TopologyStoreId.SynchronizerStore],
) extends SynchronizerTopologyClientHeadStateInitializer {

  override def initialize(
      client: SynchronizerTopologyClientWithInit
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] =
    topologyStore
      .maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
      .map { maxTopologyStoreTimestamp =>
        val snapshotLastTsEffective = EffectiveTime(snapshot.lastTs)
        // Use the highest possible effective time.
        val maxEffectiveTime = maxTopologyStoreTimestamp
          .fold(snapshotLastTsEffective) { case (_, maxStoreEffectiveTime) =>
            maxStoreEffectiveTime.max(snapshotLastTsEffective)
          }

        client.updateHead(
          SequencedTime(snapshot.lastTs),
          maxEffectiveTime,
          ApproximateTime(snapshot.lastTs),
          potentialTopologyChange = true,
        )
        client
      }
}
