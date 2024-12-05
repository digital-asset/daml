// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.topology

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.client.{
  DomainTopologyClientHeadStateInitializer,
  DomainTopologyClientWithInit,
}
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** For a provided sequencer snapshot, updates the topology client's head state up to
  * [[com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot.lastTs]], because this is up to where
  * the topology store is queried for the onboarding state.
  * See [[com.digitalasset.canton.domain.sequencing.service.GrpcSequencerAdministrationService.onboardingState]] for details.
  */
final class SequencerSnapshotBasedTopologyHeadInitializer(
    snapshot: SequencerSnapshot,
    topologyStore: TopologyStore[TopologyStoreId.DomainStore],
) extends DomainTopologyClientHeadStateInitializer {

  override def initialize(
      client: DomainTopologyClientWithInit
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[DomainTopologyClientWithInit] =
    topologyStore
      .maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true)
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
