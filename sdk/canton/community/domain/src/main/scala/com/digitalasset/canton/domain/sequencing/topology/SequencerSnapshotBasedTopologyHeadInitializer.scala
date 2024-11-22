// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.topology

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit.DefaultHeadStateInitializer
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** If a snapshot is provided, updates the topology client's head state up to
  * [[com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot.lastTs]], because this is up to where
  * the topology store is queried for the onboarding state. Otherwise, uses the default initializer.
  * See [[com.digitalasset.canton.domain.sequencing.service.GrpcSequencerAdministrationService.onboardingState]] for details.
  */
final class SequencerSnapshotBasedTopologyHeadInitializer(
    sequencerSnapshot: Option[SequencerSnapshot]
) extends DomainTopologyClientWithInit.HeadStateInitializer {

  override def initialize(
      client: DomainTopologyClientWithInit,
      store: TopologyStore[TopologyStoreId.DomainStore],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DomainTopologyClientWithInit] =
    sequencerSnapshot match {
      case Some(snapshot) =>
        store
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
      case None =>
        DefaultHeadStateInitializer.initialize(client, store)
    }
}
