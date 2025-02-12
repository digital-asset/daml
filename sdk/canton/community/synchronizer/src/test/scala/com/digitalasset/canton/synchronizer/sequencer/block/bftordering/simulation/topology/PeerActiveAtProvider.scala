// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.PeerActiveAt
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingDataProvider
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

class PeerActiveAtProvider(
    onboardingTimes: Map[SequencerId, TopologyActivationTime],
    stores: Map[SequencerId, BftOrderingStores[SimulationEnv]],
) extends OnboardingDataProvider[Option[PeerActiveAt]] {

  implicit private val traceContext: TraceContext = TraceContext.empty

  override def provide(forSequencerId: SequencerId): Option[PeerActiveAt] = {
    val onboardingTime = onboardingTimes(forSequencerId)
    // We could check all the output metadata stores. Currently, we check only one for a peer that was onboarded earlier.
    // It's similar to using a sequencer snapshot only from one peer (which is also not BFT).
    val maybeStores = stores.view
      .filterNot { case (sequencerId, _) =>
        sequencerId == forSequencerId || onboardingTime.value <= onboardingTimes(sequencerId).value
      }
      .values
      // Conservatively, find an output metadata store with the highest block number.
      .maxByOption { stores =>
        stores.outputStore.getLastConsecutiveBlock
          .resolveValue()
          .toOption
          .flatMap(_.map(_.blockNumber))
          .getOrElse(BlockNumber(0L))
      }

    // Trying to reflect what the non-simulated code would do.
    maybeStores.fold(None: Option[PeerActiveAt]) { stores =>
      val outputMetadataStore = stores.outputStore
      val maybeOnboardingBlock = outputMetadataStore
        .getLatestBlockAtOrBefore(onboardingTime.value)
        .resolveValue()
        .getOrElse(
          sys.error(
            s"Failed to get the latest block at or before $onboardingTime for peer $forSequencerId"
          )
        )
      maybeOnboardingBlock
        .map { onboardingBlock =>
          val startEpochNumber = onboardingBlock.epochNumber
          val firstBlockInEpoch = outputMetadataStore
            .getFirstBlockInEpoch(startEpochNumber)
            .resolveValue()
            .map(_.map(_.blockNumber))
            .toOption
            .flatten
            .getOrElse(
              sys.error(
                s"Failed to get the first block in onboarding epoch $startEpochNumber for peer $forSequencerId"
              )
            )
          val previousBftTime = outputMetadataStore
            .getLastBlockInEpoch(EpochNumber(startEpochNumber - 1L))
            .resolveValue()
            .map(_.map(_.blockBftTime))
            .getOrElse(
              sys.error(
                s"Failed to get the previous BFT time for peer $forSequencerId"
              )
            )
          val epochTopologyQueryTimestamp =
            stores.epochStoreReader
              .loadEpochInfo(startEpochNumber)
              .resolveValue()
              .map(_.map(_.topologyActivationTime))
              .getOrElse(
                sys.error(
                  s"Failed to get the start epoch topology query timestamp for peer $forSequencerId"
                )
              )
          PeerActiveAt(
            onboardingTime,
            Some(startEpochNumber),
            Some(firstBlockInEpoch),
            epochTopologyQueryTimestamp,
            // Switch the value deterministically so that we trigger all code paths.
            epochCouldAlterOrderingTopology = Some(startEpochNumber % 2 == 0),
            previousBftTime,
          )
        }
    }
  }
}
