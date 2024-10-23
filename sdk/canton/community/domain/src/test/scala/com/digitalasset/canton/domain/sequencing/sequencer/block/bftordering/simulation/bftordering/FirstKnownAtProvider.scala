// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.bftordering

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.memory.SimulationOutputBlockMetadataStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.snapshot.FirstKnownAt
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingDataProvider
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext

class FirstKnownAtProvider(
    onboardingTimes: Map[SequencerId, EffectiveTime],
    stores: Map[SequencerId, SimulationOutputBlockMetadataStore],
) extends OnboardingDataProvider[Option[FirstKnownAt]] {

  implicit private val traceContext: TraceContext = TraceContext.empty

  override def provide(forSequencerId: SequencerId): Option[FirstKnownAt] = {
    val onboardingTime = onboardingTimes(forSequencerId)
    // We could have checked all the stores. But currently, we check only one.
    // It's similar to using a sequencer snapshot only from one peer (which is also not BFT).
    val maybeStore = stores.view.filterNot(_._1 == forSequencerId).values.headOption
    maybeStore.fold(None: Option[FirstKnownAt]) { store =>
      val onboardingBlock = store
        .getLatestAtOrBefore(onboardingTime.value)
        .resolveValue()
        .getOrElse(
          sys.error(
            s"Failed to get the latest block at or before $onboardingTime for peer $forSequencerId"
          )
        )
      val firstBlockAndPreviousBftTime = onboardingBlock.map { block =>
        val epochNumber = block.epochNumber
        val firstBlockInEpoch = store
          .getFirstInEpoch(epochNumber)
          .resolveValue()
          .map(_.map(_.blockNumber))
          .toOption
          .flatten
          .getOrElse(
            sys.error(
              s"Failed to get the first block in onboarding epoch $epochNumber for peer $forSequencerId"
            )
          )
        val previousBftTime = store
          .getLastInEpoch(EpochNumber(epochNumber - 1L))
          .resolveValue()
          .map(_.map(_.blockBftTime))
          .getOrElse(
            sys.error(
              s"Failed to get the previous BFT time for peer $forSequencerId"
            )
          )
        firstBlockInEpoch -> previousBftTime
      }
      // Trying to reflect what the non-simulated code would use.
      Some(
        firstBlockAndPreviousBftTime
          .map { case (startBlockNumber, previousBftTime) =>
            FirstKnownAt(
              Some(onboardingTime),
              onboardingBlock.map(_.epochNumber),
              Some(startBlockNumber),
              previousBftTime,
            )
          }
          .getOrElse(
            FirstKnownAt(
              Some(onboardingTime),
              // Present from genesis.
              epochNumber = None,
              firstBlockNumberInEpoch = None,
              previousBftTime = None,
            )
          )
      )
    }
  }
}
