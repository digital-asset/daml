// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.EpochInProgress
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  Env,
  ModuleName,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.Clock

trait SegmentModuleRefFactory[E <: Env[E]] {
  def apply(
      context: E#ActorContextT[Consensus.Message[E]],
      epoch: Epoch,
      cryptoProvider: CryptoProvider[E],
      latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
      epochInProgress: EpochInProgress,
  )(
      segmentState: SegmentState,
      metricsAccumulator: EpochMetricsAccumulator,
  ): E#ModuleRefT[ConsensusSegment.Message]
}

final class SegmentModuleRefFactoryImpl[E <: Env[E]](
    storePbftMessages: Boolean,
    epochStore: EpochStore[E],
    dependencies: ConsensusModuleDependencies[E],
    clock: Clock,
    loggerFactory: NamedLoggerFactory,
    timeouts: ProcessingTimeout,
) extends SegmentModuleRefFactory[E] {
  override def apply(
      context: E#ActorContextT[Consensus.Message[E]],
      epoch: Epoch,
      cryptoProvider: CryptoProvider[E],
      latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
      epochInProgress: EpochInProgress,
  )(
      segmentState: SegmentState,
      metricsAccumulator: EpochMetricsAccumulator,
  ): E#ModuleRefT[ConsensusSegment.Message] = {
    val module = new IssSegmentModule[E](
      epoch,
      segmentState,
      metricsAccumulator,
      storePbftMessages,
      epochStore,
      clock,
      cryptoProvider,
      latestCompletedEpochLastCommits,
      epochInProgress,
      context.self,
      dependencies.availability,
      dependencies.p2pNetworkOut,
      timeouts,
      loggerFactory,
    )
    val moduleRef: E#ModuleRefT[ConsensusSegment.Message] = context.newModuleRef(
      ModuleName(
        s"segment-module-${segmentState.epochNumber}-${segmentState.segment.slotNumbers.head1}"
      )
    )
    context.setModule(moduleRef, module)
    module.ready(moduleRef)
    moduleRef
  }
}
