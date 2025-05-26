// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.EpochInProgress
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleName}
import com.digitalasset.canton.version.ProtocolVersion

import EpochState.Epoch

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
    loggerFactory: NamedLoggerFactory,
    timeouts: ProcessingTimeout,
)(implicit synchronizerProtocolVersion: ProtocolVersion, metricsContext: MetricsContext)
    extends SegmentModuleRefFactory[E] {
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
        s"segment-module-${segmentState.epoch.info.number}-${segmentState.segment.slotNumbers.head1}"
      )
    )
    context.setModule(moduleRef, module)
    module.ready(moduleRef)
    moduleRef
  }
}
