// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  ModuleSystem,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

trait LeaderSelectionInitializer[E <: Env[E]] {
  def stateForInitial(
      moduleSystem: ModuleSystem[E],
      value: Option[SequencerSnapshotAdditionalInfo],
      epochNumber: EpochNumber,
  )(implicit traceContext: TraceContext): BlacklistLeaderSelectionPolicyState

  def leaderFromState(
      state: BlacklistLeaderSelectionPolicyState,
      orderingTopology: OrderingTopology,
  ): Seq[BftNodeId]

  def leaderSelectionPolicy(
      blacklistLeaderSelectionPolicyState: BlacklistLeaderSelectionPolicyState,
      orderingTopology: OrderingTopology,
  ): LeaderSelectionPolicy[E]
}

object LeaderSelectionInitializer {
  def create[E <: Env[E]](
      thisNode: BftNodeId,
      config: BftBlockOrdererConfig,
      protocolVersion: ProtocolVersion,
      store: OutputMetadataStore[E],
      timeouts: ProcessingTimeout,
      failBootstrap: String => TraceContext => Nothing,
      metrics: BftOrderingMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit metricsContext: MetricsContext): LeaderSelectionInitializer[E] =
    config.leaderSelectionPolicy match {
      case BftBlockOrdererConfig.LeaderSelectionPolicyConfig.Simple =>
        new SimpleLeaderSelectionPolicyInitializer[E](protocolVersion)
      case blacklistLeaderSelectionPolicyConfig: BftBlockOrdererConfig.LeaderSelectionPolicyConfig.Blacklisting =>
        new BlacklistLeaderSelectionInitializer(
          thisNode,
          config,
          blacklistLeaderSelectionPolicyConfig,
          protocolVersion,
          store,
          timeouts,
          failBootstrap,
          metrics,
          loggerFactory,
        )
    }
}
