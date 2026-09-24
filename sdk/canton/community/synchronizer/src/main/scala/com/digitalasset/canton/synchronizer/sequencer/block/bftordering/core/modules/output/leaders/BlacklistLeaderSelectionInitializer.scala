// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
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

class BlacklistLeaderSelectionInitializer[E <: Env[E]](
    thisNode: BftNodeId,
    config: BftBlockOrdererConfig,
    blacklistLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig,
    protocolVersion: ProtocolVersion,
    store: OutputMetadataStore[E],
    timeouts: ProcessingTimeout,
    failBootstrap: String => TraceContext => Nothing,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit metricsContext: MetricsContext)
    extends LeaderSelectionInitializer[E]
    with NamedLogging {

  def stateForInitial(
      moduleSystem: ModuleSystem[E],
      value: Option[SequencerSnapshotAdditionalInfo],
      epochNumber: EpochNumber,
  )(implicit traceContext: TraceContext): BlacklistLeaderSelectionPolicyState =
    value match {
      case Some(snapshot) =>
        computeStateFromSnapshot(moduleSystem, snapshot)

      case None =>
        stateFromStore(moduleSystem, epochNumber)
    }

  def leaderFromState(
      state: BlacklistLeaderSelectionPolicyState,
      orderingTopology: OrderingTopology,
  ): Seq[BftNodeId] = state.computeLeaders(
    orderingTopology,
    blacklistLeaderSelectionPolicyConfig,
  )

  def leaderSelectionPolicy(
      blacklistLeaderSelectionPolicyState: BlacklistLeaderSelectionPolicyState,
      orderingTopology: OrderingTopology,
  ): LeaderSelectionPolicy[E] = BlacklistLeaderSelectionPolicy.create(
    blacklistLeaderSelectionPolicyState,
    config,
    blacklistLeaderSelectionPolicyConfig,
    orderingTopology,
    store,
    metrics,
    loggerFactory,
  )

  private def computeStateFromSnapshot(
      moduleSystem: ModuleSystem[E],
      snapshot: SequencerSnapshotAdditionalInfo,
  )(implicit traceContext: TraceContext): BlacklistLeaderSelectionPolicyState = {
    val nodeAt = snapshot.nodeActiveAt.getOrElse(
      thisNode,
      failBootstrap("Activation information is required when onboarding but it's empty")(
        traceContext
      ),
    )

    val state = nodeAt.leaderSelectionPolicyState.getOrElse(
      failBootstrap(
        "Leader selection policy state is required when onboarding but it's empty"
      )(traceContext)
    )
    awaitFuture(
      moduleSystem,
      store.insertLeaderSelectionPolicyState(state.epochNumber, state),
      "store leader selection policy state from snapshot",
    )
    state
  }

  private def stateFromStore(
      moduleSystem: ModuleSystem[E],
      epochNumber: EpochNumber,
  )(implicit
      traceContext: TraceContext
  ): BlacklistLeaderSelectionPolicyState =
    if (epochNumber == Genesis.GenesisEpochNumber) {
      val state = BlacklistLeaderSelectionPolicyState.FirstBlacklistLeaderSelectionPolicyState(
        protocolVersion
      )
      awaitFuture(
        moduleSystem,
        store.insertLeaderSelectionPolicyState(state.epochNumber, state),
        "store initial leader selection policy",
      )
      state
    } else {
      awaitFuture(
        moduleSystem,
        store.getLeaderSelectionPolicyState(epochNumber),
        s"fetching state to compute leaders for $epochNumber",
      ).getOrElse(
        failBootstrap(s"Could not fetch state for $epochNumber")(traceContext)
      )
    }

  private def awaitFuture[X](
      moduleSystem: ModuleSystem[E],
      future: E#FutureUnlessShutdownT[X],
      description: String,
  )(implicit traceContext: TraceContext): X = {
    logger.debug(description)
    moduleSystem.rootActorContext.blockingAwait(
      future,
      timeouts.default.asFiniteApproximation,
    )
  }

}
