// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
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
    protocolVersion: ProtocolVersion,
    store: OutputMetadataStore[E],
    timeouts: ProcessingTimeout,
    failBootstrap: String => TraceContext => Nothing,
    override val loggerFactory: NamedLoggerFactory,
) extends LeaderSelectionInitializer[E]
    with NamedLogging {

  def stateForInitial(
      moduleSystem: ModuleSystem[E],
      value: Option[SequencerSnapshotAdditionalInfo],
      epochNumber: EpochNumber,
  )(implicit traceContext: TraceContext): BlacklistLeaderSelectionPolicyState =
    value match {
      case Some(snapshot) =>
        computeStateFromSnapshot(snapshot)

      case None =>
        stateFromStore(moduleSystem, epochNumber)
    }

  def leaderFromState(
      state: BlacklistLeaderSelectionPolicyState,
      orderingTopology: OrderingTopology,
  ): Seq[BftNodeId] =
    state.computeLeaders(
      orderingTopology,
      config.blacklistLeaderSelectionPolicyConfig,
    )

  def leaderSelectionPolicy(
      blacklistLeaderSelectionPolicyState: BlacklistLeaderSelectionPolicyState,
      orderingTopology: OrderingTopology,
  ): LeaderSelectionPolicy[E] = BlacklistLeaderSelectionPolicy.create(
    blacklistLeaderSelectionPolicyState,
    config,
    orderingTopology,
    store,
  )

  private def computeStateFromSnapshot(
      snapshot: SequencerSnapshotAdditionalInfo
  )(implicit traceContext: TraceContext): BlacklistLeaderSelectionPolicyState = {
    val nodeAt = snapshot.nodeActiveAt.getOrElse(
      thisNode,
      failBootstrap("Activation information is required when onboarding but it's empty")(
        traceContext
      ),
    )

    nodeAt.leaderSelectionPolicyState.getOrElse(
      failBootstrap(
        "Leader selection policy state is required when onboarding but it's empty"
      )(traceContext)
    )
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
