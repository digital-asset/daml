// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
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

class SimpleLeaderSelectionPolicyInitializer[E <: Env[E]](protocolVersion: ProtocolVersion)
    extends LeaderSelectionInitializer[E] {

  private val leaderSelectionPolicy = new SimpleLeaderSelectionPolicy[E]

  override def stateForInitial(
      moduleSystem: ModuleSystem[E],
      value: Option[SequencerSnapshotAdditionalInfo],
      epochNumber: EpochNumber,
  )(implicit traceContext: TraceContext): BlacklistLeaderSelectionPolicyState =
    BlacklistLeaderSelectionPolicyState.create(epochNumber, BlockNumber(0L), Map.empty)(
      protocolVersion
    )

  override def leaderFromState(
      state: BlacklistLeaderSelectionPolicyState,
      orderingTopology: OrderingTopology,
  ): Seq[BftNodeId] = leaderSelectionPolicy.getLeaders(orderingTopology, state.epochNumber)

  override def leaderSelectionPolicy(
      blacklistLeaderSelectionPolicyState: BlacklistLeaderSelectionPolicyState,
      orderingTopology: OrderingTopology,
  ): LeaderSelectionPolicy[E] = leaderSelectionPolicy
}
