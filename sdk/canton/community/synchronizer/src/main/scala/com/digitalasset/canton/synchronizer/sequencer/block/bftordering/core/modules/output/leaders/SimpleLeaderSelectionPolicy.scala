// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.LeaderSelectionPolicy.rotateLeaders
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  FutureContext,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.immutable.SortedSet

/** A simple leader selection policy based on the one from the ISS paper. It returns a sorted set of
  * all nodes.
  */
class SimpleLeaderSelectionPolicy[E <: Env[E]] extends LeaderSelectionPolicy[E] {

  def selectLeaders(nodes: OrderingTopology): SortedSet[BftNodeId] =
    SortedSet.from(nodes.nodes)

  override def getLeaders(
      orderingTopology: OrderingTopology,
      epochNumber: EpochNumber,
  ): Seq[BftNodeId] =
    rotateLeaders(selectLeaders(orderingTopology), epochNumber)

  override def addBlock(
      epochNumber: EpochNumber,
      orderedBlockNumber: BlockNumber,
      viewNumber: ViewNumber,
  ): Unit = ()

  override def firstBlockWeNeedToAdd: Option[BlockNumber] = None

  override def getHistoricState(
      epochNumber: EpochNumber
  )(implicit
      futureContext: FutureContext[E],
      traceContext: TraceContext,
  ): E#FutureUnlessShutdownT[Option[BlacklistLeaderSelectionPolicyState]] =
    futureContext.pureFuture(None)

  override def saveStateFor(epochNumber: EpochNumber, orderingTopology: OrderingTopology)(implicit
      futureContext: FutureContext[E],
      traceContext: TraceContext,
  ): E#FutureUnlessShutdownT[Unit] =
    futureContext.pureFuture(())

}
