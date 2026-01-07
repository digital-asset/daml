// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

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

trait LeaderSelectionPolicy[E <: Env[E]] {

  def getLeaders(
      orderingTopology: OrderingTopology,
      epochNumber: EpochNumber,
  ): Seq[BftNodeId]

  def addBlock(
      epochNumber: EpochNumber,
      orderedBlockNumber: BlockNumber,
      viewNumber: ViewNumber,
  ): Unit

  /** Computes the first block we need to [[addBlock]] from. We are snapshotting the state at the
    * epoch boundary so this will usually be the first block in the epoch. If we don't rely on
    * replaying old blocks this will return [[scala.None$]].
    */
  def firstBlockWeNeedToAdd: Option[BlockNumber]

  def getHistoricState(
      epochNumber: EpochNumber
  )(implicit
      futureContext: FutureContext[E],
      traceContext: TraceContext,
  ): E#FutureUnlessShutdownT[Option[BlacklistLeaderSelectionPolicyState]]

  def saveStateFor(epochNumber: EpochNumber, orderingTopology: OrderingTopology)(implicit
      futureContext: FutureContext[E],
      traceContext: TraceContext,
  ): E#FutureUnlessShutdownT[Unit]
}

object LeaderSelectionPolicy {

  def rotateLeaders(
      originalLeaders: SortedSet[BftNodeId],
      epochNumber: EpochNumber,
  ): Seq[BftNodeId] = {
    val splitIndex = (epochNumber % originalLeaders.size).toInt
    originalLeaders.drop(splitIndex).toSeq ++ originalLeaders.take(splitIndex).toSeq
  }
}
