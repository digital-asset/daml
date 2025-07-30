// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  FutureContext,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class BlacklistLeaderSelectionPolicy[E <: Env[E]](
    initialState: BlacklistLeaderSelectionPolicyState,
    initialOrderingTopology: OrderingTopology,
    config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig,
    epochLength: EpochLength,
    store: OutputMetadataStore[E],
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val metricsContext: MetricsContext)
    extends LeaderSelectionPolicy[E]
    with NamedLogging {

  private var state = initialState

  private var blockToLeader: Map[BlockNumber, BftNodeId] =
    initialState.computeBlockToLeader(initialOrderingTopology, config, epochLength)

  private val nodesToPunish: mutable.Set[BftNodeId] = mutable.Set.empty

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  override def addBlock(
      epochNumber: EpochNumber,
      orderedBlockNumber: BlockNumber,
      viewNumber: ViewNumber,
  ): Unit = {
    implicit val tc: TraceContext = TraceContext.empty
    logger.trace(s"Adding $orderedBlockNumber | $viewNumber (epoch $epochNumber) ")
    if (epochNumber < state.epochNumber) {
      // After a restart we might reprocess old blocks in output module. We ignore them here
      return
    }

    require(
      epochNumber == state.epochNumber,
      s"${getClass.getName} received confirmation of block $orderedBlockNumber in epoch $epochNumber, but we are in ${state.epochNumber}",
    )

    if (viewNumber != ViewNumber.First) {
      // The PrePrepare for this block is not in the first view number which means that the original leader was not able
      // to propose all block in their segment. As such we will punish the node in the next epoch.
      val originalLeader = blockToLeader(orderedBlockNumber)
      punish(originalLeader)
    }
  }

  override def firstBlockWeNeedToAdd: Option[BlockNumber] = Some(state.startBlock)

  override def getLeaders(
      orderingTopology: OrderingTopology,
      epochNumber: EpochNumber,
  ): Seq[BftNodeId] = {
    assert(
      state.epochNumber == epochNumber,
      s"Leader selection is asked for leaders in epoch $epochNumber but expects epoch ${state.epochNumber}",
    )

    state.computeLeaders(orderingTopology, config)
  }

  private def updateState(
      topology: OrderingTopology,
      epochNumber: EpochNumber,
  ): Unit = {
    assert(EpochNumber(state.epochNumber + 1) == epochNumber)

    logger.trace(s"old blacklist state $state")(TraceContext.empty)
    state = state.update(topology, config, epochLength, blockToLeader, nodesToPunish.toSet)
    logger.trace(s"new blacklist state $state")(TraceContext.empty)
    nodesToPunish.clear()

    updateMetrics(topology)

    val newBlockToLeader = state.computeBlockToLeader(topology, config, epochLength)
    blockToLeader = newBlockToLeader
  }

  private def punish(node: BftNodeId): Unit =
    nodesToPunish.add(node).discard

  override def getHistoricState(
      epochNumber: EpochNumber
  )(implicit
      futureContext: FutureContext[E],
      traceContext: TraceContext,
  ): E#FutureUnlessShutdownT[Option[BlacklistLeaderSelectionPolicyState]] =
    store.getLeaderSelectionPolicyState(epochNumber)

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  override def saveStateFor(epochNumber: EpochNumber, orderingTopology: OrderingTopology)(implicit
      futureContext: FutureContext[E],
      traceContext: TraceContext,
  ): E#FutureUnlessShutdownT[Unit] = {
    if (epochNumber <= state.epochNumber) {
      // In case of restart we can see old epochs again, in that case ignore
      return futureContext.pureFuture(())
    }
    assert(
      epochNumber == EpochNumber(state.epochNumber + 1),
      s"saveStateFor $epochNumber but the state is ${state.epochNumber}",
    )
    updateState(orderingTopology, epochNumber)

    store.insertLeaderSelectionPolicyState(epochNumber, state)
  }

  private def updateMetrics(topology: OrderingTopology): Unit = {
    metrics.blacklistLeaderSelectionPolicyMetrics.cleanupBlacklistGauges(topology.nodes)

    topology.nodes.foreach { node =>
      val epochsBlacklisted: Long = state.blacklist.get(node) match {
        case Some(BlacklistStatus.OnTrial(numberOfConsecutiveFailedAttempts)) => 0L
        case Some(BlacklistStatus.Blacklisted(failedAttemptsBefore, epochsLeftUntilNewTrial)) =>
          epochsLeftUntilNewTrial
        case None => 0L
      }
      metrics.blacklistLeaderSelectionPolicyMetrics
        .blacklist(node)
        .updateValue(epochsBlacklisted)(metricsContext)
    }
  }
}

object BlacklistLeaderSelectionPolicy {

  type Blacklist = Map[BftNodeId, BlacklistStatus.BlacklistStatusMark]

  def create[E <: Env[E]](
      state: BlacklistLeaderSelectionPolicyState,
      config: BftBlockOrdererConfig,
      blacklistLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig,
      orderingTopology: OrderingTopology,
      store: OutputMetadataStore[E],
      metrics: BftOrderingMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit metricsContext: MetricsContext): BlacklistLeaderSelectionPolicy[E] =
    new BlacklistLeaderSelectionPolicy(
      state,
      orderingTopology,
      blacklistLeaderSelectionPolicyConfig,
      EpochLength(
        config.epochLength
      ), // TODO(#19289) support variable epoch lengths or leave the default if not relevant
      store,
      metrics,
      loggerFactory,
    )
}
