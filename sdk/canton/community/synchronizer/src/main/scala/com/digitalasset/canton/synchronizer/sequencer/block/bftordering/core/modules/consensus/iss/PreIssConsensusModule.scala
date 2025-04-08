// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import cats.syntax.functor.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.HasDelayedInit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions.RetransmissionsManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.util.Random

import EpochState.Epoch
import IssConsensusModule.DefaultDatabaseReadTimeout

final class PreIssConsensusModule[E <: Env[E]](
    bootstrapTopologyInfo: OrderingTopologyInfo[E],
    epochLength: EpochLength,
    epochStore: EpochStore[E],
    sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
    clock: Clock,
    metrics: BftOrderingMetrics,
    segmentModuleRefFactory: SegmentModuleRefFactory[E],
    random: Random,
    override val dependencies: ConsensusModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit mc: MetricsContext, config: BftBlockOrdererConfig)
    extends Consensus[E]
    with HasDelayedInit[Consensus.Message[E]] {

  override def ready(self: ModuleRef[Consensus.Message[E]]): Unit =
    self.asyncSend(Consensus.Init)

  override protected def receiveInternal(message: Consensus.Message[E])(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case Consensus.Init =>
        val (initialEpochState, latestCompletedEpoch, previousEpochsCommitCerts) =
          restoreEpochStateFromDB()
        val consensus = new IssConsensusModule(
          epochLength,
          IssConsensusModule.InitialState(
            bootstrapTopologyInfo,
            initialEpochState,
            latestCompletedEpoch,
            sequencerSnapshotAdditionalInfo,
          ),
          epochStore,
          clock,
          metrics,
          segmentModuleRefFactory,
          new RetransmissionsManager[E](
            bootstrapTopologyInfo.thisNode,
            dependencies.p2pNetworkOut,
            abort,
            previousEpochsCommitCerts,
            loggerFactory,
          ),
          random,
          dependencies,
          loggerFactory,
          timeouts,
        )()()
        context.become(consensus)
        // This will send all queued messages to the proper Consensus module.
        initCompleted(consensus.receive(_))
      case message =>
        ifInitCompleted(message) { _ =>
          abortInit(s"${this.getClass.toString} shouldn't receive any messages after init")
        }
    }

  @VisibleForTesting
  private[bftordering] def restoreEpochStateFromDB()(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): (EpochState[E], EpochStore.Epoch, Map[EpochNumber, Seq[CommitCertificate]]) = {

    val latestCompletedEpochFromStore =
      context.blockingAwait(
        epochStore.latestEpoch(includeInProgress = false),
        DefaultDatabaseReadTimeout,
      )

    val latestEpochFromStore =
      context.blockingAwait(
        epochStore.latestEpoch(includeInProgress = true),
        DefaultDatabaseReadTimeout,
      )

    // This query will return the in-progress epoch regardless if it has already been started or not.
    val epochInProgress =
      context.blockingAwait(
        epochStore.loadEpochProgress(latestEpochFromStore.info),
        DefaultDatabaseReadTimeout,
      )

    val previousEpochsCommitCerts = PreIssConsensusModule.loadPreviousEpochCommitCertificates(
      epochStore
    )(latestCompletedEpochFromStore.info.number, RetransmissionsManager.HowManyEpochsToKeep)

    // Set up the initial state of the Consensus module for the in-progress epoch.
    val epochState = initialEpochState(
      latestCompletedEpochFromStore.lastBlockCommits,
      latestEpochFromStore,
      epochInProgress,
    )

    (epochState, latestCompletedEpochFromStore, previousEpochsCommitCerts)
  }

  private def initialEpochState(
      latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
      latestEpochFromStore: EpochStore.Epoch,
      epochInProgress: EpochStore.EpochInProgress,
  )(implicit mc: MetricsContext, context: E#ActorContextT[Consensus.Message[E]]): EpochState[E] = {
    val epoch = Epoch(
      latestEpochFromStore.info,
      bootstrapTopologyInfo.currentMembership,
      bootstrapTopologyInfo.previousMembership,
    )

    new EpochState(
      epoch,
      clock,
      abortInit,
      metrics,
      segmentModuleRefFactory(
        context,
        epoch,
        bootstrapTopologyInfo.currentCryptoProvider,
        latestCompletedEpochLastCommits,
        epochInProgress,
      ),
      epochInProgress.completedBlocks,
      loggerFactory = loggerFactory,
      timeouts = timeouts,
    )
  }
}

object PreIssConsensusModule {

  /** @return
    *   map from epoch number to a list of commit certificates sorted by block number, for the last
    *   how many epochs from the latest completed epoch (inclusive).
    */
  @VisibleForTesting
  def loadPreviousEpochCommitCertificates[E <: Env[E]](epochStore: EpochStore[E])(
      lastCompletedEpoch: EpochNumber,
      numberOfEpochsToLoad: Int,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Map[EpochNumber, Seq[CommitCertificate]] = {
    val completedBlocks = context.blockingAwait(
      epochStore.loadCompleteBlocks(
        EpochNumber(lastCompletedEpoch - numberOfEpochsToLoad + 1),
        EpochNumber(lastCompletedEpoch),
      ),
      DefaultDatabaseReadTimeout,
    )
    completedBlocks.groupBy(_.epochNumber).fmap(_.sortBy(_.blockNumber).map(_.commitCertificate))
  }
}
