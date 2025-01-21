// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.HasDelayedInit
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.leaders.SimpleLeaderSelectionPolicy
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import EpochState.Epoch
import IssConsensusModule.DefaultDatabaseReadTimeout

final class PreIssConsensusModule[E <: Env[E]](
    initialMembership: Membership,
    initialCryptoProvider: CryptoProvider[E],
    epochLength: EpochLength,
    epochStore: EpochStore[E],
    sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
    clock: Clock,
    metrics: BftOrderingMetrics,
    segmentModuleRefFactory: SegmentModuleRefFactory[E],
    override val dependencies: ConsensusModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit mc: MetricsContext)
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
        val (initialEpochState, latestCompletedEpoch) = restoreEpochStateFromDB()
        val consensus = new IssConsensusModule(
          epochLength,
          IssConsensusModule.InitialState(
            sequencerSnapshotAdditionalInfo,
            initialMembership,
            initialCryptoProvider,
            initialEpochState,
            latestCompletedEpoch,
          ),
          epochStore,
          clock,
          metrics,
          segmentModuleRefFactory,
          initialMembership.myId,
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
  ): (EpochState[E], EpochStore.Epoch) = {

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

    // Set up the initial state of the Consensus module for the in-progress epoch.
    val epochState =
      PreIssConsensusModule.initialEpochState(
        initialMembership,
        initialCryptoProvider,
        clock,
        abortInit,
        latestCompletedEpochFromStore.lastBlockCommits,
        latestEpochFromStore,
        epochInProgress,
        metrics,
        loggerFactory,
        timeouts,
        segmentModuleRefFactory,
      )

    epochState -> latestCompletedEpochFromStore
  }
}

object PreIssConsensusModule {

  def initialEpochState[E <: Env[E]](
      initialMembership: Membership,
      initialCryptoProvider: CryptoProvider[E],
      clock: Clock,
      abortInit: String => Nothing,
      latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
      latestEpochFromStore: EpochStore.Epoch,
      epochInProgress: EpochStore.EpochInProgress,
      metrics: BftOrderingMetrics,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      segmentModuleFactory: SegmentModuleRefFactory[E],
  )(implicit mc: MetricsContext, context: E#ActorContextT[Consensus.Message[E]]): EpochState[E] = {

    val epoch = Epoch(
      latestEpochFromStore.info,
      initialMembership,
      SimpleLeaderSelectionPolicy,
    )

    new EpochState(
      epoch,
      clock,
      abortInit,
      metrics,
      segmentModuleFactory(
        context,
        epoch,
        initialCryptoProvider,
        latestCompletedEpochLastCommits,
        epochInProgress,
      ),
      loggerFactory = loggerFactory,
      timeouts = timeouts,
    )
  }
}
