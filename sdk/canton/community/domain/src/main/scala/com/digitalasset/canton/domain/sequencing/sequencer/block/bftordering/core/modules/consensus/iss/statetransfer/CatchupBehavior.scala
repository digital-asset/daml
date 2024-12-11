// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.metrics.BftOrderingMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.CatchupBehavior.InitialState
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochLength
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable

/** A catch-up behavior for [[IssConsensusModule]].
  * It reuses [[StateTransferManager]] and inherits its logic and limitations.
  * In particular, the topology at the catch-up starting epoch is assumed to allow the whole catch-up process to
  * complete and, if this is not the case, the catch-up will fail and the node will need to be re-onboarded.
  */
final class CatchupBehavior[E <: Env[E]](
    private val epochLength: EpochLength, // Currently fixed for all epochs
    private val initialState: InitialState[E],
    epochStore: EpochStore[E],
    clock: Clock,
    metrics: BftOrderingMetrics,
    segmentModuleRefFactory: SegmentModuleRefFactory[E],
    override val dependencies: ConsensusModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(private val maybeCustomStateTransferManager: Option[StateTransferManager[E]] = None)(implicit
    mc: MetricsContext
) extends Consensus[E] {

  private val postponedQueue = new mutable.Queue[Consensus.Message[E]]()

  private val stateTransferManager = maybeCustomStateTransferManager.getOrElse(
    new StateTransferManager(
      dependencies,
      epochLength,
      epochStore,
      initialState.membership.myId,
      loggerFactory,
    )
  )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var cancelled = 0

  override def ready(self: ModuleRef[Consensus.Message[E]]): Unit =
    self.asyncSend(Consensus.Init)

  override protected def receiveInternal(
      message: Consensus.Message[E]
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    lazy val messageType = shortType(message)
    val epochState = initialState.epochState

    message match {
      case Consensus.Init =>
        epochState.cancelEpoch(epochState.epoch.info.number)

      case Consensus.CatchUpMessage.SegmentCancelledEpoch =>
        cancelled += 1
        if (epochState.epoch.segments.sizeIs == cancelled) {
          if (stateTransferManager.inStateTransfer) {
            abort(s"$messageType: state transfer cannot be already in progress during catch-up")
          }
          stateTransferManager.startStateTransfer(
            initialState.membership,
            initialState.latestCompletedEpoch,
            epochState.epoch.info.number,
          )(abort)
        }

      case stateTransferMessage: Consensus.StateTransferMessage =>
        handleStateTransferMessage(stateTransferMessage)

      case Consensus.ConsensusMessage.AsyncException(e) =>
        logger.error(s"$messageType: exception raised from async consensus message: ${e.toString}")

      case _ => postponedQueue.enqueue(message)
    }
  }

  private def handleStateTransferMessage(
      stateTransferMessage: Consensus.StateTransferMessage
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    lazy val messageType = shortType(stateTransferMessage)

    val maybeNewEpochState =
      stateTransferManager.handleStateTransferMessage(
        stateTransferMessage,
        initialState.membership,
        initialState.latestCompletedEpoch,
      )(abort)

    handleStasteTransferMessageResult(messageType, maybeNewEpochState)
  }

  @VisibleForTesting
  private[bftordering] def handleStasteTransferMessageResult(
      messageType: => String,
      maybeNewEpochState: StateTransferMessageResult,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    maybeNewEpochState match {
      case StateTransferMessageResult.Continue =>

      case StateTransferMessageResult.NothingToStateTransfer =>
        abort(s"$messageType: internal inconsistency, need to catch-up but nothing to transfer")

      case StateTransferMessageResult.BlockTransferCompleted(
            lastCompletedEpoch,
            lastCompletedEpochStored,
          ) =>
        logger.info(
          s"$messageType: catch-up state transfer completed last epoch $lastCompletedEpoch, " +
            s"stored epoch info = ${lastCompletedEpochStored.info}, transitioning back to consensus"
        )
        // Transition back to consensus but do not start it: the CFT code path is triggered, i.e., the consensus module
        //  will wait for the new epoch's topology from the output module, update its state accordingly and only then
        //  create the segment modules and start the consensus protocol.
        //  The CFT code path only uses the last completed info and the new epoch number to update the state once
        //  a topology is received from the output module, however we need to provide the full consensus initial state.
        //  TODO(#22849) refactor so that the consensus module can be constructed only with the needed info in case
        //   of crash recovery and catch-up
        val membership = initialState.membership
        val cryptoProvider = initialState.cryptoProvider

        def segmentModuleRefPartial(
            segmentState: SegmentState,
            epochMetricsAccumulator: EpochMetricsAccumulator,
        ): E#ModuleRefT[ConsensusSegment.Message] =
          segmentModuleRefFactory(
            context,
            lastCompletedEpoch,
            cryptoProvider,
            lastCompletedEpochStored.lastBlockCommitMessages,
            epochInProgress = EpochStore.EpochInProgress(
              completedBlocks = Seq.empty,
              pbftMessagesForIncompleteBlocks = Seq.empty,
            ),
          )(segmentState, epochMetricsAccumulator)

        val consensusInitialEpochState =
          new EpochState[E](
            lastCompletedEpoch,
            clock,
            abort,
            metrics,
            segmentModuleRefPartial,
            completedBlocks = Seq.empty,
            loggerFactory,
            timeouts,
          )
        val consensusInitialState =
          IssConsensusModule.InitialState(
            sequencerSnapshotAdditionalInfo = None,
            membership,
            cryptoProvider,
            consensusInitialEpochState,
            lastCompletedEpochStored,
          )
        context.become(
          new IssConsensusModule[E](
            epochLength,
            consensusInitialState,
            epochStore,
            clock,
            metrics,
            segmentModuleRefFactory,
            membership.myId,
            dependencies,
            loggerFactory,
            timeouts,
            futurePbftMessageQueue = initialState.pbftMessageQueue,
            queuedConsensusMessages = postponedQueue.toSeq,
          )()(
            catchupDetector = initialState.catchupDetector
          )
        )
    }
}

object CatchupBehavior {

  final case class InitialState[E <: Env[E]](
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
      epochState: EpochState[E],
      latestCompletedEpoch: EpochStore.Epoch,
      pbftMessageQueue: mutable.Queue[SignedMessage[PbftNetworkMessage]],
      catchupDetector: CatchupDetector,
  )

  @VisibleForTesting
  private[bftordering] def unapply(
      behavior: CatchupBehavior[?]
  ): Option[
    (
        EpochLength,
        Membership,
        EpochInfo,
        EpochStore.Epoch,
    )
  ] =
    Some(
      (
        behavior.epochLength,
        behavior.initialState.membership,
        behavior.initialState.epochState.epoch.info,
        behavior.initialState.latestCompletedEpoch,
      )
    )
}
