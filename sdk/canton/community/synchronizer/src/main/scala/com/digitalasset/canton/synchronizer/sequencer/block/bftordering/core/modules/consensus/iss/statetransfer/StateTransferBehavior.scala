// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions.RetransmissionsManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferBehavior.StateTransferType.Onboarding
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferBehavior.{
  InitialState,
  StateTransferType,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput.Mode
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable
import scala.util.{Failure, Success}

/** A state transfer behavior for [[IssConsensusModule]]. It uses [[StateTransferManager]] and
  * inherits its limitations. In particular, the topology at the catch-up starting epoch is assumed
  * to allow the whole catch-up process to complete and, if this is not the case, the catch-up will
  * fail and the node will need to be re-onboarded.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class StateTransferBehavior[E <: Env[E]](
    private val epochLength: EpochLength, // Currently fixed for all epochs
    private val initialState: InitialState[E],
    stateTransferType: StateTransferType,
    catchupDetector: CatchupDetector,
    epochStore: EpochStore[E],
    clock: Clock,
    metrics: BftOrderingMetrics,
    segmentModuleRefFactory: SegmentModuleRefFactory[E],
    override val dependencies: ConsensusModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(private val maybeCustomStateTransferManager: Option[StateTransferManager[E]] = None)(implicit
    mc: MetricsContext,
    config: BftBlockOrdererConfig,
) extends Consensus[E] {

  private val thisNode = initialState.topologyInfo.thisNode

  private val postponedQueue = new mutable.Queue[Consensus.Message[E]]()

  private val stateTransferManager = maybeCustomStateTransferManager.getOrElse(
    new StateTransferManager(
      thisNode,
      dependencies,
      epochLength,
      epochStore,
      loggerFactory,
    )
  )

  private var epochState = initialState.epochState

  private var latestCompletedEpoch: EpochStore.Epoch = initialState.latestCompletedEpoch

  private var activeTopologyInfo: OrderingTopologyInfo[E] = initialState.topologyInfo

  private var cancelled = 0

  override def ready(self: ModuleRef[Consensus.Message[E]]): Unit =
    self.asyncSend(Consensus.Init)

  override protected def receiveInternal(
      message: Consensus.Message[E]
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    lazy val messageType = shortType(message)

    message match {
      case Consensus.Init =>
        // Note that for onboarding, segments are created but not started
        initialState.epochState
          .notifyEpochCancellationToSegments(initialState.epochState.epoch.info.number)

      case Consensus.SegmentCancelledEpoch =>
        cancelled += 1
        if (initialState.epochState.epoch.segments.sizeIs == cancelled) {
          if (stateTransferManager.inBlockTransfer) {
            abort(
              s"$messageType: $stateTransferType state transfer cannot be already in progress during another state transfer"
            )
          }
          val startEpochNumber = initialState.stateTransferStartEpoch
          val startEpochInfo = initialState.epochState.epoch.info
          val membership = initialState.topologyInfo.currentMembership
          val cryptoProvider = initialState.topologyInfo.currentCryptoProvider

          stateTransferManager.startStateTransfer(
            membership,
            cryptoProvider,
            initialState.latestCompletedEpoch,
            startEpochNumber,
          )(abort)

          if (stateTransferType == Onboarding) {
            // Storing this epoch should theoretically allow catching up after a restart during onboarding.
            //  However, sequencer node crash recovery is not supported during onboarding.
            storeNewEpoch(startEpochInfo, messageType)
          }
        }

      case stateTransferMessage: Consensus.StateTransferMessage =>
        handleStateTransferMessage(stateTransferMessage)

      case newEpochTopologyMessage @ Consensus.NewEpochTopology(
            newEpochNumber,
            newMembership,
            newCryptoProvider: CryptoProvider[E],
            previousEpochMaxBftTime,
            lastBlockFromPreviousEpochMode,
          ) =>
        val currentEpochInfo = epochState.epoch.info
        logger.info(
          s"$messageType: setting latest completed epoch $currentEpochInfo from $stateTransferType state transfer"
        )
        // Providing an empty commit message set, which results in using the BFT time monotonicity adjustment for
        //  the first block produced by the state-transferred node as a leader.
        val lastCommitSet = Seq.empty
        latestCompletedEpoch = EpochStore.Epoch(currentEpochInfo, lastCommitSet)

        // Storing epochs during state transfer is done on a best effort basis solely based on NewEpochTopology messages
        //  and independently of storing OrderedBlocks (PrePrepares) in StateTransferManager. This allows a slightly
        //  better crash recovery experience, where state transfer can be resumed from a further epoch than the start
        //  one, once at least one NewEpochTopology message has been received from the Output module.
        //  Other than the above, crash recovery during state transfer depends on idempotency of the stores.
        storeCompletedEpoch(currentEpochInfo, messageType)

        if (lastBlockFromPreviousEpochMode == Mode.StateTransfer.LastBlock) {
          logger.info(
            s"$messageType: received first epoch ($newEpochNumber) after $stateTransferType state transfer, " +
              s"transitioning back to consensus"
          )
          transitionBackToConsensus(newEpochTopologyMessage)
        } else {
          val latestCompletedEpochNumber = latestCompletedEpoch.info.number
          val currentEpochNumber = currentEpochInfo.number

          if (
            newEpochNumber == currentEpochNumber + 1 && newEpochNumber == latestCompletedEpochNumber + 1
          ) {
            val orderingTopology = initialState.topologyInfo.currentTopology
            val newEpochInfo =
              currentEpochInfo.next(
                epochLength,
                orderingTopology.activationTime,
                previousEpochMaxBftTime,
              )
            storeNewEpoch(newEpochInfo, messageType)
            logger.debug(
              s"$messageType: setting new epoch ${newEpochInfo.number} during $stateTransferType state transfer"
            )
            setNewEpochState(newEpochInfo, newMembership, newCryptoProvider)
          } else {
            abort(
              s"$messageType: internal inconsistency,  $stateTransferType state transfer received wrong new topology " +
                s"for epoch $newEpochNumber, expected ${currentEpochNumber + 1}"
            )
          }
        }

      case Consensus.ConsensusMessage.AsyncException(e) =>
        logger.error(s"$messageType: exception raised from async consensus message: ${e.toString}")

      case _ => postponedQueue.enqueue(message)
    }
  }

  private def storeNewEpoch(
      newEpochInfo: EpochInfo,
      messageType: => String,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    logger.debug(s"$messageType: storing new epoch ${newEpochInfo.number}")
    context.pipeToSelf(epochStore.startEpoch(newEpochInfo)) {
      case Failure(exception) => Some(Consensus.ConsensusMessage.AsyncException(exception))
      case Success(_) =>
        logger.debug(s"$messageType: stored new epoch ${newEpochInfo.number}")
        None
    }
  }

  private def storeCompletedEpoch(epochInfo: EpochInfo, messageType: => String)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    logger.debug(s"$messageType: storing complete epoch ${epochInfo.number}")
    context.pipeToSelf(epochStore.completeEpoch(epochInfo.number)) {
      case Failure(exception) => Some(Consensus.ConsensusMessage.AsyncException(exception))
      case Success(_) =>
        logger.debug(s"$messageType: complete epoch ${epochInfo.number} stored")
        None
    }
  }

  private def handleStateTransferMessage(
      stateTransferMessage: Consensus.StateTransferMessage
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    lazy val messageType = shortType(stateTransferMessage)

    val result =
      stateTransferManager.handleStateTransferMessage(
        stateTransferMessage,
        initialState.topologyInfo,
        initialState.latestCompletedEpoch,
      )(abort)

    handleStateTransferMessageResult(messageType, result)
  }

  @VisibleForTesting
  private[bftordering] def handleStateTransferMessageResult(
      messageType: => String,
      maybeNewEpochState: StateTransferMessageResult,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    maybeNewEpochState match {
      case StateTransferMessageResult.Continue =>

      case StateTransferMessageResult.NothingToStateTransfer =>
        abort(
          s"$messageType: internal inconsistency, need $stateTransferType state transfer but nothing to transfer"
        )

      case StateTransferMessageResult.BlockTransferCompleted(
            endEpochNumber,
            numberOfTransferredEpochs,
            numberOfTransferredBlocks,
          ) =>
        logger.info(
          s"$messageType: $stateTransferType block transfer completed at epoch $endEpochNumber with " +
            s"$numberOfTransferredEpochs epochs ($numberOfTransferredBlocks blocks) transferred"
        )
    }

  private def transitionBackToConsensus(newEpochTopologyMessage: Consensus.NewEpochTopology[E])(
      implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    // Transition back to consensus but do not start it: the CFT code path is triggered, i.e., the consensus module
    //  will wait for the new epoch's topology message, update its state accordingly and only then
    //  create the segment modules and start the consensus protocol.
    //  The CFT code path only uses the last completed info and the new epoch number to update the state once
    //  a topology is received from the output module, however we need to provide the full consensus initial state.
    //  TODO(#22849) refactor so that the consensus module can be constructed only with the needed info in case
    //   of crash recovery and catch-up
    val consensusInitialState =
      IssConsensusModule.InitialState(
        activeTopologyInfo,
        epochState,
        latestCompletedEpoch,
        sequencerSnapshotAdditionalInfo = None,
      )
    val consensusBehavior = new IssConsensusModule[E](
      epochLength,
      consensusInitialState,
      epochStore,
      clock,
      metrics,
      segmentModuleRefFactory,
      new RetransmissionsManager[E](
        thisNode,
        dependencies.p2pNetworkOut,
        abort,
        previousEpochsCommitCerts = Map.empty,
        loggerFactory,
      ),
      dependencies,
      loggerFactory,
      timeouts,
      futurePbftMessageQueue = initialState.pbftMessageQueue,
      queuedConsensusMessages = postponedQueue.toSeq,
    )()(catchupDetector)

    context.become(consensusBehavior)

    // Send the new epoch topology to consensus to kick it off after state transfer
    consensusBehavior.receive(newEpochTopologyMessage)
  }

  private def setNewEpochState(
      newEpochInfo: EpochInfo,
      newMembership: Membership,
      newCryptoProvider: CryptoProvider[E],
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    activeTopologyInfo = activeTopologyInfo.updateMembership(newMembership, newCryptoProvider)
    val currentMembership = activeTopologyInfo.currentMembership
    catchupDetector.updateMembership(currentMembership)

    val newEpoch =
      Epoch(
        newEpochInfo,
        currentMembership,
        activeTopologyInfo.previousMembership,
      )

    epochState = new EpochState(
      newEpoch,
      clock,
      abort,
      metrics,
      segmentModuleRefFactory = (_, _) =>
        abort(
          "Internal inconsistency, segment module factory should not be used during state transfer"
        ),
      completedBlocks = Seq.empty,
      loggerFactory = loggerFactory,
      timeouts = timeouts,
    )
  }
}

object StateTransferBehavior {

  sealed trait StateTransferType extends Product with Serializable

  object StateTransferType {
    case object Onboarding extends StateTransferType
    case object Catchup extends StateTransferType
  }

  final case class InitialState[E <: Env[E]](
      stateTransferStartEpoch: EpochNumber,
      topologyInfo: OrderingTopologyInfo[E],
      epochState: EpochState[E],
      latestCompletedEpoch: EpochStore.Epoch,
      pbftMessageQueue: mutable.Queue[SignedMessage[PbftNetworkMessage]],
  )

  @VisibleForTesting
  private[bftordering] def unapply(
      behavior: StateTransferBehavior[?]
  ): Option[
    (
        EpochLength,
        EpochNumber,
        OrderingTopologyInfo[?],
        EpochInfo,
        EpochStore.Epoch,
    )
  ] =
    Some(
      (
        behavior.epochLength,
        behavior.initialState.stateTransferStartEpoch,
        behavior.activeTopologyInfo,
        behavior.epochState.epoch.info,
        behavior.latestCompletedEpoch,
      )
    )
}
