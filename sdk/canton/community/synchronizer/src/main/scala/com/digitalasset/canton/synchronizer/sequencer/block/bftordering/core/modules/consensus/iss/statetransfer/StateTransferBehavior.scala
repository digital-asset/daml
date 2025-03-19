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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
  PureFun,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable
import scala.util.{Failure, Success}

/** A state transfer behavior for [[IssConsensusModule]]. There are 2 types of state transfer:
  * onboarding (for new nodes) and catch-up (for lagging-behind nodes). These two types work
  * similarly, differing only in the triggering mechanism. State transfer requests commit
  * certificates from a single node at a time and advances the topology accordingly, supporting
  * multiple topology changes throughout the process. Thus, it depends on historical topology state
  * from the latest pruning point. Moreover, it might use keys that are currently revoked (and even
  * compromised) in signature verification during at least part of the process. The latter
  * limitation is acceptable because key rotation, combined with regular pruning, generally prevents
  * compromised keys in Canton.
  *
  * The flow is as follows:
  *   - Cancel existing segment modules.
  *   - If it's onboarding, start by storing the first epoch.
  *   - Request blocks from the current epoch (also starts catch-up).
  *   - Receive blocks one by one from a single epoch (to avoid exceeding the maximum gRPC message
  *     size and OOM errors). In the future, we might want to pull blocks from more epochs for
  *     better performance.
  *   - Once all blocks from the epoch are validated and stored, wait for a NewEpochTopology message
  *     from the Output module (indicating that all relevant batches have been fetched).
  *   - Store both the completed epoch and the new (subsequent) epoch in the epoch store.
  *   - Repeat the process by requesting blocks from the new epoch.
  *   - Upon receiving a NewEpochTopology message for the first epoch after state transfer, store
  *     the last state-transferred epoch and complete state transfer by transitioning to the default
  *     [[IssConsensusModule]] behavior. At this point, (re)send the NewEpochTopology message.
  *
  * Crash Recovery: Since state transfer sequentially stores all new and completed epochs, it
  * inherits the crash-fault tolerance capabilities of the BFT orderer. Additionally, it still
  * depends on the idempotency of the stores when state-transferring blocks/batches from partially
  * transferred epochs after a restart.
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
          val startEpochInfo = initialState.epochState.epoch.info
          val membership = initialState.topologyInfo.currentMembership
          val cryptoProvider = initialState.topologyInfo.currentCryptoProvider

          stateTransferType match {
            case StateTransferType.Onboarding =>
              startOnboarding(startEpochInfo, membership, cryptoProvider, messageType)
            case StateTransferType.Catchup =>
              stateTransferManager.startCatchUp(
                membership,
                cryptoProvider,
                initialState.latestCompletedEpoch,
                initialState.stateTransferStartEpoch,
              )(abort)
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
        if (lastBlockFromPreviousEpochMode == Mode.StateTransfer.LastBlock) {
          logger.info(
            s"$messageType: received first epoch (${newEpochTopologyMessage.epochNumber}) after $stateTransferType " +
              s"state transfer, storing it"
          )
          completeStateTransfer(newEpochTopologyMessage, messageType)
        } else {
          val currentEpochNumber = currentEpochInfo.number
          if (newEpochNumber == currentEpochNumber + 1) {
            val newEpochInfo =
              currentEpochInfo.next(
                epochLength,
                newMembership.orderingTopology.activationTime,
                previousEpochMaxBftTime,
              )
            storeEpochs(
              currentEpochInfo,
              newEpochInfo,
              newMembership,
              newCryptoProvider,
              messageType,
            )
          } else {
            abort(
              s"$messageType: internal inconsistency, $stateTransferType state transfer received wrong new topology " +
                s"for epoch $newEpochNumber, expected ${currentEpochNumber + 1}"
            )
          }
        }

      case Consensus.NewEpochStored(newEpochInfo, membership, cryptoProvider: CryptoProvider[E]) =>
        logger.debug(
          s"$messageType: setting new epoch ${newEpochInfo.number} during $stateTransferType state transfer"
        )
        setNewEpochState(newEpochInfo, membership, cryptoProvider)
        stateTransferManager.stateTransferNewEpoch(
          newEpochInfo.number,
          membership,
          initialState.topologyInfo.currentCryptoProvider, // used only for signing the request
        )(abort)

      case Consensus.StateTransferCompleted(newEpochTopologyMessage) =>
        val currentEpochInfo = epochState.epoch.info
        // Providing an empty commit message set, which results in using the BFT time monotonicity adjustment for
        //  the first block produced by the state-transferred node as a leader.
        val lastCommitSet = Seq.empty
        latestCompletedEpoch = EpochStore.Epoch(currentEpochInfo, lastCommitSet)
        logger.info(
          s"$messageType: completed $stateTransferType state transfer (including batches), transitioning back to consensus"
        )
        transitionBackToConsensus(newEpochTopologyMessage)

      case Consensus.ConsensusMessage.AsyncException(e) =>
        logger.error(s"$messageType: exception raised from async consensus message: ${e.toString}")

      case _ => postponedQueue.enqueue(message)
    }
  }

  private def startOnboarding(
      startEpochInfo: EpochInfo,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
      messageType: => String,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val startEpochNumber = startEpochInfo.number
    // State transfer will be started once the NewEpochStored message is received.
    logger.debug(s"$messageType: storing start epoch $startEpochNumber")
    // Storing this epoch should theoretically allow catching up after a restart during onboarding.
    //  However, sequencer node crash recovery is not supported during onboarding.
    pipeToSelf(epochStore.startEpoch(startEpochInfo)) {
      case Failure(exception) => Consensus.ConsensusMessage.AsyncException(exception)
      case Success(_) =>
        logger.debug(s"$messageType: stored start epoch $startEpochNumber")
        Consensus.NewEpochStored(startEpochInfo, membership, cryptoProvider)
    }
  }

  private def handleStateTransferMessage(
      stateTransferMessage: Consensus.StateTransferMessage
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    lazy val messageType = shortType(stateTransferMessage)

    val result =
      stateTransferManager.handleStateTransferMessage(
        stateTransferMessage,
        activeTopologyInfo,
        latestCompletedEpoch,
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
          ) =>
        logger.info(
          s"$messageType: $stateTransferType block transfer completed at epoch $endEpochNumber with " +
            s"$numberOfTransferredEpochs epochs transferred"
        )
    }

  private def storeEpochs(
      currentEpochInfo: EpochInfo,
      newEpochInfo: EpochInfo,
      newMembership: Membership,
      newCryptoProvider: CryptoProvider[E],
      messageType: => String,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val currentEpochNumber = currentEpochInfo.number
    val newEpochNumber = newEpochInfo.number
    logger.debug(
      s"$messageType: storing completed epoch $currentEpochNumber and new epoch $newEpochNumber"
    )
    pipeToSelf(
      context.flatMapFuture(
        epochStore.completeEpoch(currentEpochNumber),
        PureFun.Const(epochStore.startEpoch(newEpochInfo)),
      )
    ) {
      case Failure(exception) => Consensus.ConsensusMessage.AsyncException(exception)
      case Success(_) =>
        logger.debug(
          s"$messageType: stored completed epoch $currentEpochNumber and new epoch $newEpochNumber"
        )
        Consensus.NewEpochStored(newEpochInfo, newMembership, newCryptoProvider)
    }
  }

  private def completeStateTransfer(
      newEpochTopologyMessage: Consensus.NewEpochTopology[E],
      messageType: => String,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val lastStateTransferredEpochNumber = epochState.epoch.info.number
    logger.debug(
      s"$messageType: storing last state-transferred epoch $lastStateTransferredEpochNumber"
    )
    pipeToSelf(epochStore.completeEpoch(lastStateTransferredEpochNumber)) {
      case Failure(exception) => Consensus.ConsensusMessage.AsyncException(exception)
      case Success(_) =>
        logger.debug(
          s"$messageType: stored last state-transferred epoch $lastStateTransferredEpochNumber"
        )
        Consensus.StateTransferCompleted(newEpochTopologyMessage)
    }
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

    latestCompletedEpoch = EpochStore.Epoch(
      epochState.epoch.info,
      lastBlockCommits = Seq.empty, // not relevant in the middle of state transfer
    )

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
