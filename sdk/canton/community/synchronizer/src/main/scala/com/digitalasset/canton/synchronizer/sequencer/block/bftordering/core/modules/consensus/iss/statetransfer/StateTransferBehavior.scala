// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.collection.FairBoundedQueue
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.collection.FairBoundedQueue.EnqueueResult
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  DelegationCryptoProvider,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
  PureFun,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.BoundedQueue.DropStrategy
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.util.{Failure, Random, Success}

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
  *   - Update the Availability topology.
  *   - Store both the completed epoch and the new (subsequent) epoch in the epoch store.
  *   - Repeat the process by requesting blocks from the next epoch.
  *   - Once there is nothing to transfer (and, if it's catch-up, a minimum end epoch has been
  *     reached), complete state transfer by transitioning to the default [[IssConsensusModule]]
  *     behavior.
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
    random: Random,
    override val dependencies: ConsensusModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(private val maybeCustomStateTransferManager: Option[StateTransferManager[E]] = None)(implicit
    synchronizerProtocolVersion: ProtocolVersion,
    config: BftBlockOrdererConfig,
    mc: MetricsContext,
) extends Consensus[E] {

  private val thisNode = initialState.topologyInfo.thisNode

  private var cancelledSegments = 0

  @VisibleForTesting
  private[bftordering] val postponedConsensusMessages: FairBoundedQueue[Consensus.Message[E]] =
    new FairBoundedQueue(
      config.consensusQueueMaxSize,
      config.consensusQueuePerNodeQuota,
      DropStrategy.DropNewest, // To ensure continuity of messages (and resort to another catch-up if necessary)
    )

  private val stateTransferManager = maybeCustomStateTransferManager.getOrElse(
    new StateTransferManager(
      thisNode,
      dependencies,
      epochLength,
      epochStore,
      random,
      metrics,
      loggerFactory,
    )()
  )

  // The default consensus behavior needs to get the state for the previous (the last state-transferred) topology
  //  before setting up the next one. Note that there is always at least one epoch to transfer.
  private var previousEpochState = initialState.epochState
  private var epochState = initialState.epochState
  private var previousTopologyInfo = initialState.topologyInfo
  private var activeTopologyInfo = initialState.topologyInfo

  private var latestCompletedEpoch = initialState.latestCompletedEpoch

  @VisibleForTesting
  private[bftordering] var maybeLastReceivedEpochTopology: Option[Consensus.NewEpochTopology[E]] =
    None

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
        cancelledSegments += 1
        if (initialState.epochState.epoch.segments.sizeIs == cancelledSegments) {
          if (stateTransferManager.inStateTransfer) {
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

      case newEpochTopologyMessage: Consensus.NewEpochTopology[E] =>
        val currentEpochInfo = epochState.epoch.info
        val currentEpochNumber = currentEpochInfo.number
        val newEpochNumber = newEpochTopologyMessage.epochNumber

        if (newEpochNumber == currentEpochNumber + 1) {
          stateTransferManager.cancelTimeoutForEpoch(currentEpochNumber)
          maybeLastReceivedEpochTopology = Some(newEpochTopologyMessage)

          // Update the active topology in Availability as well to use the most recently available topology
          //  to fetch batches.
          updateAvailabilityTopology(newEpochTopologyMessage)

          val newEpochInfo =
            currentEpochInfo.next(
              epochLength,
              newEpochTopologyMessage.membership.orderingTopology.activationTime,
            )
          storeEpochs(
            currentEpochInfo,
            newEpochInfo,
            newEpochTopologyMessage.membership,
            newEpochTopologyMessage.cryptoProvider,
            messageType,
          )
        } else {
          abort(
            s"$messageType: internal inconsistency, $stateTransferType state transfer received wrong new topology " +
              s"for epoch $newEpochNumber, expected ${currentEpochNumber + 1}"
          )
        }

      case Consensus.NewEpochStored(newEpochInfo, membership, cryptoProvider: CryptoProvider[E]) =>
        // Mainly so that the onboarding state transfer start epoch is not set as the latest completed epoch initially.
        // A new event can be introduced to avoid branching.
        if (newEpochInfo != epochState.epoch.info) {
          logger.debug(
            s"$messageType: setting new epoch ${newEpochInfo.number} during $stateTransferType state transfer"
          )
          setNewEpochState(newEpochInfo, membership, cryptoProvider)
        }
        cleanUpPostponedMessageQueue()
        stateTransferManager.stateTransferNewEpoch(
          newEpochInfo.number,
          membership,
          initialState.topologyInfo.currentCryptoProvider, // used only for signing the request
        )(abort)

      case Consensus.Admin.GetOrderingTopology(callback) =>
        callback(
          epochState.epoch.info.number,
          activeTopologyInfo.currentMembership.orderingTopology.nodes,
        )

      case Consensus.ConsensusMessage.AsyncException(e) =>
        logger.error(s"$messageType: exception raised from async consensus message: ${e.toString}")

      // We drop retransmission messages, as they will likely be stale once state transfer is finished.
      case _: Consensus.RetransmissionsMessage =>

      case Consensus.ConsensusMessage
            .PbftUnverifiedNetworkMessage(actualSender, _) =>
        // Use the actual sender to prevent the node from filling up other nodes' quotas in the queue.
        enqueuePbftNetworkMessage(message, actualSender)

      case Consensus.ConsensusMessage.PbftVerifiedNetworkMessage(underlyingMessage) =>
        // likely a late response from the crypto provider
        enqueuePbftNetworkMessage(message, underlyingMessage.from)

      case _ =>
        postponedConsensusMessages.enqueue(BftNodeId.Empty, message) match {
          case EnqueueResult.PerNodeQuotaExceeded(_) =>
            logger.info(
              s"Postponed messages without an originating node exceeded their quota on `$messageType` " +
                s"(likely a late internal message)"
            )
          case EnqueueResult.TotalCapacityExceeded =>
            logger.info(
              s"Postponed message queue total capacity has been exceeded by a message without an originating node " +
                s"(`$messageType`, likely a late internal message)"
            )
          case EnqueueResult.Success =>
            logger.trace(
              s"Successfully postponed a message without an originating node (`$messageType`, " +
                s"likely a late internal message)"
            )
          case FairBoundedQueue.EnqueueResult.Duplicate(_) =>
            abort("Deduplication is disabled")
        }
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

    handleStateTransferMessageResult(result, messageType)
  }

  @VisibleForTesting
  private[bftordering] def handleStateTransferMessageResult(
      result: StateTransferMessageResult,
      messageType: => String,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    result match {
      case StateTransferMessageResult.Continue =>

      case StateTransferMessageResult.NothingToStateTransfer(from) =>
        val currentEpochNumber = epochState.epoch.info.number
        stateTransferManager.cancelTimeoutForEpoch(currentEpochNumber)
        maybeLastReceivedEpochTopology match {
          // Transition back to consensus only if we transferred at least up to the minimum end epoch (if it's defined).
          case Some(newEpochTopologyMessage)
              if initialState.minimumStateTransferEndEpoch.forall(_ <= currentEpochNumber) =>
            logger.info(
              s"$messageType: nothing to state transfer for epoch $currentEpochNumber from '$from', completing state transfer"
            )
            transitionBackToConsensus(newEpochTopologyMessage)
          case _ =>
            logger.info(
              s"$messageType: nothing to state transfer from '$from', while there should be at least one epoch to transfer; " +
                s"likely reached out to a lagging-behind or malicious node, state-transferring epoch $currentEpochNumber again"
            )
            stateTransferManager.stateTransferNewEpoch(
              currentEpochNumber,
              activeTopologyInfo.currentMembership,
              initialState.topologyInfo.currentCryptoProvider, // used only for signing the request
            )(abort)
        }
    }

  private def updateAvailabilityTopology(newEpochTopology: Consensus.NewEpochTopology[E]): Unit =
    dependencies.availability.asyncSend(
      Availability.Consensus.UpdateTopologyDuringStateTransfer(
        newEpochTopology.membership.orderingTopology,
        // TODO(#25220) If the onboarding/starting epoch (`e_start`) is always immediately before the one where
        //  the node is active in the topology, the below distinction could go away.
        DelegationCryptoProvider(
          signer = initialState.topologyInfo.currentCryptoProvider,
          verifier = newEpochTopology.cryptoProvider,
        ),
      )
    )

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

  private def enqueuePbftNetworkMessage(
      message: Consensus.Message[E],
      from: BftNodeId,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit =
    postponedConsensusMessages.enqueue(from, message) match {
      case EnqueueResult.PerNodeQuotaExceeded(nodeId) =>
        logger.info(s"Node `$nodeId` exceeded its postponed message queue quota")
      case EnqueueResult.TotalCapacityExceeded =>
        logger.info("Postponed message queue total capacity has been exceeded")
      case EnqueueResult.Success =>
        logger.trace("Successfully postponed a message")
      case FairBoundedQueue.EnqueueResult.Duplicate(_) =>
        abort("Deduplication is disabled")
    }

  private def cleanUpPostponedMessageQueue(): Unit = {
    val currentEpochNumber = epochState.epoch.info.number

    postponedConsensusMessages.dequeueAll {
      case Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage(_, underlyingMessage) =>
        underlyingMessage.message.blockMetadata.epochNumber < currentEpochNumber
      case Consensus.ConsensusMessage.PbftVerifiedNetworkMessage(underlyingMessage) =>
        // likely a late response from the crypto provider
        underlyingMessage.message.blockMetadata.epochNumber < currentEpochNumber
      case _ => false
    }.discard
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
        previousTopologyInfo,
        previousEpochState,
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
        metrics,
        clock,
        loggerFactory,
      ),
      random,
      dependencies,
      loggerFactory,
      timeouts,
      futurePbftMessageQueue = initialState.pbftMessageQueue,
      postponedConsensusMessageQueue = Some(postponedConsensusMessages),
    )()(catchupDetector)

    context.become(consensusBehavior)

    // Send the new epoch topology to consensus to kick it off after state transfer
    consensusBehavior.receive(Consensus.StateTransferCompleted(newEpochTopologyMessage))
  }

  private def setNewEpochState(
      newEpochInfo: EpochInfo,
      newMembership: Membership,
      newCryptoProvider: CryptoProvider[E],
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    previousTopologyInfo = activeTopologyInfo
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

    previousEpochState = epochState
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
      minimumStateTransferEndEpoch: Option[EpochNumber],
      topologyInfo: OrderingTopologyInfo[E],
      epochState: EpochState[E],
      latestCompletedEpoch: EpochStore.Epoch,
      pbftMessageQueue: FairBoundedQueue[Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage],
  )

  @VisibleForTesting
  private[bftordering] def unapply(
      behavior: StateTransferBehavior[?]
  ): Option[
    (
        EpochLength,
        EpochNumber,
        Option[EpochNumber],
        OrderingTopologyInfo[?],
        EpochInfo,
        EpochStore.Epoch,
    )
  ] =
    Some(
      (
        behavior.epochLength,
        behavior.initialState.stateTransferStartEpoch,
        behavior.initialState.minimumStateTransferEndEpoch,
        behavior.activeTopologyInfo,
        behavior.epochState.epoch.info,
        behavior.latestCompletedEpoch,
      )
    )
}
