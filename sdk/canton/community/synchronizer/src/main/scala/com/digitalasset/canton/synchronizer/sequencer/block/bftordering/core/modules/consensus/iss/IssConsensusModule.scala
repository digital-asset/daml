// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.BootstrapDetector.BootstrapKind
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.InitialState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.{
  emitConsensusLatencyStats,
  emitNonCompliance,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.{
  GenesisEpoch,
  GenesisEpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions.RetransmissionsManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferBehavior.StateTransferType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.IssConsensusSignatureVerifier
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  HasDelayedInit,
  shortType,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.ConsensusMessage.PbftVerifiedNetworkMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.NewEpochTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage.headerFromProto
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftSignedNetworkMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.EpochStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
  Output,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.FairBoundedQueue
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.time.Instant
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Random, Success}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class IssConsensusModule[E <: Env[E]](
    private val epochLength: EpochLength, // TODO(#19289): support variable epoch lengths
    private val initialState: InitialState[E],
    epochStore: EpochStore[E],
    clock: Clock,
    metrics: BftOrderingMetrics,
    segmentModuleRefFactory: SegmentModuleRefFactory[E],
    retransmissionsManager: RetransmissionsManager[E],
    random: Random,
    override val dependencies: ConsensusModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    private val futurePbftMessageQueue: FairBoundedQueue[
      Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage
    ],
    private val postponedConsensusMessageQueue: Option[FairBoundedQueue[Consensus.Message[E]]] =
      None,
)(
    // Only tests pass the state manager as parameter, and it's convenient to have it as an option
    //  to avoid two different constructor calls depending on whether the test want to customize it or not.
    customOnboardingAndServerStateTransferManager: Option[StateTransferManager[E]] = None,
    private var activeTopologyInfo: OrderingTopologyInfo[E] = initialState.topologyInfo,
)(
    private var catchupDetector: CatchupDetector = new DefaultCatchupDetector(
      activeTopologyInfo.currentMembership,
      loggerFactory,
    ),
    // Only passed in tests
    private var newEpochTopology: Option[Consensus.NewEpochTopology[E]] = None,
)(implicit
    synchronizerProtocolVersion: ProtocolVersion,
    override val config: BftBlockOrdererConfig,
    mc: MetricsContext,
) extends Consensus[E]
    with HasDelayedInit[Consensus.Message[E]] {

  private val thisNode = initialState.topologyInfo.thisNode

  // An instance of state transfer manager to be used only in a server role.
  private val serverStateTransferManager =
    customOnboardingAndServerStateTransferManager.getOrElse(
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

  private val signatureVerifier = new IssConsensusSignatureVerifier[E](metrics)

  logger.debug(
    "Starting with " +
      s"membership = ${initialState.topologyInfo.currentMembership}, " +
      s"latest completed epoch = ${initialState.latestCompletedEpoch.info}, " +
      s"current epoch = ${initialState.epochState.epoch.info} (completed: ${initialState.epochState.epochCompletionStatus.isComplete})"
  )(TraceContext.empty)

  private var latestCompletedEpoch: EpochStore.Epoch = initialState.latestCompletedEpoch
  @VisibleForTesting
  private[iss] def getLatestCompletedEpoch: EpochStore.Epoch = latestCompletedEpoch

  private var epochState = initialState.epochState
  @VisibleForTesting
  private[iss] def getEpochState: EpochState[E] = epochState

  private var consensusWaitingForEpochCompletionSince: Option[Instant] = None
  private var consensusWaitingForEpochStartSince: Option[Instant] = None

  @VisibleForTesting
  private[iss] def getActiveTopologyInfo: OrderingTopologyInfo[E] = activeTopologyInfo

  // TODO(#16761) resend locally-led ordered blocks (PrePrepare) in activeEpoch in case my node crashed
  override def ready(self: ModuleRef[Consensus.Message[E]]): Unit = ()

  override protected def receiveInternal(message: Consensus.Message[E])(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {

      case Consensus.Init =>
        abortInit(
          s"${PreIssConsensusModule.getClass.getSimpleName} should be the only one receiving ${Consensus.Init.getClass.getSimpleName}"
        )

      case Consensus.SegmentCancelledEpoch =>
        abortInit(
          s"${StateTransferBehavior.getClass.getSimpleName} should be the only one receiving ${Consensus.SegmentCancelledEpoch.getClass.getSimpleName}"
        )

      case Consensus.Start =>
        val maybeSnapshotAdditionalInfo = initialState.sequencerSnapshotAdditionalInfo
        BootstrapDetector.detect(
          epochLength,
          maybeSnapshotAdditionalInfo,
          activeTopologyInfo.currentMembership,
          latestCompletedEpoch,
        )(abort) match {
          case BootstrapKind.Onboarding(startEpochInfo) =>
            logger.info(
              s"Detected new node onboarding from epoch ${startEpochInfo.number} for sequencer snapshot additional " +
                s"info = $maybeSnapshotAdditionalInfo"
            )
            // Other parameters are irrelevant for state transfer start.
            latestCompletedEpoch = latestCompletedEpoch.copy(info =
              latestCompletedEpoch.info.copy(number = EpochNumber(startEpochInfo.number - 1))
            )
            // TODO(#22894) consider a separate epoch state class for state transfer
            setNewEpochState(startEpochInfo, maybeNewMembershipAndCryptoProvider = None)
            startStateTransfer(
              startEpochInfo.number,
              StateTransferType.Onboarding,
              // We only know the minimum end epoch when receiving it from the catchup detector.
              minimumEndEpochNumber = None,
            )

          case BootstrapKind.RegularStartup =>
            logger.debug(
              s"(Re)starting node from epoch ${initialState.epochState.epoch.info.number}"
            )
            startConsensusForCurrentEpoch()
            initCompleted(context.self.asyncSend)
        }

      case stateTransferCompletion: Consensus.StateTransferCompleted[E] =>
        val newEpochTopologyMessage = stateTransferCompletion.newEpochTopologyMessage
        logger.info(
          s"Completed state transfer, new epoch is ${newEpochTopologyMessage.epochNumber}, completing init"
        )
        val currentEpochInfo = epochState.epoch.info
        val newEpochInfo = currentEpochInfo.next(
          epochLength,
          newEpochTopologyMessage.membership.orderingTopology.activationTime,
        )
        // Set the new epoch state before processing queued messages (including a proper segment module factory)
        //  in case catch-up needs to be triggered again due to being behind enough.
        setNewEpochState(
          newEpochInfo,
          Some(newEpochTopologyMessage.membership -> newEpochTopologyMessage.cryptoProvider),
        )
        // Complete init early to avoid re-queueing messages.
        initCompleted(context.self.asyncSend)
        processNewEpochTopology(newEpochTopologyMessage, currentEpochInfo, newEpochInfo)
        // Try to process messages that potentially triggered a catch-up (should do nothing for onboarding).
        processQueuedPbftMessages()
        // Then, go through messages that got postponed during state transfer.
        postponedConsensusMessageQueue.foreach(
          _.dequeueAll(_ => true).foreach(context.self.asyncSend)
        )

      case message: Consensus.Admin => handleAdminMessage(message)

      case message: Consensus.ProtocolMessage => handleProtocolMessage(message)

      case newEpochTopologyMessage: Consensus.NewEpochTopology[E] =>
        val currentEpochInfo = epochState.epoch.info
        val newTopologyActivationTime =
          newEpochTopologyMessage.membership.orderingTopology.activationTime
        val newEpochInfo = currentEpochInfo.next(epochLength, newTopologyActivationTime)
        processNewEpochTopology(newEpochTopologyMessage, currentEpochInfo, newEpochInfo)

      case newEpochStored @ Consensus.NewEpochStored(
            newEpochInfo,
            newMembership,
            newCryptoProvider: CryptoProvider[E],
          ) =>
        // Despite being generated internally by Consensus, we delay this event (a) for uniformity with other
        // Output module events, and (b) to prevent tests from bypassing the delayed queue when sending this event
        ifInitCompleted(newEpochStored) { _ =>
          logger.debug(s"Stored new epoch ${newEpochInfo.number}")

          // Reset any topology remembered while waiting for the previous (completed) epoch to be stored.
          newEpochTopology = None

          setNewEpochState(newEpochInfo, Some(newMembership -> newCryptoProvider))

          startConsensusForCurrentEpoch()
          logger.debug(
            s"New epoch: ${epochState.epoch.info.number} has started with ordering topology ${newMembership.orderingTopology}"
          )

          processQueuedPbftMessages()
        }
    }

  private def processNewEpochTopology(
      newEpochTopologyMessage: NewEpochTopology[E],
      currentEpochInfo: EpochInfo,
      newEpochInfo: EpochInfo,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val newEpochNumber = newEpochTopologyMessage.epochNumber
    val newMembership = newEpochTopologyMessage.membership
    val newCryptoProvider = newEpochTopologyMessage.cryptoProvider
    // Don't process a NewEpochTopology from the Output module, that may start earlier, until init completes
    ifInitCompleted(newEpochTopologyMessage) { _ =>
      val latestCompletedEpochNumber = latestCompletedEpoch.info.number
      val currentEpochNumber = currentEpochInfo.number

      if (latestCompletedEpochNumber == newEpochNumber - 1) {
        if (currentEpochNumber == newEpochNumber) {
          // The output module may re-send the topology for the current epoch upon restart if it didn't store
          //  the first block metadata or if the subscribing sequencer runtime hasn't processed it yet.
          logger.debug(
            s"Received NewEpochTopology event for epoch $newEpochNumber, but the epoch has already started; ignoring it"
          )
        } else if (currentEpochNumber == newEpochNumber - 1) {
          emitEpochStartLatency()
          startNewEpochUnlessOffboarded(
            currentEpochInfo,
            newEpochInfo,
            newMembership,
            newCryptoProvider,
          )
        } else {
          abort(
            s"Received NewEpochTopology event for epoch $newEpochNumber, " +
              s"but the current epoch number $currentEpochNumber is neither $newEpochNumber nor ${newEpochNumber - 1}"
          )
        }
      } else if (latestCompletedEpochNumber < newEpochNumber - 1) {
        logger.debug(
          s"Epoch (${newEpochNumber - 1}) has not yet been completed: remembering the topology and " +
            s"waiting for the completed epoch to be stored; latest completed epoch is $latestCompletedEpochNumber"
        )
        // Upon epoch completion, a new epoch with this topology will be started.
        newEpochTopology = Some(newEpochTopologyMessage)
      } else { // latestCompletedEpochNumber >= epochNumber
        // The output module re-sent a topology for an already completed epoch; this can happen upon restart if
        //  either the output module, or the subscribing sequencer runtime, or both are more than one epoch behind
        //  consensus, because the output module will just reprocess the blocks to be recovered.
        logger.info(
          s"Received NewEpochTopology for epoch $newEpochNumber, but the latest completed epoch is already $latestCompletedEpochNumber; ignoring"
        )
      }
    }
  }

  private def processQueuedPbftMessages()(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    // Process messages for this epoch that may have arrived when processing the previous one.
    //  PBFT messages for a future epoch may become stale after a catch-up, so we need to extract and discard them.
    val queuedPbftMessages =
      futurePbftMessageQueue.dequeueAll(
        _.underlyingNetworkMessage.message.blockMetadata.epochNumber <= epochState.epoch.info.number
      )

    queuedPbftMessages.foreach { msg =>
      val msgEpochNumber = msg.underlyingNetworkMessage.message.blockMetadata.epochNumber
      if (msgEpochNumber == epochState.epoch.info.number)
        processUnverifiedPbftMessageAtCurrentEpoch(msg)
    }
  }

  private def handleAdminMessage(message: Consensus.Admin): Unit =
    message match {

      case Consensus.Admin.GetOrderingTopology(callback) =>
        callback(
          epochState.epoch.info.number,
          activeTopologyInfo.currentMembership.orderingTopology.nodes,
        )

      case Consensus.Admin.SetPerformanceMetricsEnabled(enabled) =>
        metrics.performance.enabled = enabled
    }

  private def handleProtocolMessage(
      message: Consensus.ProtocolMessage
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    ifInitCompleted(message) {
      case localAvailabilityMessage: Consensus.LocalAvailability =>
        handleLocalAvailabilityMessage(localAvailabilityMessage)

      case consensusMessage: Consensus.ConsensusMessage =>
        handleConsensusMessage(consensusMessage)

      case Consensus.RetransmissionsMessage.VerifiedNetworkMessage(
            Consensus.RetransmissionsMessage.RetransmissionRequest(
              SignedMessage(EpochStatus(from, epochNumber, _), _)
            )
          )
          if startCatchupIfNeeded(
            catchupDetector.updateLatestKnownNodeEpoch(from, epochNumber),
            epochNumber,
          ) =>
        logger.debug(
          s"Ignoring retransmission request from $from as we are entering catch-up mode"
        )

      case msg: Consensus.RetransmissionsMessage =>
        retransmissionsManager.handleMessage(activeTopologyInfo, msg)

      case msg: Consensus.StateTransferMessage =>
        serverStateTransferManager.handleStateTransferMessage(
          msg,
          activeTopologyInfo,
          latestCompletedEpoch,
        )(abort) match {
          case StateTransferMessageResult.Continue =>
          case other => abort(s"Unexpected result $other from server-side state transfer manager")
        }
    }

  private def handleLocalAvailabilityMessage(
      localAvailabilityMessage: Consensus.LocalAvailability
  )(implicit traceContext: TraceContext): Unit =
    epochState.localAvailabilityMessageReceived(localAvailabilityMessage)

  private def handleConsensusMessage(
      consensusMessage: Consensus.ConsensusMessage
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(consensusMessage)

    consensusMessage match {
      case Consensus.ConsensusMessage.PbftVerifiedNetworkMessage(pbftEvent) =>
        processVerifiedPbftMessage(pbftEvent)

      case msg: Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage =>
        val from = msg.underlyingNetworkMessage.from
        val blockMetadata = msg.underlyingNetworkMessage.message.blockMetadata
        val epochNumber = blockMetadata.epochNumber
        val blockNumber = blockMetadata.blockNumber

        val thisNodeEpochNumber = epochState.epoch.info.number

        val updatedEpoch = catchupDetector.updateLatestKnownNodeEpoch(from, epochNumber)
        lazy val pbftMessageType = shortType(msg.underlyingNetworkMessage.message)

        if (epochNumber < thisNodeEpochNumber) {
          logger.info(
            s"Discarded PBFT message $pbftMessageType about block $blockNumber " +
              s"at epoch $epochNumber because we're at a later epoch ($thisNodeEpochNumber)"
          )
        } else if (epochNumber > thisNodeEpochNumber) {
          // Messages from future epoch are queued to be processed when we move to that epoch.
          // Note that we use the actual sender instead of the original sender, so that a node
          // cannot maliciously retransmit many messages from another node with the intent to
          // fill up the other node's quota in this queue.
          futurePbftMessageQueue.enqueue(msg.actualSender, msg) match {
            case FairBoundedQueue.EnqueueResult.Success =>
              logger.debug(
                s"Queued PBFT message $pbftMessageType from future epoch $epochNumber " +
                  s"as we're still in epoch $thisNodeEpochNumber"
              )
            case FairBoundedQueue.EnqueueResult.TotalCapacityExceeded =>
              logger.info(
                s"Dropped PBFT message $pbftMessageType from future epoch $epochNumber " +
                  s"as we're still in epoch $thisNodeEpochNumber and " +
                  s"total capacity for queueing future messages has been reached"
              )
            case FairBoundedQueue.EnqueueResult.PerNodeQuotaExceeded(node) =>
              logger.info(
                s"Dropped PBFT message $pbftMessageType from future epoch $epochNumber " +
                  s"as we're still in epoch $thisNodeEpochNumber and " +
                  s"the quota for node $node for queueing future messages has been reached"
              )
            case FairBoundedQueue.EnqueueResult.Duplicate(_) =>
              abort("Deduplication is disabled")
          }
          startCatchupIfNeeded(updatedEpoch, epochNumber).discard
        } else
          processUnverifiedPbftMessageAtCurrentEpoch(msg)

      case Consensus.ConsensusMessage.BlockOrdered(
            orderedBlock: OrderedBlock,
            commitCertificate: CommitCertificate,
            hasCompletedLedSegment,
          ) =>
        emitConsensusLatencyStats(metrics)

        if (hasCompletedLedSegment)
          consensusWaitingForEpochCompletionSince = Some(Instant.now())

        epochState.confirmBlockCompleted(orderedBlock.metadata, commitCertificate)

        epochState.epochCompletionStatus match {
          case EpochState.Complete(commitCertificates) =>
            emitEpochCompletionWaitLatency()
            consensusWaitingForEpochStartSince = Some(Instant.now())
            storeEpochCompletion(commitCertificates).discard
          case _ => ()
        }

        // TODO(#16761) - ensure the output module gets and processes the ordered block
        val epochNumber = epochState.epoch.info.number
        val blockNumber = orderedBlock.metadata.blockNumber
        val blockSegment = epochState.epoch.segments
          .find(_.slotNumbers.contains(blockNumber))
          .getOrElse(abort(s"block $blockNumber not part of any segment in epoch $epochNumber"))
        dependencies.output.asyncSend(
          Output.BlockOrdered(
            OrderedBlockForOutput(
              orderedBlock,
              commitCertificate.prePrepare.message.viewNumber,
              blockSegment.originalLeader,
              blockNumber == epochState.epoch.info.lastBlockNumber,
              OrderedBlockForOutput.Mode.FromConsensus,
            )
          )
        )

      case Consensus.ConsensusMessage.CompleteEpochStored(epoch, commitCertificates) =>
        advanceEpoch(epoch, commitCertificates, Some(messageType))

      case Consensus.ConsensusMessage.SegmentCompletedEpoch(segmentFirstBlockNumber, epochNumber) =>
        logger.debug(s"Segment module $segmentFirstBlockNumber completed epoch $epochNumber")

      case Consensus.ConsensusMessage.AsyncException(e: Throwable) =>
        logger.error(s"$messageType: exception raised from async consensus message: ${e.toString}")
    }
  }

  private def advanceEpoch(
      completeEpochSnapshot: EpochStore.Epoch,
      completeEpochCommitCertificates: Seq[CommitCertificate],
      actingOnMessageType: Option[String] = None,
      initInProgress: Boolean = false,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val completeEpochNumber = completeEpochSnapshot.info.number
    val prefix = actingOnMessageType.map(mt => s"$mt: ").getOrElse("")
    logger.debug(s"${prefix}stored w/ epoch = $completeEpochNumber")
    val currentEpochStateNumber = epochState.epoch.info.number

    if (completeEpochNumber < currentEpochStateNumber) {
      logger.info(
        s"Epoch $completeEpochNumber already advanced; current epoch = $currentEpochStateNumber; ignoring"
      )
    } else if (completeEpochNumber > currentEpochStateNumber) {
      abort(
        s"Trying to complete future epoch $completeEpochNumber before local epoch $currentEpochStateNumber has caught up!"
      )
    } else {
      // The current epoch can be completed

      if (!initInProgress) {
        // When initializing, the retransmission manager is inactive and segment modules are not started
        retransmissionsManager.epochEnded(completeEpochCommitCertificates)
        epochState.notifyEpochCompletionToSegments(completeEpochNumber)
      }

      epochState.close()

      latestCompletedEpoch = completeEpochSnapshot

      newEpochTopology match {
        case Some(Consensus.NewEpochTopology(newEpochNumber, newMembership, cryptoProvider)) =>
          emitEpochStartLatency()
          val currentEpochInfo = epochState.epoch.info
          val newEpochInfo = currentEpochInfo.next(
            epochLength,
            newMembership.orderingTopology.activationTime,
          )
          if (newEpochNumber != newEpochInfo.number) {
            abort(
              s"Trying to start new epoch ${newEpochInfo.number} with topology for unexpected epoch $newEpochNumber"
            )
          }
          startNewEpochUnlessOffboarded(
            currentEpochInfo,
            newEpochInfo,
            newMembership,
            cryptoProvider,
          )
        case None =>
          // We don't have the new topology for the new epoch yet: wait for it to arrive from the output module.
          ()
      }
    }
  }

  private def startConsensusForCurrentEpoch()(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (epochState.epoch.info == GenesisEpoch.info) {
      logger.debug("Started at genesis, self-sending its topology to start epoch 0")
      context.self.asyncSend(
        NewEpochTopology(
          EpochNumber.First,
          activeTopologyInfo.currentMembership,
          activeTopologyInfo.currentCryptoProvider,
        )
      )
    } else if (!epochState.epochCompletionStatus.isComplete) {
      logger.debug("Started during an in-progress epoch, starting segment modules")
      epochState.startSegmentModules()
      retransmissionsManager.startEpoch(epochState)
    } else {
      // The current epoch is complete: ensure the epoch completion is stored and local state is updated,
      //  which may not be the case when recovering from a crash.
      //  We leverage the idempotency of the following logic in case the epoch completion was already stored.
      epochState.epochCompletionStatus match {
        case EpochState.Complete(completeEpochCommitCertificates) =>
          val completeEpochSnapshot =
            storeEpochCompletion(completeEpochCommitCertificates, sync = true)
          advanceEpoch(
            completeEpochSnapshot,
            completeEpochCommitCertificates,
            initInProgress = true,
          )
        case _ =>
          abort("Epoch is complete but its commit certificates are not available")
      }
      logger.debug(
        "Started after a completed epoch but before starting a new one, waiting for topology from the output module"
      )
    }

  private def startNewEpochUnlessOffboarded(
      currentEpochInfo: EpochInfo,
      newEpochInfo: EpochInfo,
      newMembership: Membership,
      cryptoProvider: CryptoProvider[E],
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val newEpochNumber = newEpochInfo.number
    if (newMembership.orderingTopology.contains(thisNode)) {
      logger.debug(s"Starting new epoch $newEpochNumber from NewEpochTopology event")

      metrics.consensus.votes.cleanupVoteGauges(keepOnly = newMembership.orderingTopology.nodes)
      epochState.emitEpochStats(metrics, currentEpochInfo)

      logger.debug(s"Storing new epoch $newEpochNumber")
      pipeToSelf(epochStore.startEpoch(newEpochInfo)) {
        case Failure(exception) => Consensus.ConsensusMessage.AsyncException(exception)
        case Success(_) =>
          Consensus.NewEpochStored(newEpochInfo, newMembership, cryptoProvider)
      }
    } else {
      logger.info(
        s"Received topology for epoch $newEpochNumber, but this node isn't part of it (i.e., it has been " +
          "off-boarded): not starting consensus as this node is going to be shut down and decommissioned"
      )
    }
  }

  private def setNewEpochState(
      newEpochInfo: EpochInfo,
      maybeNewMembershipAndCryptoProvider: Option[(Membership, CryptoProvider[E])],
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val currentEpochInfo = epochState.epoch.info
    if (currentEpochInfo == newEpochInfo) {
      // It can happen after storing the first epoch after state transfer.
      logger.debug(s"New epoch state for epoch ${newEpochInfo.number} already set")
    } else if (
      newEpochInfo.number == currentEpochInfo.number + 1 || currentEpochInfo == GenesisEpochInfo
    ) {
      maybeNewMembershipAndCryptoProvider.foreach { case (newMembership, newCryptoProvider) =>
        activeTopologyInfo = activeTopologyInfo.updateMembership(newMembership, newCryptoProvider)
      }

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
        segmentModuleRefFactory = segmentModuleRefFactory(
          context,
          newEpoch,
          activeTopologyInfo.currentCryptoProvider,
          latestCompletedEpoch.lastBlockCommits,
          epochInProgress = EpochStore.EpochInProgress(
            completedBlocks = Seq.empty,
            pbftMessagesForIncompleteBlocks = Seq.empty,
          ),
        ),
        completedBlocks = Seq.empty,
        loggerFactory = loggerFactory,
        timeouts = timeouts,
      )
    } else {
      abort(
        s"Setting epoch state for unexpected epoch ${newEpochInfo.number}, current epoch is ${currentEpochInfo.number}"
      )
    }
  }

  private def processUnverifiedPbftMessageAtCurrentEpoch(
      message: Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val underlyingNetworkMessage = message.underlyingNetworkMessage
    val from = underlyingNetworkMessage.from
    val blockMetadata = underlyingNetworkMessage.message.blockMetadata
    val epochNumber = blockMetadata.epochNumber
    val blockNumber = blockMetadata.blockNumber
    lazy val messageType = shortType(message)

    def emitNonComplianceMetric(): Unit =
      emitNonCompliance(metrics)(
        from,
        metrics.security.noncompliant.labels.violationType.values.ConsensusInvalidMessage,
      )

    if (
      blockNumber < epochState.epoch.info.startBlockNumber || blockNumber > epochState.epoch.info.lastBlockNumber
    ) {
      // Messages with block numbers out of bounds of the epoch are discarded.
      val epochInfo = epochState.epoch.info
      logger.warn(
        s"Discarded PBFT message $messageType about block $blockNumber" +
          s"from epoch $epochNumber (current epoch number = ${epochInfo.number}, " +
          s"first block = ${epochInfo.startBlockNumber}, epoch length = ${epochInfo.length}) " +
          "because the block number is out of bounds of the current epoch"
      )
      emitNonComplianceMetric()
    } else
      context.pipeToSelf(
        signatureVerifier.verify(underlyingNetworkMessage, activeTopologyInfo)
      ) {
        case Failure(error) =>
          logger.warn(
            s"Message $underlyingNetworkMessage from '$from' could not be validated, dropping",
            error,
          )
          emitNonComplianceMetric()
          None
        case Success(Left(errors)) =>
          // Info because it can also happen at epoch boundaries
          logger.info(
            s"Message $underlyingNetworkMessage from '$from' failed validation, dropping: $errors"
          )
          emitNonComplianceMetric()
          None

        case Success(Right(())) =>
          logger.debug(
            s"Message ${shortType(underlyingNetworkMessage.message)} from $from is valid"
          )
          Some(PbftVerifiedNetworkMessage(underlyingNetworkMessage))
      }
  }

  private def processVerifiedPbftMessage(
      pbftMessage: SignedMessage[ConsensusSegment.ConsensusMessage.PbftNetworkMessage]
  )(implicit traceContext: TraceContext): Unit = {
    val pbftMessagePayload = pbftMessage.message
    val pbftMessageBlockMetadata = pbftMessagePayload.blockMetadata
    val epochNumber = pbftMessageBlockMetadata.epochNumber
    val thisNodeEpochNumber = epochState.epoch.info.number
    val blockNumber = pbftMessageBlockMetadata.blockNumber

    lazy val messageType = shortType(pbftMessagePayload)
    logger.debug(
      s"$messageType: received verified message from ${pbftMessage.from} w/ metadata $pbftMessageBlockMetadata"
    )

    // at the time this message started being verified, we were at the same epoch as the message,
    // but there is a chance we moved to the next epoch since then, so important to re-check
    if (epochNumber < thisNodeEpochNumber) {
      logger.info(
        s"Discarded verified PBFT message $messageType about block $blockNumber " +
          s"at epoch $epochNumber because we've moved to later epoch ($thisNodeEpochNumber) during signature verification"
      )
    } else
      epochState.processPbftMessage(PbftSignedNetworkMessage(pbftMessage))
  }

  private def startCatchupIfNeeded(
      updatedEpoch: Boolean,
      pbftMessageEpochNumber: EpochNumber,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Boolean = {
    val currentEpochNumber = epochState.epoch.info.number
    val latestCompletedEpochNumber = latestCompletedEpoch.info.number
    val minimumEndEpochNumber = catchupDetector.shouldCatchUpTo(currentEpochNumber)
    if (updatedEpoch && minimumEndEpochNumber.isDefined) {
      logger.debug(
        s"Switching to catch-up state transfer (up to at least $minimumEndEpochNumber) while in epoch $currentEpochNumber; " +
          s"latestCompletedEpoch is $latestCompletedEpochNumber and message epoch is $pbftMessageEpochNumber"
      )
      startStateTransfer(currentEpochNumber, StateTransferType.Catchup, minimumEndEpochNumber)
      true
    } else {
      false
    }
  }

  private def startStateTransfer(
      startEpochNumber: EpochNumber,
      stateTransferType: StateTransferType,
      minimumEndEpochNumber: Option[EpochNumber],
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    logger.info(s"Starting $stateTransferType state transfer from epoch $startEpochNumber")
    resetConsensusWaitingForEpochCompletion()
    val newBehavior = new StateTransferBehavior(
      epochLength,
      StateTransferBehavior.InitialState[E](
        startEpochNumber,
        minimumEndEpochNumber,
        activeTopologyInfo,
        epochState,
        latestCompletedEpoch,
        futurePbftMessageQueue,
      ),
      stateTransferType,
      catchupDetector,
      epochStore,
      clock,
      metrics,
      segmentModuleRefFactory,
      random,
      dependencies,
      loggerFactory,
      timeouts,
    )()
    context.become(newBehavior)
  }

  private def storeEpochCompletion(
      completeEpochCommitCertificates: Seq[CommitCertificate],
      sync: Boolean = false,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): EpochStore.Epoch = {
    val epochInfo = epochState.epoch.info
    logger.debug(
      s"Storing completed epoch: ${epochInfo.number}, start block number = ${epochInfo.startBlockNumber}, length = ${epochInfo.length}"
    )
    val epochSnapshot = EpochStore.Epoch(epochInfo, epochState.lastBlockCommitMessages)

    if (sync) {
      context.blockingAwait(epochStore.completeEpoch(epochInfo.number))
    } else {
      pipeToSelf(epochStore.completeEpoch(epochInfo.number)) {
        case Failure(exception) => Consensus.ConsensusMessage.AsyncException(exception)
        case Success(_) =>
          Consensus.ConsensusMessage.CompleteEpochStored(
            epochSnapshot,
            completeEpochCommitCertificates,
          )
      }
    }
    epochSnapshot
  }

  private def emitEpochCompletionWaitLatency(): Unit = {
    import metrics.performance.orderingStageLatency.*
    emitOrderingStageLatency(
      labels.stage.values.consensus.EpochCompletionWait,
      consensusWaitingForEpochCompletionSince,
      cleanup = () => resetConsensusWaitingForEpochCompletion(),
    )
  }

  private def resetConsensusWaitingForEpochCompletion(): Unit =
    consensusWaitingForEpochCompletionSince = None

  private def emitEpochStartLatency(): Unit = {
    import metrics.performance.orderingStageLatency.*
    emitOrderingStageLatency(
      labels.stage.values.consensus.EpochStartWait,
      consensusWaitingForEpochStartSince,
      cleanup = () => resetConsensusWaitingForEpochStart(),
    )
  }

  private def resetConsensusWaitingForEpochStart(): Unit =
    consensusWaitingForEpochStartSince = None
}

object IssConsensusModule {

  final case class InitialState[E <: Env[E]](
      topologyInfo: OrderingTopologyInfo[E],
      epochState: EpochState[E],
      latestCompletedEpoch: EpochStore.Epoch,
      sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
  )

  val DefaultDatabaseReadTimeout: FiniteDuration = 10.seconds

  def parseNetworkMessage(
      protoSignedMessage: v30.SignedMessage
  ): ParsingResult[SignedMessage[ConsensusSegment.ConsensusMessage.PbftNetworkMessage]] =
    SignedMessage
      .fromProtoWithNodeId(v30.ConsensusMessage)(from =>
        proto => originalByteString => parseConsensusNetworkMessage(from, proto)(originalByteString)
      )(protoSignedMessage)

  def parseConsensusNetworkMessage(
      from: BftNodeId,
      message: v30.ConsensusMessage,
  )(
      originalByteString: ByteString
  ): ParsingResult[ConsensusSegment.ConsensusMessage.PbftNetworkMessage] =
    for {
      header <- headerFromProto(message)
      result <- (message.message match {
        case v30.ConsensusMessage.Message.PrePrepare(value) =>
          ConsensusSegment.ConsensusMessage.PrePrepare.fromProto(
            header.blockMetadata,
            header.viewNumber,
            value,
            from,
          )(originalByteString)
        case v30.ConsensusMessage.Message.Prepare(value) =>
          ConsensusSegment.ConsensusMessage.Prepare.fromProto(
            header.blockMetadata,
            header.viewNumber,
            value,
            from,
          )(originalByteString)
        case v30.ConsensusMessage.Message.Commit(value) =>
          ConsensusSegment.ConsensusMessage.Commit.fromProto(
            header.blockMetadata,
            header.viewNumber,
            value,
            from,
          )(originalByteString)
        case v30.ConsensusMessage.Message.ViewChange(value) =>
          ConsensusSegment.ConsensusMessage.ViewChange.fromProto(
            header.blockMetadata,
            header.viewNumber,
            value,
            from,
          )(originalByteString)
        case v30.ConsensusMessage.Message.NewView(value) =>
          ConsensusSegment.ConsensusMessage.NewView.fromProto(
            header.blockMetadata,
            header.viewNumber,
            value,
            from,
          )(originalByteString)
        case v30.ConsensusMessage.Message.Empty =>
          Left(ProtoDeserializationError.OtherError("Empty Received"))
      }): ParsingResult[ConsensusSegment.ConsensusMessage.PbftNetworkMessage]
    } yield result

  def parseRetransmissionMessage(
      from: BftNodeId,
      message: v30.RetransmissionMessage,
  ): ParsingResult[Consensus.RetransmissionsMessage] =
    (message.message match {
      case v30.RetransmissionMessage.Message.RetransmissionRequest(value) =>
        Consensus.RetransmissionsMessage.RetransmissionRequest.fromProto(from, value)
      case v30.RetransmissionMessage.Message.RetransmissionResponse(value) =>
        Consensus.RetransmissionsMessage.RetransmissionResponse.fromProto(from, value)
      case v30.RetransmissionMessage.Message.Empty =>
        Left(ProtoDeserializationError.OtherError("Empty Received"))
    })
      .map(Consensus.RetransmissionsMessage.UnverifiedNetworkMessage.apply)

  def parseStateTransferMessage(
      from: BftNodeId,
      message: v30.StateTransferMessage,
  )(
      originalByteString: ByteString
  ): ParsingResult[Consensus.StateTransferMessage.StateTransferNetworkMessage] =
    message.message match {
      case v30.StateTransferMessage.Message.BlockRequest(value) =>
        Consensus.StateTransferMessage.BlockTransferRequest.fromProto(from, value)(
          originalByteString
        )
      case v30.StateTransferMessage.Message.BlockResponse(value) =>
        Consensus.StateTransferMessage.BlockTransferResponse.fromProto(from, value)(
          originalByteString
        )
      case v30.StateTransferMessage.Message.Empty =>
        Left(ProtoDeserializationError.OtherError("Empty Received"))
    }

  @VisibleForTesting
  private[iss] def unapply(issConsensusModule: IssConsensusModule[?]): Option[
    (
        EpochLength,
        Option[SequencerSnapshotAdditionalInfo],
        OrderingTopologyInfo[?],
        Seq[Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage],
        Option[Seq[Consensus.Message[?]]],
    )
  ] =
    Some(
      (
        issConsensusModule.epochLength,
        issConsensusModule.initialState.sequencerSnapshotAdditionalInfo,
        issConsensusModule.activeTopologyInfo,
        issConsensusModule.futurePbftMessageQueue.dump,
        issConsensusModule.postponedConsensusMessageQueue.map(_.dump),
      )
    )
}
