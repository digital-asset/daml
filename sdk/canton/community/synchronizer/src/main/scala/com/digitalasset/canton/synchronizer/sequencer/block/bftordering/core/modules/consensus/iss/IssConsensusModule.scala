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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.{
  BftBlockOrdererConfig,
  FingerprintKeyId,
}
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.{
  LeaderSelectionPolicy,
  SimpleLeaderSelectionPolicy,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  HasDelayedInit,
  shortType,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
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
  MessageAuthorizer,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.ConsensusMessage.PbftVerifiedNetworkMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.NewEpochTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage.headerFromProto
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  PbftNetworkMessage,
  PbftSignedNetworkMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.EpochStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
  Output,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import scala.collection.mutable
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
    // TODO(#23484): we cannot queue all messages (e.g., during state transfer) due to a potential OOM error
    private val futurePbftMessageQueue: mutable.Queue[SignedMessage[PbftNetworkMessage]] =
      new mutable.Queue(),
    private val queuedConsensusMessages: Seq[Consensus.Message[E]] = Seq.empty,
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
    // Only passed in tests
    private var messageAuthorizer: MessageAuthorizer = activeTopologyInfo.currentTopology,
)(implicit mc: MetricsContext, config: BftBlockOrdererConfig)
    extends Consensus[E]
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
        loggerFactory,
      )()
    )

  private val signatureVerifier = new IssConsensusSignatureVerifier[E]

  logger.debug(
    "Starting with " +
      s"membership = ${initialState.topologyInfo.currentMembership}, " +
      s"latest completed epoch = ${initialState.latestCompletedEpoch.info}, " +
      s"current epoch = ${initialState.epochState.epoch.info} (completed: ${initialState.epochState.epochCompletionStatus.isComplete})"
  )(TraceContext.empty)

  private var latestCompletedEpoch: EpochStore.Epoch = initialState.latestCompletedEpoch
  @VisibleForTesting
  private[bftordering] def getLatestCompletedEpoch: EpochStore.Epoch = latestCompletedEpoch

  private var epochState = initialState.epochState
  @VisibleForTesting
  private[bftordering] def getEpochState: EpochState[E] = epochState

  override def ready(self: ModuleRef[Consensus.Message[E]]): Unit =
    // TODO(#16761) also resend locally-led ordered blocks (PrePrepare) in activeEpoch in case my node crashed
    queuedConsensusMessages.foreach(self.asyncSend)

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
            setNewEpochState(
              startEpochInfo,
              activeTopologyInfo.currentMembership,
              activeTopologyInfo.currentCryptoProvider,
            )
            startStateTransfer(startEpochInfo.number, StateTransferType.Onboarding)

          case BootstrapKind.RegularStartup =>
            logger.debug(
              s"(Re)starting node from epoch ${initialState.epochState.epoch.info.number}"
            )
            startConsensusForCurrentEpoch()
            initCompleted(receiveInternal(_))
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
          newEpochTopologyMessage.membership,
          newEpochTopologyMessage.cryptoProvider,
        )
        initCompleted(receiveInternal(_))
        processNewEpochTopology(newEpochTopologyMessage, currentEpochInfo, newEpochInfo)

      case Consensus.Admin.GetOrderingTopology(callback) =>
        callback(
          epochState.epoch.info.number,
          activeTopologyInfo.currentMembership.orderingTopology.nodes,
        )

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

          setNewEpochState(newEpochInfo, newMembership, newCryptoProvider)

          startConsensusForCurrentEpoch()
          logger.debug(
            s"New epoch: ${epochState.epoch.info.number} has started with ordering topology ${newMembership.orderingTopology}"
          )

          // Process messages for this epoch that may have arrived when processing the previous one.
          //  PBFT messages for a future epoch may become stale after a catch-up, so we need to extract and discard them.
          val queuedPbftMessages =
            futurePbftMessageQueue.dequeueAll(
              _.message.blockMetadata.epochNumber <= epochState.epoch.info.number
            )

          queuedPbftMessages.foreach { pbftMessage =>
            if (pbftMessage.message.blockMetadata.epochNumber == epochState.epoch.info.number)
              processPbftMessage(pbftMessage)
          }
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

  private def handleProtocolMessage(
      message: Consensus.ProtocolMessage
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case stateTransferMessage: Consensus.StateTransferMessage =>
        serverStateTransferManager.handleStateTransferMessage(
          stateTransferMessage,
          activeTopologyInfo,
          latestCompletedEpoch,
        )(abort) match {
          case StateTransferMessageResult.Continue =>
          case other => abort(s"Unexpected result $other from server-side state transfer manager")
        }
      case _ =>
        ifInitCompleted(message) {
          case localAvailabilityMessage: Consensus.LocalAvailability =>
            handleLocalAvailabilityMessage(localAvailabilityMessage)

          case consensusMessage: Consensus.ConsensusMessage =>
            handleConsensusMessage(consensusMessage)

          case Consensus.RetransmissionsMessage.VerifiedNetworkMessage(
                Consensus.RetransmissionsMessage.RetransmissionRequest(
                  EpochStatus(from, epochNumber, _)
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
            retransmissionsManager.handleMessage(activeTopologyInfo.currentCryptoProvider, msg)

          case _: Consensus.StateTransferMessage => // handled at the top regardless of the init, just to make the match exhaustive
        }
    }

  private def handleLocalAvailabilityMessage(
      localAvailabilityMessage: Consensus.LocalAvailability
  )(implicit traceContext: TraceContext): Unit =
    localAvailabilityMessage match {
      case Consensus.LocalAvailability.ProposalCreated(orderingBlock, epochNumber) =>
        epochState.proposalCreated(orderingBlock, epochNumber)
    }

  private def handleConsensusMessage(
      consensusMessage: Consensus.ConsensusMessage
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(consensusMessage)

    consensusMessage match {
      case Consensus.ConsensusMessage.PbftVerifiedNetworkMessage(pbftEvent) =>
        processPbftMessage(pbftEvent)

      case Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage(underlyingNetworkMessage) =>
        val from = underlyingNetworkMessage.from
        val keyId = FingerprintKeyId.toBftKeyId(underlyingNetworkMessage.signature.signedBy)
        val blockMetadata = underlyingNetworkMessage.message.blockMetadata
        val epochNumber = blockMetadata.epochNumber
        val blockNumber = blockMetadata.blockNumber
        val viewNumber = underlyingNetworkMessage.message.viewNumber

        def emitNonComplianceMetric(): Unit =
          emitNonCompliance(metrics)(
            from,
            epochNumber,
            viewNumber,
            blockNumber,
            metrics.security.noncompliant.labels.violationType.values.ConsensusInvalidMessage,
          )

        if (messageAuthorizer.isAuthorized(from, keyId)) {
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
        } else {
          emitNonComplianceMetric()
          logger.warn(
            s"Received a message from '$from' signed with '$keyId' " +
              "but it is unauthorized in the current ordering topology " +
              s"${activeTopologyInfo.currentTopology.nodesTopologyInfo}, dropping it"
          )
        }

      case Consensus.ConsensusMessage.BlockOrdered(
            orderedBlock: OrderedBlock,
            commitCertificate: CommitCertificate,
          ) =>
        emitConsensusLatencyStats(metrics)

        epochState.confirmBlockCompleted(orderedBlock.metadata, commitCertificate)

        epochState.epochCompletionStatus match {
          case EpochState.Complete(commitCertificates) =>
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
    val prefix = actingOnMessageType.map(mt => s"$mt: ").getOrElse("")
    val completeEpochNumber = completeEpochSnapshot.info.number
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
      newMembership: Membership,
      newCryptoProvider: CryptoProvider[E],
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val currentEpochInfo = epochState.epoch.info
    if (currentEpochInfo == newEpochInfo) {
      // It can happen after storing the first epoch after state transfer.
      logger.debug(s"New epoch state for epoch ${newEpochInfo.number} already set")
    } else if (
      newEpochInfo.number == currentEpochInfo.number + 1 || currentEpochInfo == GenesisEpochInfo
    ) {
      activeTopologyInfo = activeTopologyInfo.updateMembership(newMembership, newCryptoProvider)
      messageAuthorizer = activeTopologyInfo.currentTopology

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

  private def processPbftMessage(
      pbftMessage: SignedMessage[ConsensusSegment.ConsensusMessage.PbftNetworkMessage]
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val pbftMessagePayload = pbftMessage.message
    val pbftMessageBlockMetadata = pbftMessagePayload.blockMetadata

    def emitNonComplianceMetric(): Unit =
      emitNonCompliance(metrics)(
        pbftMessagePayload.from,
        pbftMessageBlockMetadata.epochNumber,
        pbftMessagePayload.viewNumber,
        pbftMessageBlockMetadata.blockNumber,
        metrics.security.noncompliant.labels.violationType.values.ConsensusInvalidMessage,
      )

    lazy val messageType = shortType(pbftMessagePayload)
    logger.debug(
      s"$messageType: received from ${pbftMessage.from} w/ metadata $pbftMessageBlockMetadata"
    )

    val pbftMessageEpochNumber = pbftMessageBlockMetadata.epochNumber
    val thisNodeEpochNumber = epochState.epoch.info.number
    val updatedEpoch =
      catchupDetector.updateLatestKnownNodeEpoch(pbftMessage.from, pbftMessageEpochNumber)

    // Messages from stale epochs are discarded.
    if (pbftMessageEpochNumber < thisNodeEpochNumber) {
      logger.info(
        s"Discarded PBFT message $messageType about block ${pbftMessageBlockMetadata.blockNumber} " +
          s"at epoch $pbftMessageEpochNumber because we're at a later epoch ($thisNodeEpochNumber)"
      )
    } else if (pbftMessageEpochNumber > thisNodeEpochNumber) {
      // Messages from future epoch are queued to be processed when we move to that epoch.
      futurePbftMessageQueue.enqueue(pbftMessage)
      logger.debug(
        s"Queued PBFT message $messageType from future epoch $pbftMessageEpochNumber " +
          s"as we're still in epoch $thisNodeEpochNumber"
      )

      startCatchupIfNeeded(updatedEpoch, pbftMessageEpochNumber).discard
    } else if (
      pbftMessageBlockMetadata.blockNumber < epochState.epoch.info.startBlockNumber || pbftMessageBlockMetadata.blockNumber > epochState.epoch.info.lastBlockNumber
    ) {
      // Messages with block numbers out of bounds of the epoch are discarded.
      val epochInfo = epochState.epoch.info
      logger.warn(
        s"Discarded PBFT message $messageType about block ${pbftMessageBlockMetadata.blockNumber}" +
          s"from epoch $pbftMessageEpochNumber (current epoch number = ${epochInfo.number}, " +
          s"first block = ${epochInfo.startBlockNumber}, epoch length = ${epochInfo.length}) " +
          "because the block number is out of bounds of the current epoch"
      )
      emitNonComplianceMetric()
    } else if (!activeTopologyInfo.currentTopology.contains(pbftMessage.from)) {
      // Message is for current epoch but is not from a node in this epoch's topology; this is non-compliant
      //  behavior because correct BFT nodes are supposed not to start consensus for epochs they are not part of.
      // TODO(i18194) Check signature that message is from this node
      logger.warn(
        s"Discarded PBFT message $messageType message from '${pbftMessage.from}' not in the current epoch's topology"
      )
      emitNonComplianceMetric()
    } else {
      epochState.processPbftMessage(PbftSignedNetworkMessage(pbftMessage))
    }
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
    if (updatedEpoch && catchupDetector.shouldCatchUp(currentEpochNumber)) {
      logger.debug(
        s"Switching to catch-up state transfer while in epoch $currentEpochNumber; latestCompletedEpoch is "
          + s"$latestCompletedEpochNumber and message epoch is $pbftMessageEpochNumber"
      )
      startStateTransfer(currentEpochNumber, StateTransferType.Catchup)
      true
    } else {
      false
    }
  }

  private def startStateTransfer(
      startEpochNumber: EpochNumber,
      stateTransferType: StateTransferType,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    logger.info(s"Starting $stateTransferType state transfer from epoch $startEpochNumber")
    val newBehavior = new StateTransferBehavior(
      epochLength,
      StateTransferBehavior.InitialState[E](
        startEpochNumber,
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
}

object IssConsensusModule {

  final case class InitialState[E <: Env[E]](
      topologyInfo: OrderingTopologyInfo[E],
      epochState: EpochState[E],
      latestCompletedEpoch: EpochStore.Epoch,
      sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
  )

  val DefaultDatabaseReadTimeout: FiniteDuration = 10.seconds

  val DefaultLeaderSelectionPolicy: LeaderSelectionPolicy = SimpleLeaderSelectionPolicy

  def parseNetworkMessage(
      protoSignedMessage: v30.SignedMessage
  ): ParsingResult[Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage] =
    SignedMessage
      .fromProtoWithNodeId(v30.ConsensusMessage)(from =>
        proto => originalByteString => parseConsensusNetworkMessage(from, proto)(originalByteString)
      )(protoSignedMessage)
      .map(Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage.apply)

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

  def parseRetransmissionMessage(from: BftNodeId, message: v30.RetransmissionMessage)(
      originalByteString: ByteString
  ): ParsingResult[Consensus.RetransmissionsMessage.RetransmissionsNetworkMessage] =
    message.message match {
      case v30.RetransmissionMessage.Message.RetransmissionRequest(value) =>
        Consensus.RetransmissionsMessage.RetransmissionRequest.fromProto(from, value)(
          originalByteString
        )
      case v30.RetransmissionMessage.Message.RetransmissionResponse(value) =>
        Consensus.RetransmissionsMessage.RetransmissionResponse.fromProto(from, value)(
          originalByteString
        )
      case v30.RetransmissionMessage.Message.Empty =>
        Left(ProtoDeserializationError.OtherError("Empty Received"))
    }

  def parseStateTransferMessage(
      from: BftNodeId,
      message: v30.StateTransferMessage,
  )(
      originalByteString: ByteString
  ): ParsingResult[Consensus.StateTransferMessage.StateTransferNetworkMessage] =
    message.message match {
      case v30.StateTransferMessage.Message.BlockRequest(value) =>
        Right(
          Consensus.StateTransferMessage.BlockTransferRequest.fromProto(from, value)(
            originalByteString
          )
        )
      case v30.StateTransferMessage.Message.BlockResponse(value) =>
        Consensus.StateTransferMessage.BlockTransferResponse.fromProto(from, value)(
          originalByteString
        )
      case v30.StateTransferMessage.Message.Empty =>
        Left(ProtoDeserializationError.OtherError("Empty Received"))
    }

  @VisibleForTesting
  private[bftordering] def unapply(issConsensusModule: IssConsensusModule[?]): Option[
    (
        EpochLength,
        Option[SequencerSnapshotAdditionalInfo],
        OrderingTopologyInfo[?],
        mutable.Queue[SignedMessage[PbftNetworkMessage]],
        Seq[Consensus.Message[?]],
    )
  ] =
    Some(
      (
        issConsensusModule.epochLength,
        issConsensusModule.initialState.sequencerSnapshotAdditionalInfo,
        issConsensusModule.activeTopologyInfo,
        issConsensusModule.futurePbftMessageQueue,
        issConsensusModule.queuedConsensusMessages,
      )
    )
}
