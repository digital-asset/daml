// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.metrics.BftOrderingMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.ConsensusMessage.Message
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  ConsensusMessage,
  StateTransferMessage as ProtoStateTransferMessage,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.{
  DefaultLeaderSelectionPolicy,
  StartupState,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.{
  emitConsensusLatencyStats,
  emitNonCompliance,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.leaders.{
  LeaderSelectionPolicy,
  SimpleLeaderSelectionPolicy,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferManager
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferManager.NewEpochState
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.validation.IssConsensusValidator
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.{
  HasDelayedInit,
  shortType,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Consensus.ConsensusMessage.{
  PbftUnverifiedNetworkMessage,
  PbftVerifiedNetworkMessage,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Consensus.NewEpochTopology
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage.headerFromProto
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
  Output,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class IssConsensusModule[E <: Env[E]](
    epochLength: EpochLength, // Currently fixed for all epochs
    startupState: StartupState[E],
    epochStore: EpochStore[E],
    clock: Clock,
    metrics: BftOrderingMetrics,
    segmentModuleRefFactory: SegmentModuleRefFactory[E],
    thisPeer: SequencerId,
    override val dependencies: ConsensusModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(
    stateTransferManager: StateTransferManager[E] =
      new StateTransferManager(dependencies, epochLength, epochStore, thisPeer, loggerFactory)
)(implicit mc: MetricsContext)
    extends Consensus[E]
    with HasDelayedInit[Consensus.ProtocolMessage] {

  private val validator = new IssConsensusValidator[E]

  logger.debug(
    "Starting with " +
      s"membership = ${startupState.membership}, " +
      s"latest completed epoch = ${startupState.latestCompletedEpoch.info}, " +
      s"current epoch = ${startupState.epochState.epoch.info} (completed: ${startupState.epochState.isEpochComplete})"
  )(TraceContext.empty)

  private var latestCompletedEpoch: EpochStore.Epoch = startupState.latestCompletedEpoch
  private var activeMembership = startupState.membership
  private var activeCryptoProvider = startupState.cryptoProvider
  private var epochState = startupState.epochState
  @VisibleForTesting
  private[bftordering] def getEpochState: EpochState[E] = epochState

  private val futurePbftMessageQueue = new mutable.Queue[PbftNetworkMessage]()

  private var newEpochTopology: Option[(OrderingTopology, CryptoProvider[E])] = None

  override def ready(self: ModuleRef[Consensus.Message[E]]): Unit = {
    // TODO(#16761)- resend locally-led ordered blocks (PrePrepare) in activeEpoch in case my node crashed
  }

  override protected def receiveInternal(message: Consensus.Message[E])(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case Consensus.Init =>
        abortInit(
          s"${PreIssConsensusModule.getClass.toString} should be the only one receiving ${Consensus.Init.getClass.getSimpleName}"
        )

      case Consensus.Start =>
        startupState.sequencerSnapshotAdditionalInfo match {
          case Some(snapshotAdditionalInfo)
              if latestCompletedEpoch == GenesisEpoch && activeMembership.otherPeers.sizeIs > 0 =>
            stateTransferManager.clearStateTransferState()
            val startEpoch = snapshotAdditionalInfo.peerFirstKnownAt
              .get(activeMembership.myId)
              .flatMap(_.epochNumber)
              .getOrElse(
                abort("No starting epoch found for new node onboarding")
              )
            stateTransferManager.startStateTransfer(
              activeMembership,
              latestCompletedEpoch,
              startEpoch,
            )(abort)
          case _ =>
            startSegmentModulesAndCompleteInit()
        }

      case Consensus.Admin.GetOrderingTopology(callback) =>
        callback(epochState.epoch.info.number, activeMembership.orderingTopology.peers)

      case message: Consensus.ProtocolMessage => handleProtocolMessage(message)

      // Not received during state transfer, when consensus is inactive
      case Consensus.NewEpochTopology(
            newEpochNumber,
            newOrderingTopology,
            cryptoProvider: CryptoProvider[E],
          ) =>
        val latestCompletedEpochNumber = latestCompletedEpoch.info.number
        if (latestCompletedEpochNumber == newEpochNumber - 1) {
          val currentEpochNumber = epochState.epoch.info.number
          if (currentEpochNumber == newEpochNumber) {
            // The output module may re-send the topology for the current epoch upon restart if it didn't store
            //  the first block metadata or if the subscribing sequencer runtime hasn't processed it yet.
            logger.debug(
              s"Received NewEpochTopology event for epoch $newEpochNumber, but the epoch has already started; ignoring it"
            )
          } else if (currentEpochNumber == newEpochNumber - 1) {
            // The epoch has been completed and the new one hasn't started yet: start it
            logger.debug(s"Starting new epoch $newEpochNumber from NewEpochTopology event")
            startNewEpoch(newOrderingTopology, cryptoProvider)
          } else {
            abort(
              s"Received NewEpochTopology event for epoch $newEpochNumber, but the current epoch number is neither $newEpochNumber nor ${newEpochNumber - 1} ($currentEpochNumber)"
            )
          }
        } else if (latestCompletedEpochNumber < newEpochNumber - 1) {
          logger.debug(
            s"Epoch (${newEpochNumber - 1}) has not yet been completed: remembering the topology and " +
              s"waiting for the completed epoch to be stored"
          )
          // Upon epoch completion, a new epoch with this topology will be started.
          newEpochTopology = Some(newOrderingTopology -> cryptoProvider)
        } else { // latestCompletedEpochNumber >= epochNumber
          // The output module re-sent a topology for an already completed epoch; this can happen upon restart if
          //  either the output module, or the subscribing sequencer runtime, or both are more than one epoch behind
          //  consensus, because the output module will just reprocess the blocks to be recovered.
          logger.info(
            s"Received NewEpochTopology for epoch $newEpochNumber, but the latest completed epoch is already $latestCompletedEpochNumber; ignoring"
          )
        }

      case Consensus.NewEpochStored(newEpochInfo, membership, cryptoProvider) =>
        logger.debug(s"Stored new epoch ${newEpochInfo.number}")

        // Reset any topology remembered while waiting for the previous (completed) epoch to be stored.
        newEpochTopology = None

        // Update the topology and start a new epoch.
        activeMembership = membership
        activeCryptoProvider = cryptoProvider

        val newEpoch =
          Epoch(
            newEpochInfo,
            activeMembership,
            DefaultLeaderSelectionPolicy,
          )

        epochState = new EpochState(
          newEpoch,
          clock,
          abort,
          metrics,
          segmentModuleRefFactory = segmentModuleRefFactory(
            context,
            newEpoch,
            activeCryptoProvider,
            latestCompletedEpoch.lastBlockCommitMessages,
            epochInProgress = EpochStore.EpochInProgress(
              completedBlocks = Seq.empty,
              pbftMessagesForIncompleteBlocks = Seq.empty,
            ),
          ),
          completedBlocks = Seq.empty,
          loggerFactory = loggerFactory,
          timeouts = timeouts,
        )
        if (stateTransferManager.isInStateTransfer) {
          stateTransferManager.clearStateTransferState()
        }
        startSegmentModulesAndCompleteInit()
        logger.debug(
          s"New epoch: ${epochState.epoch.info.number} has started with ordering topology ${activeMembership.orderingTopology}"
        )

        // Process messages for this epoch that may have arrived when processing the previous one.
        val queuedMessages =
          futurePbftMessageQueue.dequeueAll(
            _.blockMetadata.epochNumber == epochState.epoch.info.number
          )
        queuedMessages.foreach(processPbftMessage)
    }

  private def handleProtocolMessage(
      message: Consensus.ProtocolMessage
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case stateTransferMessage: Consensus.StateTransferMessage =>
        val maybeNewEpochState =
          stateTransferManager.handleStateTransferMessage(
            stateTransferMessage,
            activeMembership,
            latestCompletedEpoch,
          )(() => startSegmentModulesAndCompleteInit(), abort)

        maybeNewEpochState.foreach { case NewEpochState(newEpochState, epoch) =>
          logger.info(
            s"State transfer: received new epoch state, epoch info = ${epoch.info}, updating"
          )
          latestCompletedEpoch = epoch
          val currentEpochNumber = epochState.epoch.info.number
          val newEpochNumber = newEpochState.info.number
          if (newEpochNumber < currentEpochNumber)
            abort("Should not state transfer to previously completed epoch")
          else if (newEpochNumber > currentEpochNumber) {
            epochState.completeEpoch(epochState.epoch.info.number)
            epochState.close()
            epochState = new EpochState(
              newEpochState,
              clock,
              abort,
              metrics,
              segmentModuleRefFactory(
                context,
                newEpochState,
                activeCryptoProvider,
                latestCompletedEpochLastCommits = epoch.lastBlockCommitMessages,
                epochInProgress = EpochStore.EpochInProgress(
                  completedBlocks = Seq.empty,
                  pbftMessagesForIncompleteBlocks = Seq.empty,
                ),
              ),
              loggerFactory = loggerFactory,
              timeouts = timeouts,
            )
          } // else it is equal, so we don't need to update the state
        }
      case _ =>
        ifInitCompleted(message) {
          case localAvailabilityMessage: Consensus.LocalAvailability =>
            handleLocalAvailabilityMessage(localAvailabilityMessage)

          case consensusMessage: Consensus.ConsensusMessage =>
            handleConsensusMessage(consensusMessage)

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
        context.pipeToSelf(
          validator.validate(underlyingNetworkMessage, context, activeCryptoProvider)
        ) {
          case Failure(error) =>
            logger.warn(s"Could not verify message $underlyingNetworkMessage, dropping", error)
            emitNonCompliance(metrics)(
              underlyingNetworkMessage.from,
              underlyingNetworkMessage.blockMetadata.epochNumber,
              underlyingNetworkMessage.viewNumber,
              underlyingNetworkMessage.blockMetadata.blockNumber,
              metrics.security.noncompliant.labels.violationType.values.ConsensusInvalidMessage,
            )
            None
          case Success(_) =>
            logger.debug(s"Message $underlyingNetworkMessage is valid")
            Some(PbftVerifiedNetworkMessage(underlyingNetworkMessage))
        }

      case Consensus.ConsensusMessage.BlockOrdered(
            orderedBlock: OrderedBlock,
            commits,
          ) =>
        emitConsensusLatencyStats(metrics)

        epochState.confirmBlockCompleted(orderedBlock.metadata, commits)

        if (epochState.isEpochComplete)
          completeEpoch()

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
              blockSegment.originalLeader,
              blockNumber == epochState.epoch.info.lastBlockNumber,
            )
          )
        )

      case Consensus.ConsensusMessage.CompleteEpochStored(epoch) =>
        logger.debug(s"$messageType: stored w/ epoch = ${epoch.info.number}")
        val currentEpochNumber = epochState.epoch.info.number

        if (epoch.info.number < currentEpochNumber)
          logger.info(
            s"Epoch ${epoch.info.number} already advanced; current epoch = $currentEpochNumber; ignoring"
          )
        else if (epoch.info.number > currentEpochNumber)
          abort(
            s"Trying to complete future epoch ${epoch.info.number} before local epoch $currentEpochNumber has caught up!"
          )
        else {
          epochState.completeEpoch(epoch.info.number)
          epochState.close()

          latestCompletedEpoch = epoch

          newEpochTopology match {
            case Some((orderingTopology, cryptoProvider)) =>
              startNewEpoch(orderingTopology, cryptoProvider)
            case None =>
              // We don't have the new topology for the new epoch yet: wait for it to arrive from the output module.
              ()
          }
        }

      case Consensus.ConsensusMessage.AsyncException(e: Throwable) =>
        logger.error(
          s"$messageType: exception raised from async consensus message: ${e.toString}"
        )
    }
  }

  private def startSegmentModulesAndCompleteInit()(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    if (epochState.epoch.info == GenesisEpoch.info) {
      logger.debug("Started at genesis, self-sending its topology to start epoch 0")
      context.self.asyncSend(
        NewEpochTopology(EpochNumber.First, activeMembership.orderingTopology, activeCryptoProvider)
      )
    } else if (!epochState.isEpochComplete) {
      logger.debug("Started during an in-progress epoch, starting segment modules")
      epochState.startSegmentModules()
    } else {
      logger.debug(
        "Started after a completed epoch but before starting a new one, waiting for topology from the output module"
      )
    }
    initCompleted(handleProtocolMessage(_)) // idempotent
  }

  private def startNewEpoch(
      orderingTopology: OrderingTopology,
      cryptoProvider: CryptoProvider[E],
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    metrics.consensus.votes.cleanupVoteGauges(keepOnly = orderingTopology.peers)
    val epochInfo = epochState.epoch.info
    epochState.emitEpochStats(metrics, epochInfo)

    val newEpochInfo = epochInfo.next(epochLength)

    logger.debug(s"Storing new epoch ${newEpochInfo.number}")
    pipeToSelf(epochStore.startEpoch(newEpochInfo)) {
      case Failure(exception) => Consensus.ConsensusMessage.AsyncException(exception)
      case Success(_) =>
        Consensus.NewEpochStored(
          newEpochInfo,
          activeMembership.copy(orderingTopology = orderingTopology),
          cryptoProvider,
        )
    }
  }

  private def processPbftMessage(
      msg: ConsensusSegment.ConsensusMessage.PbftNetworkMessage
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    lazy val messageType = shortType(msg)
    logger.debug(
      s"$messageType: received from ${msg.from} w/ metadata ${msg.blockMetadata}"
    )

    // Messages from stale epoch are discarded.
    if (msg.blockMetadata.epochNumber < epochState.epoch.info.number)
      logger.info(
        s"Discarded block ${msg.blockMetadata.blockNumber} at epoch ${msg.blockMetadata.epochNumber} because we're at a later epoch (${epochState.epoch.info.number})"
      )

    // Messages from future epoch are queued to be processed when we move to that epoch.
    else if (msg.blockMetadata.epochNumber > epochState.epoch.info.number) {
      futurePbftMessageQueue.enqueue(msg)
      logger.debug(
        s"Received PBFT message from epoch ${msg.blockMetadata.epochNumber} while being in epoch " +
          s"${epochState.epoch.info.number}, queued the message"
      )

      // Messages with blocks numbers out of bounds of the epoch are discarded.
    } else if (
      msg.blockMetadata.blockNumber < epochState.epoch.info.startBlockNumber || msg.blockMetadata.blockNumber > epochState.epoch.info.lastBlockNumber
    ) {
      val epochInfo = epochState.epoch.info
      logger.info(
        s"Discarded block ${msg.blockMetadata.blockNumber} from epoch ${msg.blockMetadata.epochNumber} (current epoch number = ${epochInfo.number}, " +
          s"first block = ${epochInfo.startBlockNumber}, epoch length = ${epochInfo.length}) because block number is out of bounds of the current epoch"
      )
    }

    // Message is for current epoch but is not from a peer in this epoch's topology
    // TODO(i18194) Check signature that message is from this peer
    else if (!activeMembership.orderingTopology.contains(msg.from))
      logger.info(
        s"Discarded Pbft message from peer ${msg.from} not in the current epoch's topology"
      )
    else
      epochState.processPbftMessage(msg)
  }

  private def completeEpoch()(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val epochInfo = epochState.epoch.info
    logger.debug(
      s"Storing completed epoch: ${epochInfo.number}, start block number = ${epochInfo.startBlockNumber}, length = ${epochInfo.length}"
    )
    val epochSnapshot = EpochStore.Epoch(epochInfo, epochState.lastBlockCommitMessages)

    pipeToSelf(epochStore.completeEpoch(epochInfo.number)) {
      case Failure(exception) => Consensus.ConsensusMessage.AsyncException(exception)
      case Success(_) => Consensus.ConsensusMessage.CompleteEpochStored(epochSnapshot)
    }
  }
}

object IssConsensusModule {

  final case class StartupState[E <: Env[E]](
      sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
      epochState: EpochState[E],
      latestCompletedEpoch: EpochStore.Epoch,
  )

  val DefaultEpochLength: EpochLength = EpochLength(10)

  val DefaultDatabaseReadTimeout: FiniteDuration = 10.seconds

  val DefaultLeaderSelectionPolicy: LeaderSelectionPolicy = SimpleLeaderSelectionPolicy

  def parseNetworkMessage(
      from: SequencerId,
      message: ConsensusMessage,
  ): ParsingResult[ConsensusSegment.ConsensusMessage.PbftNetworkMessage] =
    for {
      header <- headerFromProto(message)
      result <- (message.message match {
        case Message.PrePrepare(value) =>
          ConsensusSegment.ConsensusMessage.PrePrepare.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            value,
            from,
          )
        case Message.Prepare(value) =>
          ConsensusSegment.ConsensusMessage.Prepare.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            value,
            from,
          )
        case Message.Commit(value) =>
          ConsensusSegment.ConsensusMessage.Commit.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            value,
            from,
          )
        case Message.ViewChange(value) =>
          ConsensusSegment.ConsensusMessage.ViewChange.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            value,
            from,
          )
        case Message.NewView(value) =>
          ConsensusSegment.ConsensusMessage.NewView.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            value,
            from,
          )
        case Message.Empty => Left(ProtoDeserializationError.OtherError("Empty Received"))
      }): ParsingResult[ConsensusSegment.ConsensusMessage.PbftNetworkMessage]
    } yield result

  def parseUnverifiedNetworkMessage(
      from: SequencerId,
      message: ConsensusMessage,
  ): ParsingResult[Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage] =
    parseNetworkMessage(from, message).map(PbftUnverifiedNetworkMessage.apply)

  def parseStateTransferMessage(
      from: SequencerId,
      message: ProtoStateTransferMessage,
  ): ParsingResult[Consensus.StateTransferMessage] =
    message.message match {
      case ProtoStateTransferMessage.Message.BlockRequest(value) =>
        Right(Consensus.StateTransferMessage.BlockTransferRequest.fromProto(value, from))
      case ProtoStateTransferMessage.Message.BlockResponse(value) =>
        Consensus.StateTransferMessage.BlockTransferResponse.fromProto(value, from)
      case ProtoStateTransferMessage.Message.Empty =>
        Left(ProtoDeserializationError.OtherError("Empty Received"))
    }
}
