// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultLeaderSelectionPolicy
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferState.PrePreparesFromState
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.{
  EpochState,
  TimeoutManager,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/** A class handling state transfer instances (one at a time on the initiating/receiving side and potentially multiple
  * ones on the serving side).
  *
  * It is meant to be used by the main Consensus module only and is not thread-safe.
  *
  * Design document: https://docs.google.com/document/d/1oB1KtnpM7OiNDWQoRUL0NuoEFJYUjg58ECYIjSi4sIM
  */
class StateTransferManager[E <: Env[E]](
    dependencies: ConsensusModuleDependencies[E],
    epochLength: EpochLength, // TODO(#19661): Support transferring epochs with different lengths
    epochStore: EpochStore[E],
    thisPeer: SequencerId,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import StateTransferManager.*

  private val stateTransferState = mutable.HashMap[BlockNumber, StateTransferState]()

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var stateTransferStartEpoch: Option[EpochNumber] = None

  private val blockTransferResponseTimeouts
      : mutable.Map[SequencerId, TimeoutManager[E, Consensus.Message[E], SequencerId]] =
    mutable.Map.empty

  def isInStateTransfer: Boolean = stateTransferStartEpoch.isDefined

  def clearStateTransferState()(implicit traceContext: TraceContext): Unit = {
    logger.info("State transfer: clearing state")
    blockTransferResponseTimeouts.view.values.foreach(_.cancelTimeout())
    blockTransferResponseTimeouts.clear()
    stateTransferState.clear()
    stateTransferStartEpoch = None
  }

  def startStateTransfer(
      activeMembership: Membership,
      latestCompletedEpoch: EpochStore.Epoch,
      startEpoch: EpochNumber,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (isInStateTransfer) {
      // TODO(#19661): Verify when implementing catch-up
      logger.debug("State transfer: already in progress")
    } else {
      val latestCompletedEpochNumber = latestCompletedEpoch.info.number
      logger.info(
        s"State transfer: requesting, start epoch is $startEpoch, latest completed epoch is $latestCompletedEpochNumber"
      )
      stateTransferStartEpoch = Some(startEpoch)
      activeMembership.otherPeers.foreach { peerId =>
        blockTransferResponseTimeouts
          .put(peerId, new TimeoutManager(loggerFactory, RetryTimeout, peerId))
          .foreach(_ => abort(s"There should be no timeout manager for peer $peerId yet"))

        val blockTransferRequest = StateTransferMessage.BlockTransferRequest
          .create(startEpoch, latestCompletedEpochNumber, activeMembership.myId)
        sendBlockTransferRequest(blockTransferRequest, to = peerId)(abort)
      }
    }

  def handleStateTransferMessage(
      message: Consensus.StateTransferMessage,
      activeMembership: Membership,
      latestCompletedEpoch: EpochStore.Epoch,
  )(completeInit: () => Unit, abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Option[NewEpochState] =
    message match {
      case request @ StateTransferMessage.BlockTransferRequest(
            startEpoch,
            remoteLatestCompletedEpoch,
            from,
          ) =>
        if (activeMembership.otherPeers.contains(from)) {
          if (remoteLatestCompletedEpoch == GenesisEpochNumber && startEpoch > GenesisEpochNumber) {
            logger.info(s"State transfer: peer $from is starting onboarding from epoch $startEpoch")
          } else {
            logger.info(s"State transfer: peer $from is requesting blocks from epoch $startEpoch")
          }
          sendBlockResponse(
            to = from,
            request.startEpoch,
            latestCompletedEpoch,
          )
        } else {
          logger.info(
            s"State transfer: peer $from is requesting state transfer while not being active, " +
              s"active membership is: $activeMembership, latest completed epoch is: ${latestCompletedEpoch.info.number}, " +
              s"dropping..."
          )
        }
        NoEpochStateUpdates

      case StateTransferMessage.BlockTransferResponse(_, _, Seq(), Seq(), from) =>
        if (isInStateTransfer) {
          cancelTimeoutForPeer(from)(abort)
          handleEmptyBlockResponse(completeInit, abort)
        } else {
          logger.info(
            s"State transfer: received an empty block transfer response while not in state transfer"
          )
        }
        NoEpochStateUpdates

      case response: StateTransferMessage.BlockTransferResponse =>
        if (isInStateTransfer) {
          // TODO(#19661): Validate response, e.g., if the previous BFT time is right
          cancelTimeoutForPeer(response.from)(abort)
          handleNonEmptyBlockResponse(response, activeMembership)
        } else {
          logger.info(
            s"State transfer: received a block transfer response up to epoch ${response.latestCompletedEpoch}) " +
              "while not in state transfer"
          )
        }
        NoEpochStateUpdates

      case StateTransferMessage.SendBlockTransferRequest(blockTransferRequest, to) =>
        sendBlockTransferRequest(blockTransferRequest, to)(abort)
        NoEpochStateUpdates

      case StateTransferMessage.BlockStored(prePrepare, fullResponse) =>
        if (isInStateTransfer) {
          handleStoredPrePrepare(prePrepare, fullResponse, activeMembership)(abort)
        } else {
          logger.info(
            s"State transfer: received a block stored message for block ${prePrepare.blockMetadata} " +
              "while not in state transfer"
          )
          NoEpochStateUpdates
        }
    }

  private def sendBlockTransferRequest(
      blockTransferRequest: StateTransferMessage.BlockTransferRequest,
      to: SequencerId,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (isInStateTransfer) {
      blockTransferResponseTimeouts
        .getOrElse(to, abort(s"No timeout manager for peer $to"))
        .scheduleTimeout(
          StateTransferMessage.SendBlockTransferRequest(blockTransferRequest, to)
        )
      logger.debug(s"State transfer: sending a block transfer request to $to")
      dependencies.p2pNetworkOut.asyncSend(
        P2PNetworkOut.send(blockTransferRequest.toProto, signature = None, to)
      )
    } else {
      logger.info(
        s"State transfer: not sending a block transfer request to $to when not in state transfer (likely a race)"
      )
    }

  private def sendBlockResponse(
      to: SequencerId,
      startEpoch: EpochNumber,
      latestCompletedEpoch: EpochStore.Epoch,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val lastEpochToTransfer = latestCompletedEpoch.info.number
    val lastEpochToTransferTopologySnapshotEffectiveTime =
      latestCompletedEpoch.info.topologySnapshotEffectiveTime
    if (lastEpochToTransfer < startEpoch) {
      logger.info(
        s"State transfer: nothing to transfer to $to, start epoch was $startEpoch, " +
          s"latest locally completed epoch is $lastEpochToTransfer"
      )
      val responseProto = StateTransferMessage.BlockTransferResponse
        .create(
          lastEpochToTransfer,
          lastEpochToTransferTopologySnapshotEffectiveTime,
          prePrepares = Seq.empty,
          lastBlockCommits = Seq.empty,
          from = thisPeer,
        )
        .toProto
      dependencies.p2pNetworkOut.asyncSend(P2PNetworkOut.send(responseProto, signature = None, to))
    } else {
      logger.info(
        s"State transfer: loading pre-prepares from epochs $startEpoch to $lastEpochToTransfer"
      )
      context.pipeToSelf(
        epochStore.loadPrePreparesForCompleteBlocks(startEpoch, lastEpochToTransfer)
      ) {
        case Success(prePrepares) =>
          val startBlockNumber = prePrepares.map(_.blockMetadata.blockNumber).minOption
          logger.info(
            s"State transfer: sending blocks starting from epoch $startEpoch (block number = $startBlockNumber) up to " +
              s"$lastEpochToTransfer (inclusive) to $to"
          )
          val responseProto = StateTransferMessage.BlockTransferResponse
            .create(
              lastEpochToTransfer,
              lastEpochToTransferTopologySnapshotEffectiveTime,
              prePrepares,
              latestCompletedEpoch.lastBlockCommitMessages,
              from = thisPeer,
            )
            .toProto
          dependencies.p2pNetworkOut.asyncSend(
            P2PNetworkOut.send(responseProto, signature = None, to)
          )
          None // do not send anything back
        case Failure(exception) => Some(Consensus.ConsensusMessage.AsyncException(exception))
      }
    }
  }

  private def handleEmptyBlockResponse(completeInit: () => Unit, abort: String => Nothing)(implicit
      traceContext: TraceContext
  ): Unit = {
    // TODO(#19661): Wait for f+1
    logger.info("State transfer: nothing to be transferred to us")
    val startEpoch = stateTransferStartEpoch.getOrElse(abort("Should be in state transfer"))
    if (startEpoch > GenesisEpochNumber) {
      abort(
        s"Empty block transfer response is only supported for onboarding from genesis, start epoch = $startEpoch"
      )
      // Serving nodes figure out that a new node is onboarded once the "start epoch" is completed.
      // So in that case, there is always at least 1 epoch to transfer.
    }
    clearStateTransferState()
    completeInit()
  }

  private def handleNonEmptyBlockResponse(
      response: StateTransferMessage.BlockTransferResponse,
      activeMembership: Membership,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    response.prePrepares.foreach { prePrepare =>
      val blockNumber = prePrepare.blockMetadata.blockNumber
      stateTransferState.get(blockNumber) match {
        case Some(inProgress: StateTransferState.InProgress) =>
          val updatedState = StateTransferState.InProgress(
            inProgress.prePreparesFromState.prePrepares :+ prePrepare
          )
          stateTransferState.update(blockNumber, updatedState)
          storePrePrepareIfQuorumReceived(
            prePrepare,
            updatedState.prePreparesFromState,
            response,
            activeMembership,
          )
        case Some(StateTransferState.Done(_)) =>
          logger.debug(
            s"State transfer: received block $blockNumber for which we have a quorum of messages already, dropping..."
          )
        case None =>
          val newState = StateTransferState.InProgress(Seq(prePrepare))
          stateTransferState.update(blockNumber, newState)
          storePrePrepareIfQuorumReceived(
            prePrepare,
            newState.prePreparesFromState,
            response,
            activeMembership,
          )
      }
    }

  private def storePrePrepareIfQuorumReceived(
      prePrepare: PrePrepare,
      prePreparesFromState: PrePreparesFromState,
      fullResponse: StateTransferMessage.BlockTransferResponse,
      activeMembership: Membership,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit =
    if (prePreparesFromState.prePrepares.sizeIs >= activeMembership.orderingTopology.weakQuorum) {
      // TODO(#19661): Check if messages are correct, calculate e_end
      // TODO(#19661): Figure out how to verify the last block commits for the calculated e_end (get f+1 sets?)

      // TODO(#19661): Send and store commits (so that they can be used for retransmissions)
      context.pipeToSelf(epochStore.addOrderedBlock(prePrepare, commitMessages = Seq.empty)) {
        case Success(()) =>
          Some(StateTransferMessage.BlockStored(prePrepare, fullResponse))
        case Failure(exception) =>
          Some(Consensus.ConsensusMessage.AsyncException(exception))
      }
    }

  private def handleStoredPrePrepare(
      prePrepare: PrePrepare,
      fullResponse: StateTransferMessage.BlockTransferResponse,
      activeMembership: Membership,
  )(abort: String => Nothing)(implicit traceContext: TraceContext): Option[NewEpochState] = {
    val blockMetadata = prePrepare.blockMetadata
    val blockNumber = blockMetadata.blockNumber
    val prePrepareEpochNumber = blockMetadata.epochNumber
    stateTransferState.update(blockNumber, StateTransferState.Done(prePrepareEpochNumber))

    logger.debug(s"State transfer: sending block $blockMetadata to Output")
    val lastEpochToTransfer = fullResponse.latestCompletedEpoch
    sendBlockToOutput(prePrepare, lastEpochToTransfer)

    val blocksTransferred = stateTransferState.filter {
      case (_, StateTransferState.Done(_)) => true
      case _ => false
    }
    val startEpoch = stateTransferStartEpoch.getOrElse(abort("Should be in state transfer"))
    val numberOfEpochsToTransfer = lastEpochToTransfer - startEpoch + 1
    val numberOfBlocksToTransfer = numberOfEpochsToTransfer * epochLength
    if (blocksTransferred.size >= numberOfBlocksToTransfer) {
      if (blocksTransferred.size > numberOfBlocksToTransfer) {
        // We don't want to block progress in presence of malicious or faulty nodes.
        logger.warn(
          s"State transfer: received more blocks (${blocksTransferred.size}) than supposed to ($numberOfBlocksToTransfer)"
        )
      }
      logger.info(
        s"State transfer: finishing block transfer at epoch $lastEpochToTransfer with $numberOfEpochsToTransfer epochs " +
          s"($numberOfBlocksToTransfer blocks) transferred"
      )
      val firstBlockInLastEpoch = getFirstBlockInEpoch(lastEpochToTransfer)(abort)
      val lastEpochInfo = EpochInfo(
        // Note that the Output module will send an epoch number greater by 1 once the batches are transferred.
        lastEpochToTransfer,
        firstBlockInLastEpoch,
        epochLength,
        fullResponse.latestCompletedEpochTopologySnapshotEffectiveTime,
      )
      val lastEpoch = EpochStore.Epoch(lastEpochInfo, fullResponse.lastBlockCommits)
      val epochState =
        EpochState.Epoch(lastEpochInfo, activeMembership, DefaultLeaderSelectionPolicy)
      Some(NewEpochState(epochState, lastEpoch))
    } else {
      NoEpochStateUpdates
    }
  }

  private def sendBlockToOutput(prePrepare: PrePrepare, lastEpochToTransfer: EpochNumber): Unit = {
    val blockMetadata = prePrepare.blockMetadata
    // TODO(#19661): Calculate reasonably
    // We disable `NewEpochTopology` notifications from output to consensus, which is inactive during state transfer,
    //  by never telling the output module that there are blocks that are last in their epoch. The last
    //  state-transferred block will have the flag set to true, though, so that consensus receives the topology for,
    //  and starts processing, the next epoch.
    val isLastInEpoch =
      blockMetadata.blockNumber == (lastEpochToTransfer * epochLength) + epochLength - 1

    dependencies.output.asyncSend(
      Output.BlockOrdered(
        OrderedBlockForOutput(
          OrderedBlock(
            blockMetadata,
            prePrepare.block.proofs,
            prePrepare.canonicalCommitSet,
          ),
          prePrepare.from,
          isLastInEpoch,
          isStateTransferred = true,
        )
      )
    )
  }

  private def cancelTimeoutForPeer(
      peerId: SequencerId
  )(abort: String => Nothing)(implicit traceContext: TraceContext): Unit =
    blockTransferResponseTimeouts
      .remove(peerId)
      .getOrElse(abort(s"No timeout manager for peer $peerId"))
      .cancelTimeout()

  private def getFirstBlockInEpoch(epochNumber: EpochNumber)(abort: String => Nothing) =
    stateTransferState.view
      .flatMap { case (blockNumber, state) =>
        state match {
          case StateTransferState.InProgress(_) =>
            abort("All blocks should be transferred at this point")
          case StateTransferState.Done(epochNumberFromState)
              if epochNumberFromState == epochNumber =>
            Some(blockNumber)
          case _ => None
        }
      }
      .minOption
      .getOrElse {
        abort(s"There should be a first block in state transferred epoch $epochNumber")
      }
}

object StateTransferManager {
  val NoEpochStateUpdates: Option[NewEpochState] = None

  private val RetryTimeout = 10.seconds

  final case class NewEpochState(epochState: EpochState.Epoch, epoch: EpochStore.Epoch)
}
