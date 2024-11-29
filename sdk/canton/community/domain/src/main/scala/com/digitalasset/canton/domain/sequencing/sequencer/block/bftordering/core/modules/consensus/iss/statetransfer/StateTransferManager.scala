// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultLeaderSelectionPolicy
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.{
  GenesisEpoch,
  GenesisEpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.{
  EpochState,
  TimeoutManager,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
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
) extends NamedLogging
    with StateTransferDetector {

  import StateTransferManager.*

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var stateTransferStartEpoch: Option[EpochNumber] = None

  private val collectedBlockTransferResponses =
    mutable.Map[SequencerId, StateTransferMessage.BlockTransferResponse]()

  private val blockTransferResponseTimeouts =
    mutable.Map[SequencerId, TimeoutManager[E, Consensus.Message[E], SequencerId]]()

  override def inStateTransfer: Boolean = stateTransferStartEpoch.isDefined

  def clearStateTransferState()(implicit traceContext: TraceContext): Unit = {
    logger.info("State transfer: clearing state")
    blockTransferResponseTimeouts.view.values.foreach(_.cancelTimeout())
    blockTransferResponseTimeouts.clear()
    collectedBlockTransferResponses.clear()
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
    if (inStateTransfer) {
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

        val blockTransferRequest =
          StateTransferMessage.BlockTransferRequest.create(
            startEpoch,
            latestCompletedEpochNumber,
            activeMembership.myId,
          )
        // TODO(#20458) properly sign this message
        val signedMessage =
          SignedMessage(
            blockTransferRequest,
            Signature.noSignature,
          )

        sendBlockTransferRequest(signedMessage, to = peerId)(abort)
      }
    }

  def handleStateTransferMessage(
      message: Consensus.StateTransferMessage,
      activeMembership: Membership,
      latestCompletedEpoch: EpochStore.Epoch,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
    message match {
      case StateTransferMessage.NetworkMessage(message) =>
        handleStateTransferNetworkMessage(message, activeMembership, latestCompletedEpoch)(abort)

      case StateTransferMessage.ResendBlockTransferRequest(blockTransferRequest, to) =>
        sendBlockTransferRequest(blockTransferRequest, to)(abort)
        StateTransferMessageResult.Continue

      case StateTransferMessage.BlocksStored(commitCertificates, endEpoch) =>
        if (inStateTransfer) {
          handleStoredBlocks(commitCertificates, endEpoch, activeMembership)(abort)
        } else {
          logger.info(
            s"State transfer: stored blocks up to epoch $endEpoch while not in state transfer"
          )
          StateTransferMessageResult.Continue
        }
    }

  private def handleStateTransferNetworkMessage(
      message: Consensus.StateTransferMessage.StateTransferNetworkMessage,
      activeMembership: Membership,
      latestCompletedEpoch: EpochStore.Epoch,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
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
          )(abort)
        } else {
          logger.info(
            s"State transfer: peer $from is requesting state transfer while not being active, " +
              s"active membership is: $activeMembership, latest completed epoch is: ${latestCompletedEpoch.info.number}, " +
              s"dropping..."
          )
        }
        StateTransferMessageResult.Continue

      case response: StateTransferMessage.BlockTransferResponse =>
        if (inStateTransfer) {
          // TODO(#19661): Validate response, e.g., if the previous BFT time is right
          cancelTimeoutForPeer(response.from)(abort)
          handleBlockTransferResponse(response, activeMembership)(abort)
        } else {
          logger.info(
            s"State transfer: received a block transfer response up to epoch ${response.latestCompletedEpoch} " +
              s"from ${response.from} while not in state transfer; ignoring unneeded response"
          )
          StateTransferMessageResult.Continue
        }
    }

  private def sendBlockTransferRequest(
      blockTransferRequest: SignedMessage[StateTransferMessage.BlockTransferRequest],
      to: SequencerId,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (inStateTransfer) {
      blockTransferResponseTimeouts
        .getOrElse(to, abort(s"No timeout manager for peer $to"))
        .scheduleTimeout(
          StateTransferMessage.ResendBlockTransferRequest(blockTransferRequest, to)
        )
      logger.debug(s"State transfer: sending a block transfer request to $to")
      dependencies.p2pNetworkOut.asyncSend(
        P2PNetworkOut.send(wrapSignedMessage(blockTransferRequest), to)
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
  )(
      abort: String => Nothing
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val lastEpochToTransfer = latestCompletedEpoch.info.number
    if (lastEpochToTransfer < startEpoch) {
      logger.info(
        s"State transfer: nothing to transfer to $to, start epoch was $startEpoch, " +
          s"latest locally completed epoch is $lastEpochToTransfer"
      )
      val response = StateTransferMessage.BlockTransferResponse
        .create(lastEpochToTransfer, commitCertificates = Seq.empty, from = thisPeer)
      val signedMessage =
        SignedMessage(response, Signature.noSignature) // TODO(#20458) properly sign this message
      dependencies.p2pNetworkOut.asyncSend(
        P2PNetworkOut.send(wrapSignedMessage(signedMessage), to)
      )
    } else {
      val blocksToTransfer = (lastEpochToTransfer - startEpoch + 1) * epochLength
      logger.info(
        s"State transfer: loading pre-prepares from epochs $startEpoch to $lastEpochToTransfer " +
          s"(blocksToTransfer = $blocksToTransfer)"
      )
      context.pipeToSelf(
        epochStore.loadCompleteBlocks(startEpoch, lastEpochToTransfer)
      ) {
        case Success(blocks) =>
          if (blocks.length != blocksToTransfer) {
            abort(
              "Internal invariant violation: " +
                s"only whole epochs with blocks that have ordered can be state transferred, but ${blocks.length} " +
                s"pre-prepares have been loaded instead of $blocksToTransfer"
            )
          }
          val commitCertificates = blocks.map(_.commitCertificate)
          val startBlockNumber = blocks.map(_.blockNumber).minOption
          logger.info(
            s"State transfer: sending blocks starting from epoch $startEpoch (block number = $startBlockNumber) up to " +
              s"$lastEpochToTransfer (inclusive) to $to"
          )
          val response = StateTransferMessage.BlockTransferResponse
            .create(lastEpochToTransfer, commitCertificates, from = thisPeer)
          val signedMessage = SignedMessage(
            response,
            Signature.noSignature,
          ) // TODO(#20458) properly sign this message
          dependencies.p2pNetworkOut.asyncSend(
            P2PNetworkOut.send(wrapSignedMessage(signedMessage), to)
          )
          None // do not send anything back
        case Failure(exception) => Some(Consensus.ConsensusMessage.AsyncException(exception))
      }
    }
  }

  private def handleBlockTransferResponse(
      response: StateTransferMessage.BlockTransferResponse,
      activeMembership: Membership,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult = {
    val from = response.from
    collectedBlockTransferResponses.put(from, response).discard
    val weakQuorum = activeMembership.orderingTopology.weakQuorum
    // TODO(#19661): Get quorum of **matching** responses
    if (collectedBlockTransferResponses.sizeIs == weakQuorum) {
      val quorumOfResponses = collectedBlockTransferResponses.view.values.toSeq
      handleQuorumOfBlockTransferResponses(quorumOfResponses)(abort)
    } else if (collectedBlockTransferResponses.sizeIs > weakQuorum) {
      logger.debug(
        "State transfer: already reached quorum of matching block transfer responses; dropping an additional copy " +
          s"from peer: $from"
      )
      StateTransferMessageResult.Continue
    } else {
      StateTransferMessageResult.Continue
    }
  }

  private def handleQuorumOfBlockTransferResponses(
      quorumOfBlockTransferResponses: Seq[StateTransferMessage.BlockTransferResponse]
  )(
      abort: String => Nothing
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ) = {
    val endEpoch = calculateEndEpoch(quorumOfBlockTransferResponses)(abort)
    val commitCertsUpToEndEpoch = quorumOfBlockTransferResponses
      .flatMap(_.commitCertificates)
      .takeWhile(_.prePrepare.message.blockMetadata.epochNumber <= endEpoch)

    if (commitCertsUpToEndEpoch.isEmpty) {
      handleEmptyStateTransferResponses(abort)
    } else {
      storeBlocks(commitCertsUpToEndEpoch, endEpoch)
      StateTransferMessageResult.Continue
    }
  }

  private def handleEmptyStateTransferResponses(
      abort: String => Nothing
  )(implicit traceContext: TraceContext): StateTransferMessageResult = {
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
    StateTransferMessageResult.NothingToStateTransfer
  }

  private def storeBlocks(
      commitCertificates: Seq[CommitCertificate],
      endEpoch: EpochNumber,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val futures = commitCertificates.map(commitCertificate =>
      epochStore.addOrderedBlock(commitCertificate.prePrepare, commitCertificate.commits)
    )
    val sequencedFuture = context.sequenceFuture(futures)
    context.pipeToSelf(sequencedFuture) {
      case Success(_) =>
        Some(StateTransferMessage.BlocksStored(commitCertificates, endEpoch))
      case Failure(exception) =>
        Some(Consensus.ConsensusMessage.AsyncException(exception))
    }
  }

  private def handleStoredBlocks(
      commitCertificates: Seq[CommitCertificate],
      endEpoch: EpochNumber,
      activeMembership: Membership,
  )(abort: String => Nothing)(implicit traceContext: TraceContext): StateTransferMessageResult = {
    val startEpoch = stateTransferStartEpoch.getOrElse(abort("Should be in state transfer"))
    val numberOfTransferredEpochs = endEpoch - startEpoch + 1

    logger.info(
      s"State transfer: finishing block transfer at epoch $endEpoch with $numberOfTransferredEpochs epochs " +
        s"(${commitCertificates.size} blocks) transferred"
    )

    val prePrepares = commitCertificates.map(_.prePrepare.message)
    prePrepares.foreach { prePrepare =>
      logger.debug(s"State transfer: sending block ${prePrepare.blockMetadata} to Output")
      sendBlockToOutput(prePrepare, endEpoch)
    }

    val firstBlockInLastEpoch = getFirstBlockInLastEpoch(prePrepares)(abort)
    val lastEpochInfo = EpochInfo(
      // Note that the Output module will send an epoch number greater by 1 once the batches are transferred.
      endEpoch,
      firstBlockInLastEpoch,
      epochLength,
      // TODO(#19661) Not used during state transfer but will likely need to be correctly saved to the DB
      //  to support restarts (or the approach must be changed).
      GenesisEpoch.info.topologyActivationTime,
    )
    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    val lastBlockCommits =
      commitCertificates.maxBy(_.prePrepare.message.blockMetadata.blockNumber).commits
    val lastEpoch = EpochStore.Epoch(lastEpochInfo, lastBlockCommits)
    val newEpochState =
      EpochState.Epoch(lastEpochInfo, activeMembership, DefaultLeaderSelectionPolicy)
    StateTransferMessageResult.BlockTransferCompleted(newEpochState, lastEpoch)
  }

  private def sendBlockToOutput(prePrepare: PrePrepare, endEpoch: EpochNumber): Unit = {
    val blockMetadata = prePrepare.blockMetadata
    // TODO(#19661): don't assume a fixed epoch length
    val isLastInEpoch =
      (blockMetadata.blockNumber + 1) % epochLength == 0 // As blocks are 0-indexed
    val isLastStateTransferred =
      blockMetadata.blockNumber == (endEpoch * epochLength) + epochLength - 1

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
          mode =
            if (isLastStateTransferred) OrderedBlockForOutput.Mode.StateTransfer.LastBlock
            else OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock,
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
}

object StateTransferManager {

  private val RetryTimeout = 10.seconds

  private def calculateEndEpoch(
      quorumOfBlockTransferResponses: Seq[StateTransferMessage.BlockTransferResponse]
  )(abort: String => Nothing) =
    quorumOfBlockTransferResponses.view
      .map(response => response.latestCompletedEpoch)
      // TODO(#19661): Figure out what to do with faulty nodes that provide correct responses
      //  with a low latest completed epoch.
      .minOption
      .getOrElse(
        abort(
          "Internal invariant violation: state transfer should have quorum of block transfer responses at this point"
        )
      )

  private def getFirstBlockInLastEpoch(prePrepares: Seq[PrePrepare])(abort: String => Nothing) = {
    def fail = abort(
      s"There should be at least one state-transferred block, but there was ${prePrepares.size}"
    )
    val lastEpoch = prePrepares.view
      .map(_.blockMetadata.epochNumber)
      .maxOption
      .getOrElse(fail)
    prePrepares.view
      .filter(_.blockMetadata.epochNumber == lastEpoch)
      .map(_.blockMetadata.blockNumber)
      .minOption
      .getOrElse(fail)
  }

  private def wrapSignedMessage(
      signedMessage: SignedMessage[StateTransferMessage.StateTransferNetworkMessage]
  ): P2PNetworkOut.BftOrderingNetworkMessage =
    P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(signedMessage)
}

sealed trait StateTransferMessageResult

object StateTransferMessageResult {

  case object NothingToStateTransfer extends StateTransferMessageResult

  // Usually an inconclusive result
  case object Continue extends StateTransferMessageResult

  final case class BlockTransferCompleted(newEpochState: EpochState.Epoch, epoch: EpochStore.Epoch)
      extends StateTransferMessageResult
}
