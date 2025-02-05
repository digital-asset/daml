// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultLeaderSelectionPolicy
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.{
  EpochState,
  TimeoutManager,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/** Manages a single state transfer instance in a client role and multiple state transfer instances in a server role.
  *
  * It is meant to be used by Consensus behaviors only and is not thread-safe.
  *
  * Design document: https://docs.google.com/document/d/1oB1KtnpM7OiNDWQoRUL0NuoEFJYUjg58ECYIjSi4sIM
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class StateTransferManager[E <: Env[E]](
    dependencies: ConsensusModuleDependencies[E],
    epochLength: EpochLength, // TODO(#19289) support variable epoch lengths
    epochStore: EpochStore[E],
    thisPeer: SequencerId,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import StateTransferManager.*

  private var stateTransferStartEpoch: Option[EpochNumber] = None

  private var latestCompletedEpochAtStart: Option[EpochNumber] = None

  private var quorumOfMatchingBlockTransferResponses
      : Option[Set[StateTransferMessage.BlockTransferResponse]] = None

  private var blockTransferResponseQuorumBuilder: Option[BlockTransferResponseQuorumBuilder] = None

  private val messageSender = new StateTransferMessageSender[E](
    dependencies,
    epochLength,
    epochStore,
    thisPeer,
    loggerFactory,
  )

  private val blockTransferResponseTimeouts =
    mutable.Map[SequencerId, TimeoutManager[E, Consensus.Message[E], SequencerId]]()

  def inStateTransfer: Boolean = stateTransferStartEpoch.isDefined

  def startStateTransfer(
      activeMembership: Membership,
      activeCryptoProvider: CryptoProvider[E],
      latestCompletedEpoch: EpochStore.Epoch,
      startEpoch: EpochNumber,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (inStateTransfer) {
      logger.debug("State transfer: already in progress")
    } else {
      val latestCompletedEpochNumber = latestCompletedEpoch.info.number
      logger.info(
        s"State transfer: requesting, start epoch is $startEpoch, latest completed epoch is $latestCompletedEpochNumber"
      )
      stateTransferStartEpoch = Some(startEpoch)
      latestCompletedEpochAtStart = Some(latestCompletedEpochNumber)
      blockTransferResponseQuorumBuilder = Some(
        new BlockTransferResponseQuorumBuilder(activeMembership)
      )

      val blockTransferRequest =
        StateTransferMessage.BlockTransferRequest.create(
          startEpoch,
          latestCompletedEpochNumber,
          activeMembership.myId,
        )
      messageSender.signMessage(activeCryptoProvider, blockTransferRequest) { signedMessage =>
        activeMembership.otherPeers.foreach { peerId =>
          blockTransferResponseTimeouts
            .put(peerId, new TimeoutManager(loggerFactory, RetryTimeout, peerId))
            .foreach(_ => abort(s"There should be no timeout manager for peer $peerId yet"))
          sendBlockTransferRequest(signedMessage, to = peerId)(abort)
        }
      }
    }

  def handleStateTransferMessage(
      message: Consensus.StateTransferMessage,
      topologyInfo: OrderingTopologyInfo[E],
      latestCompletedEpoch: EpochStore.Epoch,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
    message match {
      case StateTransferMessage.UnverifiedStateTransferMessage(unverifiedMessage) =>
        StateTransferMessageValidator.verifyStateTransferMessage(
          unverifiedMessage,
          topologyInfo.currentMembership,
          topologyInfo.currentCryptoProvider,
          loggerFactory,
        )
        StateTransferMessageResult.Continue

      case StateTransferMessage.VerifiedStateTransferMessage(message) =>
        handleStateTransferNetworkMessage(
          message,
          topologyInfo.currentMembership,
          topologyInfo.currentCryptoProvider,
          latestCompletedEpoch,
        )(abort)

      case StateTransferMessage.ResendBlockTransferRequest(blockTransferRequest, to) =>
        sendBlockTransferRequest(blockTransferRequest, to)(abort)
        StateTransferMessageResult.Continue

      case StateTransferMessage.BlocksStored(prePrepares, endEpoch) =>
        if (inStateTransfer) {
          handleStoredBlocks(
            prePrepares,
            endEpoch,
            topologyInfo.currentMembership,
            topologyInfo.previousMembership,
          )(abort)
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
      activeCryptoProvider: CryptoProvider[E],
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
        StateTransferMessageValidator
          .validateBlockTransferRequest(request, activeMembership)
          .fold(
            validationMessage => logger.info(s"State transfer: $validationMessage, dropping..."),
            { _ =>
              if (isOnboarding(startEpoch, remoteLatestCompletedEpoch))
                logger
                  .info(s"State transfer: peer $from is starting onboarding from epoch $startEpoch")
              else
                logger
                  .info(s"State transfer: peer $from is catching up from epoch $startEpoch")

              messageSender.sendBlockTransferResponse(
                activeCryptoProvider,
                to = from,
                request.startEpoch,
                latestCompletedEpoch,
              )(abort)
            },
          )
        StateTransferMessageResult.Continue

      case response: StateTransferMessage.BlockTransferResponse =>
        if (inStateTransfer) {
          cancelTimeoutForPeer(response.from)(abort)
          handleBlockTransferResponse(response)(abort)
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
      messageSender.sendBlockTransferRequest(blockTransferRequest, to)
    } else {
      logger.info(
        s"State transfer: not sending a block transfer request to $to when not in state transfer (likely a race)"
      )
    }

  private def handleBlockTransferResponse(
      response: StateTransferMessage.BlockTransferResponse
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
    quorumOfMatchingBlockTransferResponses match {
      case Some(_) =>
        logger.debug(
          "State transfer: already reached quorum of matching block transfer responses; dropping an additional copy " +
            s"from peer: ${response.from}"
        )
        StateTransferMessageResult.Continue
      case None =>
        val responseQuorumBuilder = blockTransferResponseQuorumBuilder
          .getOrElse(
            abort("Internal invariant violation: no block transfer response quorum builder")
          )
        responseQuorumBuilder.addResponse(response)
        quorumOfMatchingBlockTransferResponses = responseQuorumBuilder.build
        handlePotentialQuorumOfBlockTransferResponses(abort)
    }

  private def handlePotentialQuorumOfBlockTransferResponses(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
    quorumOfMatchingBlockTransferResponses
      .map { quorumOfResponses =>
        val endEpoch = calculateEndEpoch(abort)
        val prePreparesUpToEndEpoch = quorumOfResponses
          // Responses are supposed to match, so we can just take the first one.
          .headOption.toList
          .flatMap(_.prePrepares)
          .takeWhile(_.message.blockMetadata.epochNumber <= endEpoch)

        if (prePreparesUpToEndEpoch.isEmpty) {
          if (latestCompletedEpochAtStart.exists(_ < endEpoch)) {
            abort(
              "Internal invariant violation: pre-prepare set is empty while the state transfer end epoch " +
                s"$endEpoch is greater than the latest locally completed epoch $latestCompletedEpochAtStart"
            )
          }
          // This can only happen if at least a quorum of nodes is lagging behind (or the number of tolerable faulty
          //  nodes is exceeded). The receiving node will then likely realize that it is behind and start the catch-up
          //  process again.
          // When onboarding, serving nodes figure out that a new node is onboarded once the "start epoch" is completed.
          //  In that case, there is always at least 1 epoch to transfer.
          logger.info("State transfer: nothing to be transferred to us")
          StateTransferMessageResult.NothingToStateTransfer
        } else {
          storeBlocks(prePreparesUpToEndEpoch, endEpoch)
          StateTransferMessageResult.Continue
        }
      }
      .getOrElse(StateTransferMessageResult.Continue)

  private def calculateEndEpoch(abort: String => Nothing) =
    quorumOfMatchingBlockTransferResponses.toList.view.flatten
      .map(response => response.latestCompletedEpoch)
      // TODO(#23143) Figure out what to do with faulty/slow nodes that provide correct responses
      //  with a low latest completed epoch.
      .minOption
      .getOrElse(
        abort("Cannot calculate end epoch before gathering a quorum of block transfer responses")
      )

  private def storeBlocks(
      blockPrePrepares: Seq[SignedMessage[PrePrepare]],
      endEpoch: EpochNumber,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val futures = blockPrePrepares.map(prePrepare =>
      epochStore.addOrderedBlock(prePrepare, commitMessages = Seq.empty)
    )
    val sequencedFuture = context.sequenceFuture(futures)
    context.pipeToSelf(sequencedFuture) {
      case Success(_) =>
        Some(StateTransferMessage.BlocksStored(blockPrePrepares.map(_.message), endEpoch))
      case Failure(exception) =>
        Some(Consensus.ConsensusMessage.AsyncException(exception))
    }
  }

  private def handleStoredBlocks(
      prePrepares: Seq[PrePrepare],
      endEpoch: EpochNumber,
      activeMembership: Membership,
      previousMembership: Membership,
  )(abort: String => Nothing)(implicit traceContext: TraceContext): StateTransferMessageResult = {
    val startEpoch = stateTransferStartEpoch.getOrElse(abort("Should be in state transfer"))
    val numberOfTransferredEpochs = endEpoch - startEpoch + 1

    logger.info(
      s"State transfer: finishing block transfer at epoch $endEpoch with $numberOfTransferredEpochs epochs " +
        s"(${prePrepares.size} blocks) transferred"
    )

    prePrepares.foreach { prePrepare =>
      logger.debug(s"State transfer: sending block ${prePrepare.blockMetadata} to Output")
      messageSender.sendBlockToOutput(prePrepare, endEpoch)
    }

    val firstBlockInLastEpoch = getFirstBlockInLastEpoch(prePrepares)(abort)
    val lastEpochInfo = EpochInfo(
      // Note that the Output module will send an epoch number greater by 1 once the batches are transferred.
      endEpoch,
      firstBlockInLastEpoch,
      epochLength,
      // TODO(#23143) store epochs and set the activation time to a correct recent value to prevent getting stuck on restarts
      // Sets the topology activation time (used for querying the topology on a restart) for the last state-transferred
      //  epoch to to the one from the beginning of the state transfer.
      //
      // It is fine for onboarding given that multiple concurrent topology changes are unsupported.
      //
      // For catch-up, a temporary assumption is that if the topology changes too much, the node might get stuck
      //  and require re-onboarding. The chances of running into such a problem for the last state-transferred epoch
      //  should however be small, because the time between receiving the last state-transferred epoch and joining
      //  consensus (after the Output module finishes processing all state-transferred blocks) should be small.
      //
      // However, the value below is currently unused because we don't store state-transferred epochs at all.
      //  This means that, on restart, a node will start catching up from scratch and will rely on
      //  the idempotency of the stores. Previously stored ordered blocks and especially batches should make
      //  the process faster, as batches that were already stored will allow the node to skip fetching them.
      activeMembership.orderingTopology.activationTime,
    )
    // Providing an empty commit message set, which results in using the BFT time monotonicity adjustment for the first
    //  block produced by the state-transferred node as a leader.
    val lastStoredEpoch = EpochStore.Epoch(lastEpochInfo, lastBlockCommits = Seq.empty)
    val newEpoch = EpochState.Epoch(
      lastEpochInfo,
      activeMembership,
      previousMembership,
      DefaultLeaderSelectionPolicy,
    )
    StateTransferMessageResult.BlockTransferCompleted(newEpoch, lastStoredEpoch)
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

  private def isOnboarding(startEpoch: EpochNumber, latestCompletedEpoch: EpochNumber) =
    latestCompletedEpoch == GenesisEpochNumber && startEpoch > GenesisEpochNumber

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
}

sealed trait StateTransferMessageResult extends Product with Serializable

object StateTransferMessageResult {

  case object NothingToStateTransfer extends StateTransferMessageResult

  // Signals that state transfer is still in progress
  case object Continue extends StateTransferMessageResult

  final case class BlockTransferCompleted(
      lastCompletedEpoch: EpochState.Epoch,
      lastCompletedEpochStored: EpochStore.Epoch,
  ) extends StateTransferMessageResult
}
