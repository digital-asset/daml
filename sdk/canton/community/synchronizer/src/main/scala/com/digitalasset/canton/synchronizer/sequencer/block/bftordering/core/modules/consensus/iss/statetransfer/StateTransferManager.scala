// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.TimeoutManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
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

/** Manages a single state transfer instance in a client role and multiple state transfer instances
  * in a server role.
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

  def inBlockTransfer: Boolean = stateTransferStartEpoch.isDefined

  def startStateTransfer(
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
      latestCompletedEpoch: EpochStore.Epoch,
      startEpoch: EpochNumber,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (inBlockTransfer) {
      logger.debug("State transfer: already in progress")
    } else {
      val latestCompletedEpochNumber = latestCompletedEpoch.info.number
      logger.info(
        s"State transfer: requesting, start epoch is $startEpoch, latest completed epoch is $latestCompletedEpochNumber"
      )
      stateTransferStartEpoch = Some(startEpoch)
      latestCompletedEpochAtStart = Some(latestCompletedEpochNumber)
      blockTransferResponseQuorumBuilder = Some(new BlockTransferResponseQuorumBuilder(membership))

      val blockTransferRequest =
        StateTransferMessage.BlockTransferRequest.create(
          startEpoch,
          latestCompletedEpochNumber,
          membership.myId,
        )
      messageSender.signMessage(cryptoProvider, blockTransferRequest) { signedMessage =>
        membership.otherPeers.foreach { peerId =>
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
        if (inBlockTransfer) {
          handleStoredBlocks(prePrepares, endEpoch)(abort)
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
            _, // TODO(#23618) remove this field (latestCompletedEpoch)
            from,
          ) =>
        StateTransferMessageValidator
          .validateBlockTransferRequest(request, activeMembership)
          .fold(
            validationMessage => logger.info(s"State transfer: $validationMessage, dropping..."),
            { _ =>
              logger.info(s"State transfer: peer $from is starting transfer from epoch $startEpoch")

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
        if (inBlockTransfer) {
          // Cancel a timeout if exists
          blockTransferResponseTimeouts.remove(response.from).foreach(_.cancelTimeout())
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
    if (inBlockTransfer) {
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
        // Clean up remaining timeouts as we're done with this part of block transfer.
        blockTransferResponseTimeouts.view.values.foreach(_.cancelTimeout())
        blockTransferResponseTimeouts.clear()

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
  )(abort: String => Nothing)(implicit traceContext: TraceContext): StateTransferMessageResult = {
    val startEpoch = stateTransferStartEpoch.getOrElse(abort("Should be in state transfer"))
    val numberOfTransferredEpochs = endEpoch - startEpoch + 1

    prePrepares.foreach { prePrepare =>
      logger.debug(s"State transfer: sending block ${prePrepare.blockMetadata} to Output")
      messageSender.sendBlockToOutput(prePrepare, endEpoch)
    }

    StateTransferMessageResult.BlockTransferCompleted(
      endEpoch,
      numberOfTransferredEpochs,
      prePrepares.size.toLong,
    )
  }
}

object StateTransferManager {
  private val RetryTimeout = 10.seconds
}

sealed trait StateTransferMessageResult extends Product with Serializable

object StateTransferMessageResult {

  case object NothingToStateTransfer extends StateTransferMessageResult

  // Signals that state transfer is in progress
  case object Continue extends StateTransferMessageResult

  final case class BlockTransferCompleted(
      endEpochNumber: EpochNumber,
      numberOfTransferredEpochs: Long,
      numberOfTransferredBlocks: Long,
  ) extends StateTransferMessageResult
}
