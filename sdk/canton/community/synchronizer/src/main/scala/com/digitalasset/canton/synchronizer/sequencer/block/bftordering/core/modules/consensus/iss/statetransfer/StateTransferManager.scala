// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.TimeoutManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.utils.BftNodeShuffler
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell
import com.google.common.annotations.VisibleForTesting

import scala.util.{Failure, Random, Success}

/** Manages a single state transfer instance in a client role and multiple state transfer instances
  * in a server role.
  *
  * It is meant to be used by Consensus behaviors only and is not thread-safe.
  *
  * Design document: https://docs.google.com/document/d/1oB1KtnpM7OiNDWQoRUL0NuoEFJYUjg58ECYIjSi4sIM
  */
class StateTransferManager[E <: Env[E]](
    thisNode: BftNodeId,
    dependencies: ConsensusModuleDependencies[E],
    epochLength: EpochLength, // TODO(#19289) support variable epoch lengths
    epochStore: EpochStore[E],
    override val loggerFactory: NamedLoggerFactory,
)(implicit config: BftBlockOrdererConfig)
    extends NamedLogging {

  private val stateTransferStartEpoch = new SingleUseCell[EpochNumber]

  private val validator = new StateTransferMessageValidator[E](loggerFactory)

  private val messageSender = new StateTransferMessageSender[E](
    thisNode,
    dependencies,
    epochLength,
    epochStore,
    loggerFactory,
  )

  @VisibleForTesting
  private[bftordering] val maybeBlockTransferResponseTimeoutManager =
    new SingleUseCell[TimeoutManager[E, Consensus.Message[E], EpochNumber]]
  private def blockTransferResponseTimeoutManager(abort: String => Nothing) =
    maybeBlockTransferResponseTimeoutManager.getOrElse(
      abort(
        "Internal inconsistency: there should be a block transfer response timeout manager available"
      )
    )

  private val maybeNodeShuffler = new SingleUseCell[BftNodeShuffler]
  private def nodeShuffler(abort: String => Nothing) =
    maybeNodeShuffler.getOrElse(
      abort("Internal inconsistency: there should be a node shuffler available")
    )

  def inStateTransfer: Boolean = stateTransferStartEpoch.isDefined

  def startCatchUp(
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
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
        s"State transfer: starting catch-up from epoch $startEpoch, latest completed epoch is $latestCompletedEpochNumber"
      )
      initStateTransfer(startEpoch, abort)

      val blockTransferRequest =
        StateTransferMessage.BlockTransferRequest.create(startEpoch, membership.myId)
      messageSender.signMessage(cryptoProvider, blockTransferRequest) { signedMessage =>
        sendBlockTransferRequest(signedMessage, membership)(abort)
      }
    }

  def stateTransferNewEpoch(
      newEpochNumber: EpochNumber,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
  )(
      abort: String => Nothing
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    if (inStateTransfer) {
      logger.info(s"State transfer: requesting new epoch $newEpochNumber")
    } else {
      logger.info(s"State transfer: starting onboarding state transfer from epoch $newEpochNumber")
      initStateTransfer(newEpochNumber, abort)
    }
    val blockTransferRequest =
      StateTransferMessage.BlockTransferRequest.create(newEpochNumber, membership.myId)
    messageSender.signMessage(cryptoProvider, blockTransferRequest) { signedMessage =>
      sendBlockTransferRequest(signedMessage, membership)(abort)
    }
  }

  private def initStateTransfer(startEpoch: EpochNumber, abort: String => Nothing): Unit = {
    stateTransferStartEpoch
      .putIfAbsent(startEpoch)
      .foreach(_ =>
        abort(
          "Internal inconsistency: state transfer manager in a client role should not be reused"
        )
      )
    maybeBlockTransferResponseTimeoutManager
      .putIfAbsent(
        new TimeoutManager[E, Consensus.Message[E], EpochNumber](
          loggerFactory,
          config.epochStateTransferRetryTimeout,
          timeoutId = startEpoch, // reuse the start epoch number, not relevant
        )
      )
      .foreach(_ =>
        abort(
          "Internal inconsistency: block transfer response timeout manager should be set only once"
        )
      )
    // Derive the random seed from the epoch number so that it's deterministic.
    // Note that if multiple nodes are onboarded from the same epoch or come back online after a network partition,
    //  they might use the same serving nodes around the same time.
    maybeNodeShuffler
      .putIfAbsent(new BftNodeShuffler(new Random(startEpoch)))
      .foreach(_ => abort("Internal inconsistency: node shuffler should be set only once"))
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
        validator.verifyStateTransferMessage(
          unverifiedMessage,
          topologyInfo.currentMembership,
          topologyInfo.currentCryptoProvider,
        )
        StateTransferMessageResult.Continue

      case StateTransferMessage.VerifiedStateTransferMessage(message) =>
        handleStateTransferNetworkMessage(message, topologyInfo, latestCompletedEpoch)(abort)

      case StateTransferMessage.RetryBlockTransferRequest(request) =>
        sendBlockTransferRequest(request, topologyInfo.currentMembership)(abort)
        StateTransferMessageResult.Continue

      case StateTransferMessage.BlockVerified(
            commitCert,
            remoteLatestCompleteEpoch,
            from,
          ) =>
        storeBlock(commitCert, remoteLatestCompleteEpoch, from)

      case StateTransferMessage.BlockStored(commitCert, remoteLatestCompleteEpoch, from) =>
        if (inStateTransfer) {
          handleStoredBlock(commitCert, remoteLatestCompleteEpoch)
        } else {
          logger.info(
            s"State transfer: stored block ${commitCert.prePrepare.message.blockMetadata} from '$from' " +
              s"while not in state transfer"
          )
        }
        StateTransferMessageResult.Continue
    }

  def epochTransferred(epochNumber: EpochNumber)(abort: String => Nothing)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.info(
      s"State transfer: fully transferred epoch $epochNumber (including batches), cancelling the timeout"
    )
    // Currently, epoch completion in state transfer is entirely Output-based, i.e., all batches from an epoch
    //  need to be fetched to cancel the timeout. If it's cancelled too soon, while "only" batches are left to fetch,
    //  the state-transferring node will ask another node for blocks and rely on the idempotency of the stores.
    blockTransferResponseTimeoutManager(abort).cancelTimeout()
  }

  private def handleStateTransferNetworkMessage(
      message: Consensus.StateTransferMessage.StateTransferNetworkMessage,
      orderingTopologyInfo: OrderingTopologyInfo[E],
      latestCompletedEpoch: EpochStore.Epoch,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
    message match {
      case request @ StateTransferMessage.BlockTransferRequest(epoch, from) =>
        validator
          .validateBlockTransferRequest(request, orderingTopologyInfo.currentMembership)
          .fold(
            // TODO(#23313) emit metrics
            validationError => logger.warn(s"State transfer: $validationError, dropping..."),
            { _ =>
              logger.info(s"State transfer: '$from' is requesting block transfer for epoch $epoch")

              messageSender.sendBlockTransferResponses(
                orderingTopologyInfo.currentCryptoProvider,
                to = from,
                request.epoch,
                latestCompletedEpoch,
              )(abort)
            },
          )
        StateTransferMessageResult.Continue

      case response: StateTransferMessage.BlockTransferResponse =>
        if (inStateTransfer) {
          handleBlockTransferResponse(
            response,
            latestCompletedEpoch.info.number,
            orderingTopologyInfo,
          )
        } else {
          val blockMetadata = response.commitCertificate.map(_.prePrepare.message.blockMetadata)
          logger.info(
            s"State transfer: received a block transfer response for block $blockMetadata " +
              s"from ${response.from} while not in state transfer; ignoring unneeded response"
          )
          StateTransferMessageResult.Continue
        }
    }

  private def sendBlockTransferRequest(
      request: SignedMessage[StateTransferMessage.BlockTransferRequest],
      membership: Membership,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (inStateTransfer) {
      // Ask a single node for an entire epoch of blocks to compromise between noise for different nodes
      //  and load balancing. Note that we're shuffling (instead of round-robin), so the same node might be chosen
      //  multiple times in a row, resulting in uneven load balancing for certain periods. On the other hand,
      //  the order in which nodes are chosen is less predictable.
      val servingNode =
        nodeShuffler(abort)
          .shuffle(membership.otherNodes.toSeq)
          .headOption
          .getOrElse(
            abort(
              "Internal inconsistency: there should be at least one serving node to send a block transfer request to"
            )
          )
      blockTransferResponseTimeoutManager(abort)
        .scheduleTimeout(StateTransferMessage.RetryBlockTransferRequest(request))
      messageSender.sendBlockTransferRequest(request, servingNode)
    } else {
      logger.info(
        s"State transfer: not sending a block transfer request when not in state transfer (likely a race)"
      )
    }

  private def handleBlockTransferResponse(
      response: StateTransferMessage.BlockTransferResponse,
      latestLocallyCompletedEpoch: EpochNumber,
      orderingTopologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
    validator
      .validateBlockTransferResponse(
        response,
        latestLocallyCompletedEpoch,
        orderingTopologyInfo.currentMembership,
      )
      .fold(
        { validationError =>
          logger.warn(s"State transfer: $validationError, dropping...")
          // TODO(#23313) emit metrics
          StateTransferMessageResult.Continue
        },
        { _ =>
          val from = response.from
          response.commitCertificate match {
            case None => StateTransferMessageResult.NothingToStateTransfer(from)
            case Some(commitCert) =>
              validator.verifyCommitCertificate(
                commitCert,
                from,
                response.latestCompletedEpoch,
                orderingTopologyInfo,
              )
              StateTransferMessageResult.Continue
          }
        },
      )

  private def storeBlock(
      commitCert: CommitCertificate,
      remoteLatestCompleteEpoch: EpochNumber,
      from: BftNodeId,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult = {
    context.pipeToSelf(epochStore.addOrderedBlock(commitCert.prePrepare, commitCert.commits)) {
      case Success(_) =>
        Some(StateTransferMessage.BlockStored(commitCert, remoteLatestCompleteEpoch, from))
      case Failure(exception) =>
        Some(Consensus.ConsensusMessage.AsyncException(exception))
    }
    StateTransferMessageResult.Continue
  }

  private def handleStoredBlock(
      commitCert: CommitCertificate,
      remoteLatestCompleteEpoch: EpochNumber,
  )(implicit traceContext: TraceContext): Unit = {
    val prePrepare = commitCert.prePrepare.message
    val blockMetadata = prePrepare.blockMetadata
    // TODO(#19289) support variable epoch lengths
    val blockLastInEpoch = (blockMetadata.blockNumber + 1) % epochLength == 0
    // TODO(#24717) calculate reasonably
    // Right now, the state transfer end epoch is the latest remotely completed epoch according to the last received
    //  block transfer response, so slow/malicious nodes can significantly impact the process.
    val endEpoch = remoteLatestCompleteEpoch

    // Blocks within an epoch can be received and stored out of order, but that's fine because the Output module
    //  orders them (has a Peano queue).
    logger.debug(s"State transfer: sending block $blockMetadata to Output")
    messageSender.sendBlockToOutput(prePrepare, blockLastInEpoch, endEpoch)
  }
}

sealed trait StateTransferMessageResult extends Product with Serializable

object StateTransferMessageResult {

  // Signals that state transfer is in progress
  case object Continue extends StateTransferMessageResult

  final case class NothingToStateTransfer(from: BftNodeId) extends StateTransferMessageResult
}
