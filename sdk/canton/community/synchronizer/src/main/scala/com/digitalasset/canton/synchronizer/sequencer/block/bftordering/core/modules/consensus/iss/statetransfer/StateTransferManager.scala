// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
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
import com.digitalasset.canton.version.ProtocolVersion

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
    random: Random,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(
    private val maybeCustomTimeoutManager: Option[TimeoutManager[E, Consensus.Message[E], String]] =
      None
)(implicit
    synchronizerProtocolVersion: ProtocolVersion,
    config: BftBlockOrdererConfig,
    mc: MetricsContext,
) extends NamedLogging {

  private val stateTransferStartEpoch = new SingleUseCell[EpochNumber]

  private val validator = new StateTransferMessageValidator[E](metrics, loggerFactory)

  private val messageSender = new StateTransferMessageSender[E](
    thisNode,
    dependencies,
    epochLength,
    epochStore,
    loggerFactory,
  )

  private val timeoutManager = maybeCustomTimeoutManager.getOrElse(
    new TimeoutManager[E, Consensus.Message[E], String](
      loggerFactory,
      config.epochStateTransferRetryTimeout,
      timeoutId = "state transfer",
    )
  )

  private val nodeShuffler = new BftNodeShuffler(random)

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
      initStateTransfer(startEpoch)(abort)

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
      initStateTransfer(newEpochNumber)(abort)
    }
    val blockTransferRequest =
      StateTransferMessage.BlockTransferRequest.create(newEpochNumber, membership.myId)
    messageSender.signMessage(cryptoProvider, blockTransferRequest) { signedMessage =>
      sendBlockTransferRequest(signedMessage, membership)(abort)
    }
  }

  private def initStateTransfer(startEpoch: EpochNumber)(abort: String => Nothing): Unit =
    stateTransferStartEpoch
      .putIfAbsent(startEpoch)
      .foreach(_ =>
        abort(
          "Internal inconsistency: state transfer manager in a client role should not be reused"
        )
      )

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
            from,
          ) =>
        storeBlock(commitCert, from)

      case StateTransferMessage.BlockStored(commitCert, from) =>
        if (inStateTransfer) {
          handleStoredBlock(commitCert)
        } else {
          logger.info(
            s"State transfer: stored block ${commitCert.prePrepare.message.blockMetadata} from '$from' " +
              s"while not in state transfer"
          )
        }
        StateTransferMessageResult.Continue
    }

  def cancelTimeoutForEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.info(s"State transfer: cancelling a timeout for epoch $epochNumber")
    timeoutManager.cancelTimeout()
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
            { validationError =>
              logger.warn(s"State transfer: $validationError, dropping...")
              emitNonCompliance(metrics)(
                from,
                Some(epoch),
                view = None,
                block = None,
                metrics.security.noncompliant.labels.violationType.values.StateTransferInvalidMessage,
              )
            },
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
        // TODO(#25082) consider authorizing/handling a response only if it comes `from` the requested node
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
      //  multiple times in a row, resulting in uneven load balancing for certain periods. On the other hand, shuffling
      //  is more straightforward code-wise. A potentially irrelevant implication is that the order in which nodes
      //  are chosen is less predictable (e.g., by malicious nodes), and, at the same time, harder to reason about.
      // TODO(#24940) consider not rotating when everything runs smoothly
      val servingNode =
        nodeShuffler
          .shuffle(membership.otherNodes.toSeq)
          .headOption
          .getOrElse(
            abort(
              "Internal inconsistency: there should be at least one serving node to send a block transfer request to"
            )
          )
      timeoutManager
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
          val blockMetadata = response.commitCertificate.map(_.prePrepare.message.blockMetadata)
          emitNonCompliance(metrics)(
            response.from,
            blockMetadata.map(_.epochNumber),
            view = None,
            blockMetadata.map(_.blockNumber),
            metrics.security.noncompliant.labels.violationType.values.StateTransferInvalidMessage,
          )
          StateTransferMessageResult.Continue
        },
        { _ =>
          val from = response.from
          response.commitCertificate match {
            case None =>
              StateTransferMessageResult.NothingToStateTransfer(from)
            case Some(commitCert) =>
              validator.verifyCommitCertificate(commitCert, from, orderingTopologyInfo)
              StateTransferMessageResult.Continue
          }
        },
      )

  private def storeBlock(
      commitCert: CommitCertificate,
      from: BftNodeId,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult = {
    context.pipeToSelf(epochStore.addOrderedBlock(commitCert.prePrepare, commitCert.commits)) {
      case Success(_) =>
        Some(StateTransferMessage.BlockStored(commitCert, from))
      case Failure(exception) =>
        Some(Consensus.ConsensusMessage.AsyncException(exception))
    }
    StateTransferMessageResult.Continue
  }

  private def handleStoredBlock(
      commitCert: CommitCertificate
  )(implicit traceContext: TraceContext): Unit = {
    val prePrepare = commitCert.prePrepare.message
    val blockMetadata = prePrepare.blockMetadata
    // TODO(#19289) support variable epoch lengths
    val blockLastInEpoch = (blockMetadata.blockNumber + 1) % epochLength == 0

    // Blocks within an epoch can be received and stored out of order, but that's fine because the Output module
    //  orders them (has a Peano queue).
    logger.debug(s"State transfer: sending block $blockMetadata to Output")
    messageSender.sendBlockToOutput(prePrepare, blockLastInEpoch)
  }
}

sealed trait StateTransferMessageResult extends Product with Serializable

object StateTransferMessageResult {

  // Signals that state transfer is in progress
  case object Continue extends StateTransferMessageResult

  final case class NothingToStateTransfer(from: BftNodeId) extends StateTransferMessageResult
}
