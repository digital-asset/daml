// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.TimeoutManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.IssConsensusSignatureVerifier
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
    thisNode: BftNodeId,
    dependencies: ConsensusModuleDependencies[E],
    epochLength: EpochLength, // TODO(#19289) support variable epoch lengths
    epochStore: EpochStore[E],
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import StateTransferManager.*

  private var stateTransferStartEpoch: Option[EpochNumber] = None

  private val signatureVerifier = new IssConsensusSignatureVerifier[E]()

  private val messageSender = new StateTransferMessageSender[E](
    thisNode,
    dependencies,
    epochLength,
    epochStore,
    loggerFactory,
  )

  private val blockTransferResponseTimeouts =
    mutable.Map[BftNodeId, TimeoutManager[E, Consensus.Message[E], BftNodeId]]()

  def inBlockTransfer: Boolean = stateTransferStartEpoch.isDefined

  def startCatchUp(
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
        s"State transfer: starting catch-up from epoch $startEpoch, latest completed epoch is $latestCompletedEpochNumber"
      )
      stateTransferStartEpoch = Some(startEpoch)

      val blockTransferRequest =
        StateTransferMessage.BlockTransferRequest.create(startEpoch, membership.myId)
      messageSender.signMessage(cryptoProvider, blockTransferRequest) { signedMessage =>
        membership.otherNodes.headOption.foreach { node => // TODO(#24524) rotate nodes
          blockTransferResponseTimeouts
            .put(node, new TimeoutManager(loggerFactory, RetryTimeout, node))
            .foreach(_ => abort(s"There should be no timeout manager for '$node' yet"))
          sendBlockTransferRequest(signedMessage, to = node)(abort)
        }
      }
    }

  def stateTransferNewEpoch(
      newEpochNumber: EpochNumber,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
  )(
      abort: String => Nothing
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    if (stateTransferStartEpoch.isEmpty) {
      logger.info(s"State transfer: starting onboarding state transfer from epoch $newEpochNumber")
      stateTransferStartEpoch = Some(newEpochNumber)
    } else {
      logger.info(s"State transfer: requesting new epoch $newEpochNumber")
    }
    val blockTransferRequest =
      StateTransferMessage.BlockTransferRequest.create(newEpochNumber, membership.myId)
    messageSender.signMessage(cryptoProvider, blockTransferRequest) { signedMessage =>
      membership.otherNodes.headOption.foreach { node => // TODO(#24524) rotate nodes
        blockTransferResponseTimeouts
          .put(node, new TimeoutManager(loggerFactory, RetryTimeout, node))
          .discard
        sendBlockTransferRequest(signedMessage, to = node)(abort)
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
        handleStateTransferNetworkMessage(message, topologyInfo, latestCompletedEpoch)(abort)

      case StateTransferMessage.ResendBlockTransferRequest(blockTransferRequest, to) =>
        sendBlockTransferRequest(blockTransferRequest, to)(abort) // TODO(#24524) rotate nodes
        StateTransferMessageResult.Continue

      case StateTransferMessage.BlockVerified(
            commitCert,
            remoteLatestCompleteEpoch,
            from,
          ) =>
        storeBlock(commitCert, remoteLatestCompleteEpoch, from)

      case StateTransferMessage.BlockStored(commitCert, remoteLatestCompleteEpoch, from) =>
        if (inBlockTransfer) {
          handleStoredBlock(commitCert, remoteLatestCompleteEpoch, from)(abort)
        } else {
          logger.info(
            s"State transfer: stored block ${commitCert.prePrepare.message.blockMetadata} while not in state transfer"
          )
          StateTransferMessageResult.Continue
        }
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
        StateTransferMessageValidator
          .validateBlockTransferRequest(request, orderingTopologyInfo.currentMembership)
          .fold(
            validationMessage => logger.info(s"State transfer: $validationMessage, dropping..."),
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
        if (inBlockTransfer) {
          handleBlockTransferResponse(response, orderingTopologyInfo)
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
      blockTransferRequest: SignedMessage[StateTransferMessage.BlockTransferRequest],
      to: BftNodeId,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (inBlockTransfer) {
      blockTransferResponseTimeouts
        .getOrElse(to, abort(s"No timeout manager for '$to'"))
        .scheduleTimeout(
          StateTransferMessage.ResendBlockTransferRequest(blockTransferRequest, to)
        )
      messageSender.sendBlockTransferRequest(blockTransferRequest, to)
    } else {
      logger.info(
        s"State transfer: not sending a block transfer request to '$to' when not in state transfer (likely a race)"
      )
    }

  private def handleBlockTransferResponse(
      response: StateTransferMessage.BlockTransferResponse,
      orderingTopologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
    response.commitCertificate.fold[StateTransferMessageResult](
      StateTransferMessageResult.NothingToStateTransfer
    ) { commitCert =>
      // TODO(#24524) validate response
      val from = response.from
      context.pipeToSelf(
        signatureVerifier.validateConsensusCertificate(commitCert, orderingTopologyInfo)
      ) {
        case Success(Right(())) =>
          Some(StateTransferMessage.BlockVerified(commitCert, response.latestCompletedEpoch, from))
        case Success(Left(errors)) =>
          // TODO(#23313) emit metrics
          logger.warn(
            s"State transfer: commit certificate from '$from' failed signature verification, dropping: $errors"
          )
          None
        case Failure(exception) =>
          // TODO(#23313) emit metrics
          logger.warn(
            s"State transfer: commit certificate from '$from' could not be verified, dropping",
            exception,
          )
          None
      }
      StateTransferMessageResult.Continue
    }

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
      from: BftNodeId,
  )(abort: String => Nothing)(implicit
      traceContext: TraceContext
  ): StateTransferMessageResult = {
    val startEpoch = stateTransferStartEpoch.getOrElse(abort("Should be in state transfer"))
    // TODO(#24524) calculate reasonably
    // Right now, the state transfer end epoch is the latest remotely completed epoch according to the last received
    //  block transfer response, so slow/malicious nodes can significantly impact the process.
    val endEpoch = remoteLatestCompleteEpoch

    val prePrepare = commitCert.prePrepare.message
    val blockMetadata = prePrepare.blockMetadata
    // TODO(#19289) support variable epoch lengths
    val blockLastInEpoch = (blockMetadata.blockNumber + 1) % epochLength == 0

    // Blocks within an epoch can be received and stored out of order, but that's fine because the Output module
    //  orders them (has a Peano queue).
    logger.debug(s"State transfer: sending block $blockMetadata to Output")
    messageSender.sendBlockToOutput(prePrepare, blockLastInEpoch, endEpoch)

    if (blockMetadata.epochNumber == endEpoch && blockLastInEpoch) {
      // Clean up remaining timeouts as we're done with block transfer
      blockTransferResponseTimeouts.view.values.foreach(_.cancelTimeout())
      blockTransferResponseTimeouts.clear()
      val numberOfTransferredEpochs = remoteLatestCompleteEpoch - startEpoch + 1
      StateTransferMessageResult.BlockTransferCompleted(endEpoch, numberOfTransferredEpochs)
    } else if (blockLastInEpoch) {
      // Cancel a timeout if exists
      blockTransferResponseTimeouts.get(from).foreach(_.cancelTimeout())
      // Wait for storing new epoch
      StateTransferMessageResult.Continue
    } else {
      StateTransferMessageResult.Continue
    }
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
  ) extends StateTransferMessageResult
}
