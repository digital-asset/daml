// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.util.{Failure, Success}

/** Belongs to [[StateTransferManager]] and sends state transfer-related messages. */
final class StateTransferMessageSender[E <: Env[E]](
    thisNode: BftNodeId,
    consensusDependencies: ConsensusModuleDependencies[E],
    epochLength: EpochLength, // TODO(#19289) support variable epoch lengths
    epochStore: EpochStore[E],
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import StateTransferMessageSender.*

  def sendBlockTransferRequest(
      blockTransferRequest: SignedMessage[StateTransferMessage.BlockTransferRequest],
      to: BftNodeId,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(
      s"State transfer: sending a block transfer request for epoch ${blockTransferRequest.message.epoch} to '$to'"
    )
    consensusDependencies.p2pNetworkOut.asyncSend(
      P2PNetworkOut.send(wrapSignedMessage(blockTransferRequest), to)
    )
  }

  def sendBlockTransferResponses(
      activeCryptoProvider: CryptoProvider[E],
      to: BftNodeId,
      forEpoch: EpochNumber,
      latestCompletedEpoch: EpochStore.Epoch,
  )(
      abort: String => Nothing
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val latestCompletedEpochNumber = latestCompletedEpoch.info.number
    if (latestCompletedEpochNumber < forEpoch) {
      logger.info(
        s"State transfer: nothing to transfer to '$to' for epoch $forEpoch, " +
          s"latest locally completed epoch is $latestCompletedEpochNumber"
      )
      val response =
        StateTransferMessage.BlockTransferResponse.create(commitCertificate = None, from = thisNode)
      sendResponse(response, activeCryptoProvider, to)
    } else {
      logger.info(s"State transfer: loading blocks from epoch $forEpoch (length=$epochLength)")
      context.pipeToSelf(
        // We load only one epoch at a time to avoid OOM errors.
        epochStore.loadCompleteBlocks(forEpoch, forEpoch)
      ) {
        case Success(blocks) =>
          if (blocks.length != epochLength) {
            abort(
              "Internal invariant violation: " +
                s"only whole epochs with blocks that have been ordered can be state transferred, but ${blocks.length} " +
                s"blocks have been loaded instead of $epochLength"
            )
          }
          val commitCerts = blocks.map(_.commitCertificate)
          val startBlockNumber = blocks.map(_.blockNumber).minOption
          logger.info(
            s"State transfer: sending block responses from epoch $forEpoch (start block number = $startBlockNumber) to '$to'"
          )
          commitCerts.foreach { commitCert =>
            // We send only one block at a time to avoid exceeding the max gRPC message size.
            val response =
              StateTransferMessage.BlockTransferResponse.create(Some(commitCert), from = thisNode)
            sendResponse(response, activeCryptoProvider, to)
          }
          None // do not send anything back
        case Failure(exception) => Some(Consensus.ConsensusMessage.AsyncException(exception))
      }
    }
  }

  def sendBlockToOutput(prePrepare: PrePrepare, lastInEpoch: Boolean): Unit = {
    val blockMetadata = prePrepare.blockMetadata

    consensusDependencies.output.asyncSend(
      Output.BlockOrdered(
        OrderedBlockForOutput(
          OrderedBlock(
            blockMetadata,
            prePrepare.block.proofs,
            prePrepare.canonicalCommitSet,
          ),
          prePrepare.viewNumber,
          prePrepare.from,
          lastInEpoch,
          mode = OrderedBlockForOutput.Mode.FromStateTransfer,
        )
      )
    )
  }

  def signMessage[Message <: StateTransferMessage.StateTransferNetworkMessage](
      cryptoProvider: CryptoProvider[E],
      stateTransferMessage: Message,
  )(
      continuation: SignedMessage[Message] => Unit
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit =
    context.pipeToSelf(
      cryptoProvider.signMessage(
        stateTransferMessage,
        AuthenticatedMessageType.BftSignedStateTransferMessage,
      )
    ) {
      case Failure(exception) =>
        logger.error(
          s"Can't sign state transfer message ${shortType(stateTransferMessage)}",
          exception,
        )
        None
      case Success(Left(errors)) =>
        logger.error(
          s"Can't sign state transfer message ${shortType(stateTransferMessage)}: $errors"
        )
        None
      case Success(Right(signedMessage)) =>
        continuation(signedMessage)
        None
    }

  private def sendResponse[Message <: StateTransferMessage.StateTransferNetworkMessage](
      response: Message,
      cryptoProvider: CryptoProvider[E],
      to: BftNodeId,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit =
    signMessage(cryptoProvider, response) { signedMessage =>
      consensusDependencies.p2pNetworkOut.asyncSend(
        P2PNetworkOut.send(wrapSignedMessage(signedMessage), to)
      )
    }
}

private object StateTransferMessageSender {

  private def wrapSignedMessage(
      signedMessage: SignedMessage[StateTransferMessage.StateTransferNetworkMessage]
  ): P2PNetworkOut.BftOrderingNetworkMessage =
    P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(signedMessage)
}
