// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.crypto.{HashPurpose, SigningKeyUsage}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
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
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.util.{Failure, Success}

/** Belongs to [[StateTransferManager]] and sends state transfer-related messages. */
final class StateTransferMessageSender[E <: Env[E]](
    consensusDependencies: ConsensusModuleDependencies[E],
    epochLength: EpochLength, // TODO(#19289) support variable epoch lengths
    epochStore: EpochStore[E],
    thisPeer: SequencerId,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import StateTransferMessageSender.*

  def sendBlockTransferRequest(
      blockTransferRequest: SignedMessage[StateTransferMessage.BlockTransferRequest],
      to: SequencerId,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"State transfer: sending a block transfer request to $to")
    consensusDependencies.p2pNetworkOut.asyncSend(
      P2PNetworkOut.send(wrapSignedMessage(blockTransferRequest), to)
    )
  }

  def sendBlockTransferResponse(
      activeCryptoProvider: CryptoProvider[E],
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
        .create(lastEpochToTransfer, prePrepares = Seq.empty, from = thisPeer)
      respond(response, activeCryptoProvider, to)
    } else {
      val blocksToTransfer = (lastEpochToTransfer - startEpoch + 1) * epochLength
      logger.info(
        s"State transfer: loading blocks from epochs $startEpoch to $lastEpochToTransfer " +
          s"(blocksToTransfer = $blocksToTransfer)"
      )
      context.pipeToSelf(
        epochStore.loadCompleteBlocks(startEpoch, lastEpochToTransfer)
      ) {
        case Success(blocks) =>
          if (blocks.length != blocksToTransfer) {
            abort(
              "Internal invariant violation: " +
                s"only whole epochs with blocks that have been ordered can be state transferred, but ${blocks.length} " +
                s"blocks have been loaded instead of $blocksToTransfer"
            )
          }
          val prePrepares = blocks.map(_.commitCertificate.prePrepare)
          val startBlockNumber = blocks.map(_.blockNumber).minOption
          logger.info(
            s"State transfer: sending blocks starting from epoch $startEpoch (block number = $startBlockNumber) up to " +
              s"$lastEpochToTransfer (inclusive) to $to"
          )
          val response = StateTransferMessage.BlockTransferResponse
            .create(lastEpochToTransfer, prePrepares, from = thisPeer)
          respond(response, activeCryptoProvider, to)
          None // do not send anything back
        case Failure(exception) => Some(Consensus.ConsensusMessage.AsyncException(exception))
      }
    }
  }

  def sendBlockToOutput(prePrepare: PrePrepare, endEpoch: EpochNumber): Unit = {
    val blockMetadata = prePrepare.blockMetadata
    // TODO(#19289) support variable epoch lengths
    val isLastInEpoch =
      (blockMetadata.blockNumber + 1) % epochLength == 0 // As blocks are 0-indexed
    val isLastStateTransferred =
      blockMetadata.blockNumber == (endEpoch * epochLength) + epochLength - 1

    consensusDependencies.output.asyncSend(
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

  def signMessage[Message <: StateTransferMessage.StateTransferNetworkMessage](
      cryptoProvider: CryptoProvider[E],
      stateTransferMessage: Message,
  )(
      continuation: SignedMessage[Message] => Unit
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit =
    context.pipeToSelf(
      cryptoProvider.signMessage(
        stateTransferMessage,
        HashPurpose.BftSignedStateTransferMessage,
        SigningKeyUsage.ProtocolOnly,
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

  private def respond[Message <: StateTransferMessage.StateTransferNetworkMessage](
      response: Message,
      cryptoProvider: CryptoProvider[E],
      to: SequencerId,
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
