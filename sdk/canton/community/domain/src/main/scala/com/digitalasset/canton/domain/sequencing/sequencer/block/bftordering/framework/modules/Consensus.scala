// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  BftOrderingMessageBody as ProtoBftOrderingMessageBody,
  BlockTransferData as ProtoBlockTransferData,
  BlockTransferRequest as ProtoBlockTransferRequest,
  BlockTransferResponse as ProtoBlockTransferResponse,
  StateTransferMessage as ProtoStateTransferMessage,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.OrderedBlock
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{Env, Module}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.processing.EffectiveTime

object Consensus {

  sealed trait Message[+E] extends Product

  final case object Init extends Message[Nothing]

  final case object Start extends Message[Nothing]

  sealed trait Admin extends Message[Nothing]
  object Admin {
    final case class GetOrderingTopology(callback: (EpochNumber, Set[SequencerId]) => Unit)
        extends Admin
  }

  sealed trait ProtocolMessage extends Message[Nothing]

  sealed trait LocalAvailability extends ProtocolMessage
  object LocalAvailability {
    final case class ProposalCreated(orderingBlock: OrderingBlock, epochNumber: EpochNumber)
        extends LocalAvailability
  }

  /** The networked consensus protocol for ISS running on top of PBFT
    */
  sealed trait ConsensusMessage extends ProtocolMessage
  object ConsensusMessage {
    final case class PbftUnverifiedNetworkMessage(
        underlyingNetworkMessage: SignedMessage[
          ConsensusSegment.ConsensusMessage.PbftNetworkMessage
        ]
    ) extends ConsensusMessage

    final case class PbftVerifiedNetworkMessage(
        underlyingNetworkMessage: SignedMessage[
          ConsensusSegment.ConsensusMessage.PbftNetworkMessage
        ]
    ) extends ConsensusMessage

    final case class BlockOrdered(
        block: OrderedBlock,
        commits: Seq[SignedMessage[ConsensusSegment.ConsensusMessage.Commit]],
    ) extends ConsensusMessage

    final case class CompleteEpochStored(epoch: Epoch) extends ConsensusMessage

    final case class AsyncException(e: Throwable) extends ConsensusMessage
  }

  sealed trait StateTransferMessage extends ProtocolMessage
  object StateTransferMessage {
    final case class BlockTransferRequest private (
        startEpoch: EpochNumber,
        latestCompletedEpoch: EpochNumber,
        from: SequencerId,
    ) extends StateTransferMessage {
      def toProto: ProtoBftOrderingMessageBody =
        ProtoBftOrderingMessageBody.of(
          ProtoBftOrderingMessageBody.Message.StateTransferMessage(
            ProtoStateTransferMessage.of(
              ProtoStateTransferMessage.Message.BlockRequest(
                ProtoBlockTransferRequest.of(startEpoch, latestCompletedEpoch)
              )
            )
          )
        )
    }
    object BlockTransferRequest {
      def create(
          startEpoch: EpochNumber,
          latestCompletedEpoch: EpochNumber,
          from: SequencerId,
      ): BlockTransferRequest = BlockTransferRequest(startEpoch, latestCompletedEpoch, from)

      def fromProto(request: ProtoBlockTransferRequest, from: SequencerId): BlockTransferRequest =
        BlockTransferRequest(
          EpochNumber(request.startEpoch),
          EpochNumber(request.latestCompletedEpoch),
          from,
        )
    }

    final case class BlockTransferData private (
        prePrepare: SignedMessage[PrePrepare],
        pendingTopologyChanges: Boolean,
    ) {
      def toProto: ProtoBlockTransferData =
        ProtoBlockTransferData.of(
          Some(prePrepare.toProto),
          pendingTopologyChanges,
        )
    }
    object BlockTransferData {
      def create(
          prePrepare: SignedMessage[PrePrepare],
          pendingTopologyChanges: Boolean,
      ): BlockTransferData = BlockTransferData(prePrepare, pendingTopologyChanges)

      def fromProto(protoData: ProtoBlockTransferData): ParsingResult[BlockTransferData] =
        for {
          signedMessage <-
            ProtoConverter.required(
              "blockPrePrepare",
              protoData.blockPrePrepare,
            )
          prePrepare <-
            SignedMessage.fromProto(
              ConsensusSegment.ConsensusMessage.PrePrepare.fromProtoConsensusMessage
            )(signedMessage)
        } yield BlockTransferData(prePrepare, protoData.pendingTopologyChanges)
    }

    final case class BlockTransferResponse private (
        latestCompletedEpoch: EpochNumber,
        latestCompletedEpochTopologySnapshotEffectiveTime: EffectiveTime,
        blockTransferData: Seq[BlockTransferData],
        lastBlockCommits: Seq[SignedMessage[Commit]],
        from: SequencerId,
    ) extends StateTransferMessage {
      def toProto: ProtoBftOrderingMessageBody = {
        val protoLastBlockCommits = lastBlockCommits.view.map(_.toProto).toSeq
        ProtoBftOrderingMessageBody.of(
          ProtoBftOrderingMessageBody.Message.StateTransferMessage(
            ProtoStateTransferMessage.of(
              ProtoStateTransferMessage.Message.BlockResponse(
                ProtoBlockTransferResponse.of(
                  latestCompletedEpoch,
                  Some(latestCompletedEpochTopologySnapshotEffectiveTime.value.toProtoTimestamp),
                  blockTransferData.view.map(_.toProto).toSeq,
                  protoLastBlockCommits,
                )
              )
            )
          )
        )
      }
    }
    object BlockTransferResponse {
      def create(
          latestCompletedEpoch: EpochNumber,
          lastEpochToTransferTopologySnapshotEffectiveTime: EffectiveTime,
          blockTransferData: Seq[BlockTransferData],
          lastBlockCommits: Seq[SignedMessage[Commit]],
          from: SequencerId,
      ): BlockTransferResponse = BlockTransferResponse(
        latestCompletedEpoch,
        lastEpochToTransferTopologySnapshotEffectiveTime,
        blockTransferData,
        lastBlockCommits,
        from,
      )

      def fromProto(
          protoResponse: ProtoBlockTransferResponse,
          from: SequencerId,
      ): ParsingResult[BlockTransferResponse] = {
        val blockTransferDataE = protoResponse.blockTransferData.traverse {
          BlockTransferData.fromProto
        }

        val lastBlockCommitsE = protoResponse.lastBlockCommits.traverse {
          SignedMessage.fromProto(
            ConsensusSegment.ConsensusMessage.Commit.fromProtoConsensusMessage
          )
        }

        for {
          blockTransferData <- blockTransferDataE
          lastBlockCommits <- lastBlockCommitsE
          latestCompletedEpochTopologySnapshotSequencingInstantP <- ProtoConverter
            .required(
              "latestCompletedEpochTopologyEffectiveTime",
              protoResponse.latestCompletedEpochTopologyEffectiveTime,
            )
          latestCompletedEpochTopologyEffectiveTime <- CantonTimestamp
            .fromProtoTimestamp(latestCompletedEpochTopologySnapshotSequencingInstantP)
            .map(EffectiveTime(_))
        } yield BlockTransferResponse(
          EpochNumber(protoResponse.latestCompletedEpoch),
          latestCompletedEpochTopologyEffectiveTime,
          blockTransferData,
          lastBlockCommits,
          from,
        )
      }
    }

    final case class SendBlockTransferRequest(
        blockTransferRequest: BlockTransferRequest,
        to: SequencerId,
    ) extends StateTransferMessage

    final case class BlockStored[E <: Env[E]](
        blockTransferData: BlockTransferData,
        fullResponse: StateTransferMessage.BlockTransferResponse,
    ) extends StateTransferMessage
  }

  final case class NewEpochTopology[E <: Env[E]](
      epochNumber: EpochNumber,
      orderingTopology: OrderingTopology,
      cryptoProvider: CryptoProvider[E],
  ) extends Message[E]

  final case class NewEpochStored[E <: Env[E]](
      newEpochInfo: EpochInfo,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
  ) extends Message[E]
}

trait Consensus[E <: Env[E]] extends Module[E, Consensus.Message[E]] {
  val dependencies: ConsensusModuleDependencies[E]
}
