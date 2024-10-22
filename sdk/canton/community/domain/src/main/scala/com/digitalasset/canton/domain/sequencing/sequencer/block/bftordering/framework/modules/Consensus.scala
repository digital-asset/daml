// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  BftOrderingMessageBody as ProtoBftOrderingMessageBody,
  BlockTransferRequest as ProtoBlockTransferRequest,
  BlockTransferResponse as ProtoBlockTransferResponse,
  StateTransferMessage as ProtoStateTransferMessage,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
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
        underlyingNetworkMessage: ConsensusSegment.ConsensusMessage.PbftNetworkMessage
    ) extends ConsensusMessage

    final case class PbftVerifiedNetworkMessage(
        underlyingNetworkMessage: ConsensusSegment.ConsensusMessage.PbftNetworkMessage
    ) extends ConsensusMessage

    final case class BlockOrdered(
        block: OrderedBlock,
        commits: Seq[ConsensusSegment.ConsensusMessage.Commit],
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

    final case class BlockTransferResponse private (
        latestCompletedEpoch: EpochNumber,
        latestCompletedEpochTopologySnapshotEffectiveTime: EffectiveTime,
        prePrepares: Seq[PrePrepare],
        lastBlockCommits: Seq[Commit],
        from: SequencerId,
    ) extends StateTransferMessage {
      def toProto: ProtoBftOrderingMessageBody = {
        val protoPrePrepares = prePrepares.view.map(_.toProto).toSeq
        val protoLastBlockCommits = lastBlockCommits.view.map(_.toProto).toSeq
        ProtoBftOrderingMessageBody.of(
          ProtoBftOrderingMessageBody.Message.StateTransferMessage(
            ProtoStateTransferMessage.of(
              ProtoStateTransferMessage.Message.BlockResponse(
                ProtoBlockTransferResponse.of(
                  latestCompletedEpoch,
                  Some(latestCompletedEpochTopologySnapshotEffectiveTime.value.toProtoTimestamp),
                  protoPrePrepares,
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
          prePrepares: Seq[PrePrepare],
          lastBlockCommits: Seq[Commit],
          from: SequencerId,
      ): BlockTransferResponse = BlockTransferResponse(
        latestCompletedEpoch,
        lastEpochToTransferTopologySnapshotEffectiveTime,
        prePrepares,
        lastBlockCommits,
        from,
      )

      def fromProto(
          protoResponse: ProtoBlockTransferResponse,
          from: SequencerId,
      ): ParsingResult[BlockTransferResponse] = {
        val prePreparesE = protoResponse.blockPrePrepares.map { protoConsensusMessage =>
          for {
            header <- ConsensusSegment.ConsensusMessage.PbftNetworkMessage.headerFromProto(
              protoConsensusMessage
            )
            protoPrePrepare <- protoConsensusMessage.message.prePrepare.toRight(
              ProtoDeserializationError.OtherError("Not a PrePrepare message")
            )
            prePrepare <- PrePrepare.fromProto(
              header.blockMetadata,
              header.viewNumber,
              header.timestamp,
              protoPrePrepare,
              header.from,
            )
          } yield prePrepare
        }.sequence

        val lastBlockCommitsE = protoResponse.lastBlockCommits.map { protoConsensusMessage =>
          for {
            header <- ConsensusSegment.ConsensusMessage.PbftNetworkMessage.headerFromProto(
              protoConsensusMessage
            )
            protoCommit <- protoConsensusMessage.message.commit.toRight(
              ProtoDeserializationError.OtherError("Not a Commit message")
            )
            commit <- Commit.fromProto(
              header.blockMetadata,
              header.viewNumber,
              header.timestamp,
              protoCommit,
              header.from,
            )
          } yield commit
        }.sequence

        for {
          prePrepares <- prePreparesE
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
          prePrepares,
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
        prePrepare: PrePrepare,
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
