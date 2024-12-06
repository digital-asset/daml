// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{Env, Module}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

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

    final case class SegmentCompletedEpoch(
        segmentFirstBlockNumber: BlockNumber,
        epochNumber: EpochNumber,
    ) extends ConsensusMessage

    final case class AsyncException(e: Throwable) extends ConsensusMessage
  }

  sealed trait StateTransferMessage extends ProtocolMessage

  object StateTransferMessage {
    sealed trait StateTransferNetworkMessage
        extends HasRepresentativeProtocolVersion
        with ProtocolVersionedMemoizedEvidence
        with MessageFrom

    final case class BlockTransferRequest private (
        startEpoch: EpochNumber,
        latestCompletedEpoch: EpochNumber,
        from: SequencerId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          BlockTransferRequest.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends StateTransferNetworkMessage
        with HasProtocolVersionedWrapper[BlockTransferRequest] {
      def toProto: v1.StateTransferMessage =
        v1.StateTransferMessage.of(
          v1.StateTransferMessage.Message.BlockRequest(
            v1.BlockTransferRequest.of(startEpoch, latestCompletedEpoch)
          )
        )

      override protected val companionObj: BlockTransferRequest.type = BlockTransferRequest

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object BlockTransferRequest
        extends HasMemoizedProtocolVersionedWithContextCompanion[
          BlockTransferRequest,
          SequencerId,
        ] {
      override def name: String = "BlockTransferRequest"
      def create(
          startEpoch: EpochNumber,
          latestCompletedEpoch: EpochNumber,
          from: SequencerId,
      ): BlockTransferRequest = BlockTransferRequest(startEpoch, latestCompletedEpoch, from)(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum),
        None,
      )

      private def fromProtoStateTransferMessage(from: SequencerId, value: v1.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferRequest] = for {
        protoBlockTransferRequest <- value.message.blockRequest.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
      } yield fromProto(from, protoBlockTransferRequest)(originalByteString)

      def fromProto(from: SequencerId, request: v1.BlockTransferRequest)(
          originalByteString: ByteString
      ): BlockTransferRequest =
        BlockTransferRequest(
          EpochNumber(request.startEpoch),
          EpochNumber(request.latestCompletedEpoch),
          from,
        )(protocolVersionRepresentativeFor(ProtocolVersion.minimum), Some(originalByteString))

      override def supportedProtoVersions: SupportedProtoVersions =
        SupportedProtoVersions(
          ProtoVersion(30) ->
            VersionedProtoConverter(
              ProtocolVersion.v33
            )(v1.StateTransferMessage)(
              supportedProtoVersionMemoized(_)(
                fromProtoStateTransferMessage
              ),
              _.toProto,
            )
        )
    }

    final case class BlockTransferResponse private (
        latestCompletedEpoch: EpochNumber,
        commitCertificates: Seq[CommitCertificate],
        from: SequencerId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          BlockTransferResponse.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends StateTransferNetworkMessage
        with HasProtocolVersionedWrapper[BlockTransferResponse] {
      def toProto: v1.StateTransferMessage =
        v1.StateTransferMessage.of(
          v1.StateTransferMessage.Message.BlockResponse(
            v1.BlockTransferResponse.of(
              latestCompletedEpoch,
              commitCertificates.view.map(_.toProto).toSeq,
            )
          )
        )
      override protected val companionObj: BlockTransferResponse.type = BlockTransferResponse

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object BlockTransferResponse
        extends HasMemoizedProtocolVersionedWithContextCompanion[
          BlockTransferResponse,
          SequencerId,
        ] {
      override def name: String = "BlockTransferResponse"
      def create(
          latestCompletedEpoch: EpochNumber,
          commitCertificates: Seq[CommitCertificate],
          from: SequencerId,
      ): BlockTransferResponse = BlockTransferResponse(
        latestCompletedEpoch,
        commitCertificates,
        from,
      )(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum),
        None,
      )

      private def fromProtoStateTransferMessage(from: SequencerId, value: v1.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferResponse] = for {
        protoBlockTransferResponse <- value.message.blockResponse.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        blockTransferResponse <- fromProto(from, protoBlockTransferResponse)(originalByteString)
      } yield blockTransferResponse

      def fromProto(
          from: SequencerId,
          protoResponse: v1.BlockTransferResponse,
      )(originalByteString: ByteString): ParsingResult[BlockTransferResponse] =
        for {
          commitCertificates <- protoResponse.commitCertificates.traverse(
            CommitCertificate.fromProto
          )
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield BlockTransferResponse(
          EpochNumber(protoResponse.latestCompletedEpoch),
          commitCertificates,
          from,
        )(rpv, Some(originalByteString))

      override def supportedProtoVersions: SupportedProtoVersions =
        SupportedProtoVersions(
          ProtoVersion(30) ->
            VersionedProtoConverter(
              ProtocolVersion.v33
            )(v1.StateTransferMessage)(
              supportedProtoVersionMemoized(_)(
                fromProtoStateTransferMessage
              ),
              _.toProto,
            )
        )
    }

    final case class NetworkMessage(message: StateTransferNetworkMessage)
        extends StateTransferMessage

    final case class ResendBlockTransferRequest(
        blockTransferRequest: SignedMessage[BlockTransferRequest],
        to: SequencerId,
    ) extends StateTransferMessage

    final case class BlocksStored[E <: Env[E]](
        commitCertificates: Seq[CommitCertificate],
        stateTransferEndEpoch: EpochNumber,
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

  trait CatchUpMessage extends Message[Nothing]
  object CatchUpMessage {
    final case object SegmentCancelledEpoch extends CatchUpMessage
  }
}

trait Consensus[E <: Env[E]] extends Module[E, Consensus.Message[E]] {
  val dependencies: ConsensusModuleDependencies[E]
}
