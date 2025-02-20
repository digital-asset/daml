// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, Module}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import ConsensusSegment.ConsensusMessage.PrePrepare

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
        commitCertificate: CommitCertificate,
    ) extends ConsensusMessage

    final case class CompleteEpochStored(epoch: Epoch, commitCertificates: Seq[CommitCertificate])
        extends ConsensusMessage

    final case class SegmentCompletedEpoch(
        segmentFirstBlockNumber: BlockNumber,
        epochNumber: EpochNumber,
    ) extends ConsensusMessage

    final case class AsyncException(e: Throwable) extends ConsensusMessage
  }

  sealed trait RetransmissionsMessage extends ProtocolMessage
  object RetransmissionsMessage {
    sealed trait RetransmissionsNetworkMessage
        extends HasRepresentativeProtocolVersion
        with ProtocolVersionedMemoizedEvidence
        with MessageFrom

    final case object PeriodicStatusBroadcast extends RetransmissionsMessage
    final case class SegmentStatus(
        epochNumber: EpochNumber,
        segmentIndex: Int,
        status: ConsensusStatus.SegmentStatus,
    ) extends RetransmissionsMessage
    final case class UnverifiedNetworkMessage(message: SignedMessage[RetransmissionsNetworkMessage])
        extends RetransmissionsMessage
    final case class VerifiedNetworkMessage(message: RetransmissionsNetworkMessage)
        extends RetransmissionsMessage

    final case class RetransmissionRequest private (epochStatus: ConsensusStatus.EpochStatus)(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          RetransmissionRequest.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends RetransmissionsNetworkMessage
        with HasProtocolVersionedWrapper[RetransmissionRequest] {

      def toProto: v30.RetransmissionMessage =
        v30.RetransmissionMessage.of(
          v30.RetransmissionMessage.Message.RetransmissionRequest(
            epochStatus.toProto
          )
        )

      override protected val companionObj: RetransmissionRequest.type = RetransmissionRequest

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString

      override def from: SequencerId = epochStatus.from
    }

    object RetransmissionRequest
        extends VersioningCompanionContextMemoization[
          RetransmissionRequest,
          SequencerId,
        ] {
      override def name: String = "RetransmissionRequest"
      def create(epochStatus: ConsensusStatus.EpochStatus): RetransmissionRequest =
        RetransmissionRequest(epochStatus)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
          None,
        )

      private def fromProtoRetransmissionMessage(
          from: SequencerId,
          value: v30.RetransmissionMessage,
      )(
          originalByteString: ByteString
      ): ParsingResult[RetransmissionRequest] = for {
        protoRetransmissionRequest <- value.message.retransmissionRequest.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        result <- fromProto(from, protoRetransmissionRequest)(originalByteString)
      } yield result

      def fromProto(
          from: SequencerId,
          proto: v30.EpochStatus,
      )(
          originalByteString: ByteString
      ): ParsingResult[RetransmissionRequest] = for {
        epochStatus <- ConsensusStatus.EpochStatus.fromProto(from, proto)
      } yield RetransmissionRequest(epochStatus)(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
        Some(originalByteString),
      )

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v33
          )(v30.RetransmissionMessage)(
            supportedProtoVersionMemoized(_)(
              fromProtoRetransmissionMessage
            ),
            _.toProto,
          )
      )
    }

    final case class RetransmissionResponse private (
        from: SequencerId,
        commitCertificates: Seq[CommitCertificate],
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          RetransmissionResponse.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends RetransmissionsNetworkMessage
        with HasProtocolVersionedWrapper[RetransmissionResponse] {
      def toProto: v30.RetransmissionMessage =
        v30.RetransmissionMessage.of(
          v30.RetransmissionMessage.Message.RetransmissionResponse(
            v30.RetransmissionResponse(commitCertificates.map(_.toProto))
          )
        )

      override protected val companionObj: RetransmissionResponse.type = RetransmissionResponse

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object RetransmissionResponse
        extends VersioningCompanionContextMemoization[
          RetransmissionResponse,
          SequencerId,
        ] {
      override def name: String = "RetransmissionResponse"
      def create(
          from: SequencerId,
          commitCertificates: Seq[CommitCertificate],
      ): RetransmissionResponse =
        RetransmissionResponse(from, commitCertificates)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
          None,
        )

      private def fromProtoRetransmissionMessage(
          from: SequencerId,
          value: v30.RetransmissionMessage,
      )(
          originalByteString: ByteString
      ): ParsingResult[RetransmissionResponse] = for {
        protoRetransmissionResponse <- value.message.retransmissionResponse.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        response <- fromProto(from, protoRetransmissionResponse)(originalByteString)
      } yield response

      def fromProto(
          from: SequencerId,
          protoRetransmissionResponse: v30.RetransmissionResponse,
      )(
          originalByteString: ByteString
      ): ParsingResult[RetransmissionResponse] = for {
        commitCertificates <- protoRetransmissionResponse.commitCertificates.traverse(
          CommitCertificate.fromProto
        )
      } yield RetransmissionResponse(from, commitCertificates)(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
        Some(originalByteString),
      )

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v33
          )(v30.RetransmissionMessage)(
            supportedProtoVersionMemoized(_)(
              fromProtoRetransmissionMessage
            ),
            _.toProto,
          )
      )
    }
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
      def toProto: v30.StateTransferMessage =
        v30.StateTransferMessage.of(
          v30.StateTransferMessage.Message.BlockRequest(
            v30.BlockTransferRequest.of(startEpoch, latestCompletedEpoch)
          )
        )

      override protected val companionObj: BlockTransferRequest.type = BlockTransferRequest

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object BlockTransferRequest
        extends VersioningCompanionContextMemoization[
          BlockTransferRequest,
          SequencerId,
        ] {
      override def name: String = "BlockTransferRequest"
      def create(
          startEpoch: EpochNumber,
          latestCompletedEpoch: EpochNumber,
          from: SequencerId,
      ): BlockTransferRequest = BlockTransferRequest(startEpoch, latestCompletedEpoch, from)(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
        None,
      )

      private def fromProtoStateTransferMessage(from: SequencerId, value: v30.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferRequest] = for {
        protoBlockTransferRequest <- value.message.blockRequest.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
      } yield fromProto(from, protoBlockTransferRequest)(originalByteString)

      def fromProto(from: SequencerId, request: v30.BlockTransferRequest)(
          originalByteString: ByteString
      ): BlockTransferRequest =
        BlockTransferRequest(
          EpochNumber(request.startEpoch),
          EpochNumber(request.latestCompletedEpoch),
          from,
        )(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum),
          Some(originalByteString),
        ) // TODO(#23248)

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v33
          )(v30.StateTransferMessage)(
            supportedProtoVersionMemoized(_)(
              fromProtoStateTransferMessage
            ),
            _.toProto,
          )
      )
    }

    final case class BlockTransferResponse private (
        latestCompletedEpoch: EpochNumber,
        prePrepares: Seq[SignedMessage[PrePrepare]],
        from: SequencerId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          BlockTransferResponse.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends StateTransferNetworkMessage
        with HasProtocolVersionedWrapper[BlockTransferResponse] {
      def toProto: v30.StateTransferMessage =
        v30.StateTransferMessage.of(
          v30.StateTransferMessage.Message.BlockResponse(
            v30.BlockTransferResponse.of(
              latestCompletedEpoch,
              prePrepares.view.map(_.toProtoV1).toSeq,
            )
          )
        )
      override protected val companionObj: BlockTransferResponse.type = BlockTransferResponse

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object BlockTransferResponse
        extends VersioningCompanionContextMemoization[
          BlockTransferResponse,
          SequencerId,
        ] {
      override def name: String = "BlockTransferResponse"
      def create(
          latestCompletedEpoch: EpochNumber,
          prePrepares: Seq[SignedMessage[PrePrepare]],
          from: SequencerId,
      ): BlockTransferResponse = BlockTransferResponse(
        latestCompletedEpoch,
        prePrepares,
        from,
      )(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
        None,
      )

      private def fromProtoStateTransferMessage(from: SequencerId, value: v30.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferResponse] = for {
        protoBlockTransferResponse <- value.message.blockResponse.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        blockTransferResponse <- fromProto(from, protoBlockTransferResponse)(originalByteString)
      } yield blockTransferResponse

      def fromProto(
          from: SequencerId,
          protoResponse: v30.BlockTransferResponse,
      )(originalByteString: ByteString): ParsingResult[BlockTransferResponse] =
        for {
          prePrepares <- protoResponse.blockPrePrepares.traverse(
            SignedMessage.fromProto(v30.ConsensusMessage)(PrePrepare.fromProtoConsensusMessage)
          )
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield BlockTransferResponse(
          EpochNumber(protoResponse.latestCompletedEpoch),
          prePrepares,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v33
          )(v30.StateTransferMessage)(
            supportedProtoVersionMemoized(_)(
              fromProtoStateTransferMessage
            ),
            _.toProto,
          )
      )
    }
    final case class UnverifiedStateTransferMessage(
        signedMessage: SignedMessage[StateTransferMessage.StateTransferNetworkMessage]
    ) extends StateTransferMessage

    final case class VerifiedStateTransferMessage(message: StateTransferNetworkMessage)
        extends StateTransferMessage

    final case class ResendBlockTransferRequest(
        blockTransferRequest: SignedMessage[BlockTransferRequest],
        to: SequencerId,
    ) extends StateTransferMessage

    final case class BlocksStored[E <: Env[E]](
        prePrepares: Seq[PrePrepare],
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
      orderingTopology: OrderingTopology,
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
