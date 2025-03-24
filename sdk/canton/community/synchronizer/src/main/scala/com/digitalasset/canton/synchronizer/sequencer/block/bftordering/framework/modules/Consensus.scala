// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, Module}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

object Consensus {

  sealed trait Message[+E] extends Product

  final case object Init extends Message[Nothing]

  final case object Start extends Message[Nothing]

  sealed trait Admin extends Message[Nothing]
  object Admin {
    final case class GetOrderingTopology(callback: (EpochNumber, Set[BftNodeId]) => Unit)
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

      override def from: BftNodeId = epochStatus.from
    }

    object RetransmissionRequest
        extends VersioningCompanionContextMemoization[
          RetransmissionRequest,
          BftNodeId,
        ] {
      override def name: String = "RetransmissionRequest"
      def create(epochStatus: ConsensusStatus.EpochStatus): RetransmissionRequest =
        RetransmissionRequest(epochStatus)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
          None,
        )

      private def fromProtoRetransmissionMessage(
          from: BftNodeId,
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
          from: BftNodeId,
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
        from: BftNodeId,
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
          BftNodeId,
        ] {
      override def name: String = "RetransmissionResponse"
      def create(
          from: BftNodeId,
          commitCertificates: Seq[CommitCertificate],
      ): RetransmissionResponse =
        RetransmissionResponse(from, commitCertificates)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
          None,
        )

      private def fromProtoRetransmissionMessage(
          from: BftNodeId,
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
          from: BftNodeId,
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
        epoch: EpochNumber,
        from: BftNodeId,
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
            v30.BlockTransferRequest.of(epoch)
          )
        )

      override protected val companionObj: BlockTransferRequest.type = BlockTransferRequest

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object BlockTransferRequest
        extends VersioningCompanionContextMemoization[
          BlockTransferRequest,
          BftNodeId,
        ] {
      override def name: String = "BlockTransferRequest"
      def create(
          epoch: EpochNumber,
          from: BftNodeId,
      ): BlockTransferRequest = BlockTransferRequest(epoch, from)(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
        None,
      )

      private def fromProtoStateTransferMessage(from: BftNodeId, value: v30.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferRequest] = for {
        protoBlockTransferRequest <- value.message.blockRequest.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
      } yield fromProto(from, protoBlockTransferRequest)(originalByteString)

      def fromProto(from: BftNodeId, request: v30.BlockTransferRequest)(
          originalByteString: ByteString
      ): BlockTransferRequest =
        BlockTransferRequest(EpochNumber(request.epoch), from)(
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
        commitCertificate: Option[CommitCertificate],
        latestCompletedEpoch: EpochNumber,
        from: BftNodeId,
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
            v30.BlockTransferResponse.of(commitCertificate.map(_.toProto), latestCompletedEpoch)
          )
        )
      override protected val companionObj: BlockTransferResponse.type = BlockTransferResponse

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object BlockTransferResponse
        extends VersioningCompanionContextMemoization[
          BlockTransferResponse,
          BftNodeId,
        ] {
      override def name: String = "BlockTransferResponse"
      def create(
          commitCertificate: Option[CommitCertificate],
          latestCompletedEpoch: EpochNumber,
          from: BftNodeId,
      ): BlockTransferResponse = BlockTransferResponse(
        commitCertificate,
        latestCompletedEpoch,
        from,
      )(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
        None,
      )

      private def fromProtoStateTransferMessage(from: BftNodeId, value: v30.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferResponse] = for {
        protoBlockTransferResponse <- value.message.blockResponse.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        blockTransferResponse <- fromProto(from, protoBlockTransferResponse)(originalByteString)
      } yield blockTransferResponse

      def fromProto(
          from: BftNodeId,
          protoResponse: v30.BlockTransferResponse,
      )(originalByteString: ByteString): ParsingResult[BlockTransferResponse] =
        for {
          commitCert <- protoResponse.commitCertificate.map(CommitCertificate.fromProto).sequence
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield BlockTransferResponse(
          commitCert,
          EpochNumber(protoResponse.latestCompletedEpoch),
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

    final case class RetryBlockTransferRequest(request: SignedMessage[BlockTransferRequest])
        extends StateTransferMessage

    final case class BlockVerified[E <: Env[E]](
        commitCertificate: CommitCertificate,
        remoteLatestCompleteEpoch: EpochNumber,
        from: BftNodeId,
    ) extends StateTransferMessage

    final case class BlockStored[E <: Env[E]](
        commitCertificate: CommitCertificate,
        remoteLatestCompleteEpoch: EpochNumber,
        from: BftNodeId,
    ) extends StateTransferMessage
  }

  final case class StateTransferCompleted[E <: Env[E]](
      newEpochTopologyMessage: Consensus.NewEpochTopology[E]
  ) extends Message[E]

  final case class NewEpochTopology[E <: Env[E]](
      epochNumber: EpochNumber,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
      previousEpochMaxBftTime: CantonTimestamp,
      lastBlockFromPreviousEpochMode: OrderedBlockForOutput.Mode,
  ) extends Message[E]

  final case class NewEpochStored[E <: Env[E]](
      newEpochInfo: EpochInfo,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
  ) extends Message[E]

  final case object SegmentCancelledEpoch extends Message[Nothing]
}

trait Consensus[E <: Env[E]] extends Module[E, Consensus.Message[E]] {
  val dependencies: ConsensusModuleDependencies[E]
}
