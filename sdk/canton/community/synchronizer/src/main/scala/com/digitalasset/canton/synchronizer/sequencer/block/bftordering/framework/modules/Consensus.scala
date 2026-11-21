// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
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
    final case class SetPerformanceMetricsEnabled(enabled: Boolean) extends Admin
  }

  sealed trait ProtocolMessage extends Message[Nothing]

  sealed trait LocalAvailability extends ProtocolMessage
  object LocalAvailability {
    final case object NoProposalAvailableYet extends LocalAvailability
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
        hasCompletedLedSegment: Boolean,
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
    final case object PeriodicStatusBroadcast extends RetransmissionsMessage
    final case class SegmentStatus(
        epochNumber: EpochNumber,
        segmentIndex: Int,
        status: ConsensusStatus.SegmentStatus,
    ) extends RetransmissionsMessage

    final case class UnverifiedNetworkMessage(message: RetransmissionsNetworkMessage)
        extends RetransmissionsMessage
    final case class VerifiedNetworkMessage(message: RetransmissionsNetworkMessage)
        extends RetransmissionsMessage

    sealed trait RetransmissionsNetworkMessage extends MessageFrom {
      def toProto: v30.RetransmissionMessage
    }

    final case class RetransmissionRequest(
        signedEpochStatus: SignedMessage[ConsensusStatus.EpochStatus]
    ) extends RetransmissionsNetworkMessage {
      def toProto: v30.RetransmissionMessage =
        v30.RetransmissionMessage(
          v30.RetransmissionMessage.Message.RetransmissionRequest(
            signedEpochStatus.toProtoV1
          )
        )
      override def from: BftNodeId = signedEpochStatus.from
    }

    object RetransmissionRequest {
      def fromProto(
          from: BftNodeId,
          request: v30.SignedMessage,
      ): ParsingResult[RetransmissionRequest] = SignedMessage
        .fromProto(v30.EpochStatus)(
          ConsensusStatus.EpochStatus.fromProto(from, _)
        )(request)
        .map(Consensus.RetransmissionsMessage.RetransmissionRequest.apply)
    }

    final case class RetransmissionResponse(
        from: BftNodeId,
        commitCertificates: Seq[CommitCertificate],
    ) extends RetransmissionsNetworkMessage {
      def toProto: v30.RetransmissionMessage =
        v30.RetransmissionMessage(
          v30.RetransmissionMessage.Message.RetransmissionResponse(
            v30.RetransmissionResponse(commitCertificates.map(_.toProto))
          )
        )
    }

    object RetransmissionResponse {
      def fromProto(
          from: BftNodeId,
          protoRetransmissionResponse: v30.RetransmissionResponse,
      ): ParsingResult[RetransmissionResponse] =
        for {
          commitCertificates <- protoRetransmissionResponse.commitCertificates.traverse(
            CommitCertificate.fromProto(_, actualSender = Some(from))
          )
        } yield RetransmissionResponse(from, commitCertificates)
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
        v30.StateTransferMessage(
          v30.StateTransferMessage.Message.BlockRequest(
            v30.BlockTransferRequest(epoch)
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
      )(implicit synchronizerProtocolVersion: ProtocolVersion): BlockTransferRequest =
        BlockTransferRequest(epoch, from)(
          protocolVersionRepresentativeFor(synchronizerProtocolVersion),
          None,
        )

      private def fromProtoStateTransferMessage(from: BftNodeId, value: v30.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferRequest] =
        for {
          protoBlockTransferRequest <- value.message.blockRequest.toRight(
            ProtoDeserializationError.OtherError(s"Not a $name message")
          )
          result <- fromProto(from, protoBlockTransferRequest)(originalByteString)
        } yield result

      def fromProto(from: BftNodeId, request: v30.BlockTransferRequest)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferRequest] =
        for {
          rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData)
        } yield BlockTransferRequest(EpochNumber(request.epoch), from)(
          rpv,
          Some(originalByteString),
        )

      override def versioningTable: VersioningTable =
        VersioningTable(
          SupportedVersions.ProtoData ->
            VersionedProtoCodec(SupportedVersions.CantonProtocol)(v30.StateTransferMessage)(
              supportedProtoVersionMemoized(_)(fromProtoStateTransferMessage),
              _.toProto,
            )
        )
    }

    final case class BlockTransferResponse private (
        commitCertificate: Option[CommitCertificate],
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          BlockTransferResponse.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends StateTransferNetworkMessage
        with HasProtocolVersionedWrapper[BlockTransferResponse] {

      def toProto: v30.StateTransferMessage =
        v30.StateTransferMessage(
          v30.StateTransferMessage.Message.BlockResponse(
            v30.BlockTransferResponse(commitCertificate.map(_.toProto))
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
          from: BftNodeId,
      )(implicit synchronizerProtocolVersion: ProtocolVersion): BlockTransferResponse =
        BlockTransferResponse(
          commitCertificate,
          from,
        )(
          protocolVersionRepresentativeFor(synchronizerProtocolVersion),
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
      )(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferResponse] =
        for {
          commitCert <- protoResponse.commitCertificate
            .map(CommitCertificate.fromProto(_, actualSender = Some(from)))
            .sequence
          rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData)
        } yield BlockTransferResponse(commitCert, from)(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        SupportedVersions.ProtoData ->
          VersionedProtoCodec(SupportedVersions.CantonProtocol)(v30.StateTransferMessage)(
            supportedProtoVersionMemoized(_)(fromProtoStateTransferMessage),
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
        from: BftNodeId,
    ) extends StateTransferMessage

    final case class BlockStored[E <: Env[E]](
        commitCertificate: CommitCertificate,
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
