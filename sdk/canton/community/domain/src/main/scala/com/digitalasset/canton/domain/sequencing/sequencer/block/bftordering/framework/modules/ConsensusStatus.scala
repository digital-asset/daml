// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

/** Status messages that describe how far into the consensus process a node is.
  * This is used as part of retransmissions such that receiving nodes can tell if there are messages
  * they can retransmit in order to help the originating node make progress.
  */
object ConsensusStatus {

  final case class EpochStatus private (
      from: SequencerId,
      epochNumber: EpochNumber,
      segments: Seq[SegmentStatus],
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        EpochStatus.type
      ],
      override val deserializedFrom: Option[ByteString],
  ) extends ProtocolVersionedMemoizedEvidence
      with MessageFrom
      with HasProtocolVersionedWrapper[EpochStatus] {
    def toProto: v1.EpochStatus = v1.EpochStatus(epochNumber, segments.map(_.toProto))

    override protected val companionObj: EpochStatus.type = EpochStatus

    override protected[this] def toByteStringUnmemoized: ByteString =
      super[HasProtocolVersionedWrapper].toByteString
  }

  object EpochStatus
      extends HasMemoizedProtocolVersionedWithContextCompanion[
        EpochStatus,
        SequencerId,
      ] {
    override def name: String = "EpochStatus"
    def create(
        from: SequencerId,
        epochNumber: EpochNumber,
        segments: Seq[SegmentStatus],
    ): EpochStatus = EpochStatus(from, epochNumber, segments)(
      protocolVersionRepresentativeFor(ProtocolVersion.minimum),
      None,
    )

    override def supportedProtoVersions: SupportedProtoVersions =
      SupportedProtoVersions(
        ProtoVersion(30) ->
          VersionedProtoConverter(
            ProtocolVersion.v33
          )(v1.EpochStatus)(
            supportedProtoVersionMemoized(_)(
              fromProto
            ),
            _.toProto,
          )
      )

    def fromProto(from: SequencerId, protoEpochStatus: v1.EpochStatus)(
        originalByteString: ByteString
    ): ParsingResult[EpochStatus] = for {
      segments <- protoEpochStatus.segments.traverse(SegmentStatus.fromProto)
    } yield EpochStatus(
      from,
      EpochNumber(protoEpochStatus.epochNumber),
      segments,
    )(protocolVersionRepresentativeFor(ProtocolVersion.minimum), Some(originalByteString))
  }

  sealed trait SegmentStatus {
    def toProto: v1.SegmentStatus
  }

  object SegmentStatus {
    final object Complete extends SegmentStatus {
      override val toProto: v1.SegmentStatus =
        v1.SegmentStatus(v1.SegmentStatus.Status.Complete(com.google.protobuf.empty.Empty()))
    }
    sealed trait Incomplete extends SegmentStatus {
      def viewNumber: ViewNumber
      def areBlocksComplete: Seq[Boolean]
    }

    final case class InProgress(viewNumber: ViewNumber, blockStatuses: Seq[BlockStatus])
        extends Incomplete {
      override def areBlocksComplete: Seq[Boolean] = blockStatuses.map(_.isComplete)
      override def toProto: v1.SegmentStatus =
        v1.SegmentStatus(
          v1.SegmentStatus.Status.InProgress(
            v1.SegmentInProgress(viewNumber, blockStatuses.map(_.toProto))
          )
        )
    }
    final case class InViewChange(
        viewNumber: ViewNumber,
        viewChangeMessagesPresent: Seq[Boolean],
        areBlocksComplete: Seq[Boolean],
    ) extends Incomplete {
      override def toProto: v1.SegmentStatus = v1.SegmentStatus(
        v1.SegmentStatus.Status.InViewChange(
          v1.SegmentInViewChange(viewNumber, viewChangeMessagesPresent, areBlocksComplete)
        )
      )
    }

    private[modules] def fromProto(proto: v1.SegmentStatus): ParsingResult[SegmentStatus] =
      proto.status match {
        case v1.SegmentStatus.Status.InViewChange(
              v1.SegmentInViewChange(viewChange, viewChangeMessagesPresent, areBlocksComplete)
            ) =>
          Right(
            SegmentStatus
              .InViewChange(ViewNumber(viewChange), viewChangeMessagesPresent, areBlocksComplete)
          )
        case v1.SegmentStatus.Status
              .InProgress(v1.SegmentInProgress(viewNumber, blockStatuses)) =>
          for {
            blocks <- blockStatuses.traverse(BlockStatus.fromProto)
          } yield SegmentStatus.InProgress(
            ViewNumber(viewNumber),
            blocks,
          )
        case v1.SegmentStatus.Status.Complete(_) => Right(SegmentStatus.Complete)
        case v1.SegmentStatus.Status.Empty =>
          Left(ProtoDeserializationError.OtherError("Empty Received"))
      }

  }

  sealed trait BlockStatus {
    def isComplete: Boolean
    def toProto: v1.BlockStatus
  }

  object BlockStatus {
    final object Complete extends BlockStatus {
      override val isComplete: Boolean = true
      override val toProto: v1.BlockStatus =
        v1.BlockStatus(v1.BlockStatus.Status.Complete(com.google.protobuf.empty.Empty()))
    }
    final case class InProgress(
        prePrepared: Boolean,
        preparesPresent: Seq[Boolean],
        commitsPresent: Seq[Boolean],
    ) extends BlockStatus {
      override def isComplete: Boolean = false
      override def toProto: v1.BlockStatus = v1.BlockStatus(
        v1.BlockStatus.Status.InProgress(
          v1.BlockInProgress(prePrepared, preparesPresent, commitsPresent)
        )
      )
    }

    private[modules] def fromProto(proto: v1.BlockStatus): ParsingResult[BlockStatus] =
      proto.status match {
        case v1.BlockStatus.Status.InProgress(
              v1.BlockInProgress(prePrepared, preparesPresent, commitsPresent)
            ) =>
          Right(BlockStatus.InProgress(prePrepared, preparesPresent, commitsPresent))
        case v1.BlockStatus.Status.Complete(_) => Right(BlockStatus.Complete)
        case v1.BlockStatus.Status.Empty =>
          Left(ProtoDeserializationError.OtherError("Empty Received"))
      }
  }

  final case class RetransmissionResult(
      messages: Seq[SignedMessage[PbftNetworkMessage]],
      commitCerts: Seq[CommitCertificate] = Seq.empty,
  )

  object RetransmissionResult {
    val empty = RetransmissionResult(Seq.empty, Seq.empty)
  }
}
