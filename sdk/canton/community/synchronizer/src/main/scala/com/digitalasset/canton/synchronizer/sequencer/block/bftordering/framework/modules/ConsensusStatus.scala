// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30

/** Status messages that describe how far into the consensus process a node is. This is used as part
  * of retransmissions such that receiving nodes can tell if there are messages they can retransmit
  * in order to help the originating node make progress.
  */
object ConsensusStatus {

  final case class EpochStatus(
      from: BftNodeId,
      epochNumber: EpochNumber,
      segments: Seq[SegmentStatus],
  ) {
    def toProto: v30.EpochStatus = v30.EpochStatus(epochNumber, segments.map(_.toProto))
  }

  object EpochStatus {
    def fromProto(
        from: BftNodeId,
        protoEpochStatus: v30.EpochStatus,
    ): ParsingResult[EpochStatus] =
      for {
        segments <- protoEpochStatus.segments.traverse(SegmentStatus.fromProto)
      } yield EpochStatus(
        from,
        EpochNumber(protoEpochStatus.epochNumber),
        segments,
      )
  }

  sealed trait SegmentStatus {
    def toProto: v30.SegmentStatus
  }

  object SegmentStatus {
    final object Complete extends SegmentStatus {
      override val toProto: v30.SegmentStatus =
        v30.SegmentStatus(v30.SegmentStatus.Status.Complete(com.google.protobuf.empty.Empty()))
    }
    sealed trait Incomplete extends SegmentStatus {
      def viewNumber: ViewNumber
      def areBlocksComplete: Seq[Boolean]
    }

    final case class InProgress(viewNumber: ViewNumber, blockStatuses: Seq[BlockStatus])
        extends Incomplete {
      override def areBlocksComplete: Seq[Boolean] = blockStatuses.map(_.isComplete)
      override def toProto: v30.SegmentStatus =
        v30.SegmentStatus(
          v30.SegmentStatus.Status.InProgress(
            v30.SegmentInProgress(viewNumber, blockStatuses.map(_.toProto))
          )
        )
    }
    final case class InViewChange(
        viewNumber: ViewNumber,
        viewChangeMessagesPresent: Seq[Boolean],
        areBlocksComplete: Seq[Boolean],
    ) extends Incomplete {
      override def toProto: v30.SegmentStatus = v30.SegmentStatus(
        v30.SegmentStatus.Status.InViewChange(
          v30.SegmentInViewChange(viewNumber, viewChangeMessagesPresent, areBlocksComplete)
        )
      )
    }

    private[modules] def fromProto(proto: v30.SegmentStatus): ParsingResult[SegmentStatus] =
      proto.status match {
        case v30.SegmentStatus.Status.InViewChange(
              v30.SegmentInViewChange(viewChange, viewChangeMessagesPresent, areBlocksComplete)
            ) =>
          Right(
            SegmentStatus
              .InViewChange(ViewNumber(viewChange), viewChangeMessagesPresent, areBlocksComplete)
          )
        case v30.SegmentStatus.Status
              .InProgress(v30.SegmentInProgress(viewNumber, blockStatuses)) =>
          for {
            blocks <- blockStatuses.traverse(BlockStatus.fromProto)
          } yield SegmentStatus.InProgress(
            ViewNumber(viewNumber),
            blocks,
          )
        case v30.SegmentStatus.Status.Complete(_) => Right(SegmentStatus.Complete)
        case v30.SegmentStatus.Status.Empty =>
          Left(ProtoDeserializationError.OtherError("Empty Received"))
      }

  }

  sealed trait BlockStatus {
    def isComplete: Boolean
    def toProto: v30.BlockStatus
  }

  object BlockStatus {
    final object Complete extends BlockStatus {
      override val isComplete: Boolean = true
      override val toProto: v30.BlockStatus =
        v30.BlockStatus(v30.BlockStatus.Status.Complete(com.google.protobuf.empty.Empty()))
    }
    final case class InProgress(
        prePrepared: Boolean,
        preparesPresent: Seq[Boolean],
        commitsPresent: Seq[Boolean],
    ) extends BlockStatus {
      override def isComplete: Boolean = false
      override def toProto: v30.BlockStatus = v30.BlockStatus(
        v30.BlockStatus.Status.InProgress(
          v30.BlockInProgress(prePrepared, preparesPresent, commitsPresent)
        )
      )
    }

    private[modules] def fromProto(proto: v30.BlockStatus): ParsingResult[BlockStatus] =
      proto.status match {
        case v30.BlockStatus.Status.InProgress(
              v30.BlockInProgress(prePrepared, preparesPresent, commitsPresent)
            ) =>
          Right(BlockStatus.InProgress(prePrepared, preparesPresent, commitsPresent))
        case v30.BlockStatus.Status.Complete(_) => Right(BlockStatus.Complete)
        case v30.BlockStatus.Status.Empty =>
          Left(ProtoDeserializationError.OtherError("Empty Received"))
      }
  }
}
