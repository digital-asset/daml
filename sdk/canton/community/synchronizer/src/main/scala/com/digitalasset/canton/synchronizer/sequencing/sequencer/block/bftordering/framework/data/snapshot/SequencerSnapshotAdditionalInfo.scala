// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.snapshot

import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.topology.SequencerId
import com.google.protobuf.ByteString

final case class SequencerSnapshotAdditionalInfo(
    peerActiveAt: Map[SequencerId, PeerActiveAt]
) {

  def toProto: v30.BftSequencerSnapshotAdditionalInfo = {
    val peerActiveAtEpochNumbersProto = peerActiveAt.view.map { case (peer, activeAt) =>
      peer.toProtoPrimitive ->
        v30.BftSequencerSnapshotAdditionalInfo.PeerActiveAt.of(
          activeAt.timestamp.map(_.value.toMicros),
          activeAt.epochNumber,
          activeAt.firstBlockNumberInEpoch,
          activeAt.pendingTopologyChangesInEpoch,
          activeAt.previousBftTime.map(_.toMicros),
        )
    }.toMap
    v30.BftSequencerSnapshotAdditionalInfo.of(peerActiveAtEpochNumbersProto)
  }
}

object SequencerSnapshotAdditionalInfo {

  def fromProto(
      byteString: ByteString
  ): ParsingResult[SequencerSnapshotAdditionalInfo] =
    for {
      proto <- ProtoConverter.protoParser(v30.BftSequencerSnapshotAdditionalInfo.parseFrom)(
        byteString
      )
      peerFirstKnownAtEpochNumbers <- proto.peersActiveAt.view
        .map { case (sequencerUidProto, firstKnownAtProto) =>
          for {
            sequencerId <- SequencerId.fromProtoPrimitive(sequencerUidProto, "sequencerUid")
            timestamp <- firstKnownAtProto.timestamp
              .map(timestamp =>
                CantonTimestamp
                  .fromProtoPrimitive(timestamp)
                  .map(TopologyActivationTime(_))
                  .map(Some(_))
              )
              .getOrElse(Right(None))
            epochNumber = firstKnownAtProto.epochNumber.map(EpochNumber(_))
            firstBlockNumberInEpoch = firstKnownAtProto.firstBlockNumberInEpoch.map(BlockNumber(_))
            previousBftTime <- firstKnownAtProto.previousBftTime
              .map(time => CantonTimestamp.fromProtoPrimitive(time).map(Some(_)))
              .getOrElse(Right(None))
          } yield sequencerId -> PeerActiveAt(
            timestamp,
            epochNumber,
            firstBlockNumberInEpoch,
            firstKnownAtProto.pendingTopologyChangesInEpoch,
            previousBftTime,
          )
        }
        .toSeq
        .sequence
    } yield SequencerSnapshotAdditionalInfo(peerFirstKnownAtEpochNumbers.toMap)
}

final case class PeerActiveAt(
    timestamp: Option[TopologyActivationTime],
    epochNumber: Option[EpochNumber],
    firstBlockNumberInEpoch: Option[BlockNumber],
    pendingTopologyChangesInEpoch: Option[Boolean],
    previousBftTime: Option[CantonTimestamp],
)
