// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot

import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.topology.SequencerId
import com.google.protobuf.ByteString

final case class SequencerSnapshotAdditionalInfo(
    peerActiveAt: Map[SequencerId, PeerActiveAt]
) {

  def toProto30: v30.BftSequencerSnapshotAdditionalInfo = {
    val peerActiveAtEpochNumbersProto = peerActiveAt.view.map { case (peer, activeAt) =>
      peer.toProtoPrimitive ->
        v30.BftSequencerSnapshotAdditionalInfo.PeerActiveAt.of(
          activeAt.timestamp.value.toMicros,
          activeAt.epochNumber,
          activeAt.firstBlockNumberInEpoch,
          activeAt.epochTopologyQueryTimestamp.map(_.value.toMicros),
          activeAt.epochCouldAlterOrderingTopology,
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
            timestamp <- CantonTimestamp
              .fromProtoPrimitive(firstKnownAtProto.timestamp)
              .map(TopologyActivationTime(_))
            epochNumber = firstKnownAtProto.epochNumber.map(EpochNumber(_))
            firstBlockNumberInEpoch = firstKnownAtProto.firstBlockNumberInEpoch.map(BlockNumber(_))
            epochTopologyQueryTimestamp <- firstKnownAtProto.epochTopologyQueryTimestamp
              .map(time =>
                CantonTimestamp.fromProtoPrimitive(time).map(TopologyActivationTime(_)).map(Some(_))
              )
              .getOrElse(Right(None))
            previousBftTime <- firstKnownAtProto.previousBftTime
              .map(time => CantonTimestamp.fromProtoPrimitive(time).map(Some(_)))
              .getOrElse(Right(None))
          } yield sequencerId -> PeerActiveAt(
            timestamp,
            epochNumber,
            firstBlockNumberInEpoch,
            epochTopologyQueryTimestamp,
            firstKnownAtProto.epochCouldAlterOrderingTopology,
            previousBftTime,
          )
        }
        .toSeq
        .sequence
    } yield SequencerSnapshotAdditionalInfo(peerFirstKnownAtEpochNumbers.toMap)
}

final case class PeerActiveAt(
    timestamp: TopologyActivationTime,
    epochNumber: Option[EpochNumber],
    firstBlockNumberInEpoch: Option[BlockNumber],
    epochTopologyQueryTimestamp: Option[TopologyActivationTime],
    epochCouldAlterOrderingTopology: Option[Boolean],
    previousBftTime: Option[CantonTimestamp],
)
