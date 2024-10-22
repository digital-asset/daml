// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.snapshot

import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.google.protobuf.ByteString

final case class SequencerSnapshotAdditionalInfo(
    peerFirstKnownAt: Map[SequencerId, FirstKnownAt]
) {

  def toProto: v30.BftSequencerSnapshotAdditionalInfo = {
    val peerFirstKnownAtEpochNumbersProto = peerFirstKnownAt.view.map { case (peer, firstKnownAt) =>
      peer.toProtoPrimitive ->
        v30.BftSequencerSnapshotAdditionalInfo.FirstKnownAt.of(
          firstKnownAt.timestamp.map(_.value.toMicros),
          firstKnownAt.epochNumber,
          firstKnownAt.firstBlockNumberInEpoch,
          firstKnownAt.previousBftTime.map(_.toMicros),
        )
    }.toMap
    v30.BftSequencerSnapshotAdditionalInfo.of(peerFirstKnownAtEpochNumbersProto)
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
      peerFirstKnownAtEpochNumbers <- proto.sequencersFirstKnownAt.view
        .map { case (sequencerUidProto, firstKnownAtProto) =>
          for {
            sequencerId <- SequencerId.fromProtoPrimitive(sequencerUidProto, "sequencerUid")
            timestamp <- firstKnownAtProto.timestamp
              .map(timestamp =>
                CantonTimestamp.fromProtoPrimitive(timestamp).map(EffectiveTime(_)).map(Some(_))
              )
              .getOrElse(Right(None))
            epochNumber = firstKnownAtProto.epochNumber.map(EpochNumber(_))
            firstBlockNumberInEpoch = firstKnownAtProto.firstBlockNumberInEpoch.map(BlockNumber(_))
            previousBftTime <- firstKnownAtProto.previousBftTime
              .map(time => CantonTimestamp.fromProtoPrimitive(time).map(Some(_)))
              .getOrElse(Right(None))
          } yield sequencerId -> FirstKnownAt(
            timestamp,
            epochNumber,
            firstBlockNumberInEpoch,
            previousBftTime,
          )
        }
        .toSeq
        .sequence
    } yield SequencerSnapshotAdditionalInfo(peerFirstKnownAtEpochNumbers.toMap)
}

final case class FirstKnownAt(
    timestamp: Option[EffectiveTime],
    epochNumber: Option[EpochNumber],
    firstBlockNumberInEpoch: Option[BlockNumber],
    previousBftTime: Option[CantonTimestamp],
)
