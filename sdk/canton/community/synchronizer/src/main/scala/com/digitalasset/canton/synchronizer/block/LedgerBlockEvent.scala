// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block

import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.HasLoggerName
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  MaxRequestSizeToDeserialize,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfTimestamp, ProtoDeserializationError}
import com.google.protobuf.ByteString

import RawLedgerBlock.RawBlockEvent

/** Trait that generalizes over the kind of events that could be observed from a
  * [[com.digitalasset.canton.synchronizer.sequencer.block.BlockOrderer]].
  */
sealed trait LedgerBlockEvent extends Product with Serializable {
  def timestamp: CantonTimestamp
}

object LedgerBlockEvent extends HasLoggerName {

  final case class Send(
      timestamp: CantonTimestamp,
      signedSubmissionRequest: SignedSubmissionRequest,
      orderingSequencerId: SequencerId,
      originalPayloadSize: Int =
        0, // default is 0 for testing as this value is only used for metrics
  ) extends LedgerBlockEvent
  final case class Acknowledgment(
      timestamp: CantonTimestamp,
      request: SignedContent[AcknowledgeRequest],
  ) extends LedgerBlockEvent

  def fromRawBlockEvent(
      protocolVersion: ProtocolVersion,
      maxRequestSizeToDeserialize: MaxRequestSizeToDeserialize,
  )(blockEvent: RawBlockEvent): ParsingResult[LedgerBlockEvent] =
    blockEvent match {
      case RawBlockEvent.Send(request, microsecondsSinceEpoch, orderingSequencerId) =>
        for {
          sequencerId <- SequencerId.fromProtoPrimitive(
            orderingSequencerId,
            "orderingSequencerId",
          )
          deserializedRequest <-
            deserializeSignedSubmissionRequest(
              protocolVersion,
              maxRequestSizeToDeserialize,
            )(request)
          timestamp <-
            LfTimestamp
              .fromLong(microsecondsSinceEpoch)
              .leftMap(e => ProtoDeserializationError.TimestampConversionError(e))
        } yield LedgerBlockEvent.Send(
          CantonTimestamp(timestamp),
          deserializedRequest,
          orderingSequencerId = sequencerId,
          request.size,
        )
      case RawBlockEvent.Acknowledgment(acknowledgement, microsecondsSinceEpoch) =>
        for {
          deserializedRequest <- deserializeSignedAcknowledgeRequest(protocolVersion)(
            acknowledgement
          )
          timestamp <-
            LfTimestamp
              .fromLong(microsecondsSinceEpoch)
              .leftMap(e => ProtoDeserializationError.TimestampConversionError(e))

        } yield LedgerBlockEvent.Acknowledgment(CantonTimestamp(timestamp), deserializedRequest)

    }

  def deserializeSignedSubmissionRequest(
      protocolVersion: ProtocolVersion,
      maxRequestSizeToDeserialize: MaxRequestSizeToDeserialize,
  )(submissionRequestBytes: ByteString): ParsingResult[SignedSubmissionRequest] =
    SignedContent
      .fromByteString(protocolVersion, submissionRequestBytes)
      .flatMap(
        _.deserializeContent(
          SubmissionRequest.fromByteString(protocolVersion, maxRequestSizeToDeserialize)
        )
      )

  private def deserializeSignedAcknowledgeRequest(protocolVersion: ProtocolVersion)(
      ackRequestBytes: ByteString
  ): ParsingResult[SignedContent[AcknowledgeRequest]] =
    SignedContent
      .fromByteString(protocolVersion, ackRequestBytes)
      .flatMap(
        _.deserializeContent(AcknowledgeRequest.fromByteString(protocolVersion, _))
      )
}

/** @param tickTopologyAtLeastAt
  *   See [[RawLedgerBlock.tickTopologyAtMicrosFromEpoch]]
  */
final case class BlockEvents(
    height: Long,
    events: Seq[Traced[LedgerBlockEvent]],
    tickTopologyAtLeastAt: Option[CantonTimestamp] = None,
)
