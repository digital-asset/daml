// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.SenderSigned
import com.digitalasset.canton.logging.HasLoggerName
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  MaxRequestSizeToDeserialize,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfTimestamp, ProtoDeserializationError}
import com.google.protobuf.ByteString

/** Trait that generalizes over the kind of events that could be observed in a blockchain integration.
  *
  * Used by Ethereum and Fabric.
  */
sealed trait LedgerBlockEvent extends Product with Serializable

object LedgerBlockEvent extends HasLoggerName {

  final case class Send(
      timestamp: CantonTimestamp,
      signedSubmissionRequest: SenderSigned[SubmissionRequest],
      originalPayloadSize: Int =
        0, // default is 0 for testing as this value is only used for metrics
  ) extends LedgerBlockEvent

  final case class Acknowledgment(request: SenderSigned[AcknowledgeRequest])
      extends LedgerBlockEvent

  def fromRawBlockEvent(
      protocolVersion: ProtocolVersion
  )(blockEvent: RawBlockEvent): ParsingResult[LedgerBlockEvent] =
    blockEvent match {
      case RawBlockEvent.Send(request, microsecondsSinceEpoch) =>
        for {
          deserializedRequest <- deserializeSignedRequest(protocolVersion)(request)
          timestamp <- LfTimestamp
            .fromLong(microsecondsSinceEpoch)
            .leftMap(e => ProtoDeserializationError.TimestampConversionError(e))
        } yield LedgerBlockEvent.Send(CantonTimestamp(timestamp), deserializedRequest, request.size)
      case RawBlockEvent.Acknowledgment(acknowledgement) =>
        deserializeSignedAcknowledgeRequest(protocolVersion)(acknowledgement).map(
          LedgerBlockEvent.Acknowledgment
        )
    }

  def deserializeSignedRequest(
      protocolVersion: ProtocolVersion
  )(
      submissionRequestBytes: ByteString
  ): ParsingResult[SenderSigned[SubmissionRequest]] = {

    // TODO(i10428) Prevent zip bombing when decompressing the request
    for {
      sequencerSignedContent <- SignedContent
        .fromByteString(protocolVersion)(submissionRequestBytes)
      signedOrderingRequest <- deserializeSenderSignedSubmissionRequest(protocolVersion)(
        sequencerSignedContent
      )
    } yield signedOrderingRequest
  }

  private def deserializeSenderSignedSubmissionRequest[A <: HasCryptographicEvidence](
      protocolVersion: ProtocolVersion
  )(signedContent: SignedContent[A]): ParsingResult[SenderSigned[SubmissionRequest]] = {
    signedContent.deserializeContent(
      deserializeSubmissionRequest(protocolVersion)
    )
  }

  private def deserializeSubmissionRequest(
      protocolVersion: ProtocolVersion
  ): ByteString => ParsingResult[SubmissionRequest] = { bytes =>
    SubmissionRequest.fromByteString(protocolVersion)(
      MaxRequestSizeToDeserialize.NoLimit
    )(bytes)
  }

  private def deserializeSignedAcknowledgeRequest(protocolVersion: ProtocolVersion)(
      ackRequestBytes: ByteString
  ): ParsingResult[SignedContent[AcknowledgeRequest]] =
    SignedContent
      .fromByteString(protocolVersion)(ackRequestBytes)
      .flatMap(
        _.deserializeContent(AcknowledgeRequest.fromByteString(protocolVersion))
      )
}

final case class BlockEvents(height: Long, events: Seq[Traced[LedgerBlockEvent]])
