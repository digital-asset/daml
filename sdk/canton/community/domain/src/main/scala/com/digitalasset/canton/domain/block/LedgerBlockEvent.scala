// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.{
  SenderSigned,
  SignedOrderingRequest,
  SignedOrderingRequestOps,
}
import com.digitalasset.canton.logging.HasLoggerName
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  MaxRequestSizeToDeserialize,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  BytestringWithCryptographicEvidence,
  HasCryptographicEvidence,
}
import com.digitalasset.canton.topology.Member
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
      signedOrderingRequest: SignedOrderingRequest,
  ) extends LedgerBlockEvent {
    lazy val signedSubmissionRequest = signedOrderingRequest.signedSubmissionRequest
  }
  final case class AddMember(member: Member) extends LedgerBlockEvent
  final case class Acknowledgment(request: SignedContent[AcknowledgeRequest])
      extends LedgerBlockEvent

  def fromRawBlockEvent(
      protocolVersion: ProtocolVersion
  )(blockEvent: RawBlockEvent): ParsingResult[LedgerBlockEvent] =
    blockEvent match {
      case RawBlockEvent.Send(request, microsecondsSinceEpoch) =>
        for {
          deserializedRequest <- deserializeSignedOrderingRequest(protocolVersion)(request)
          timestamp <- LfTimestamp
            .fromLong(microsecondsSinceEpoch)
            .leftMap(e => ProtoDeserializationError.TimestampConversionError(e))
        } yield LedgerBlockEvent.Send(CantonTimestamp(timestamp), deserializedRequest)
      case RawBlockEvent.AddMember(memberString) =>
        Member
          .fromProtoPrimitive_(memberString)
          .bimap(ProtoDeserializationError.StringConversionError, LedgerBlockEvent.AddMember)
      case RawBlockEvent.Acknowledgment(acknowledgement) =>
        deserializeSignedAcknowledgeRequest(protocolVersion)(acknowledgement).map(
          LedgerBlockEvent.Acknowledgment
        )
    }

  def deserializeSignedOrderingRequest(
      protocolVersion: ProtocolVersion
  )(submissionRequestBytes: ByteString): ParsingResult[SignedOrderingRequest] = {

    // TODO(i10428) Prevent zip bombing when decompressing the request
    for {
      sequencerSignedContent <- SignedContent
        .fromByteString(protocolVersion)(submissionRequestBytes)
      signedOrderingRequest <- sequencerSignedContent
        .deserializeContent(
          deserializeOrderingRequestToValueClass(protocolVersion)
            .andThen(deserializeOrderingRequestSignedContent(protocolVersion))
        )
    } yield signedOrderingRequest
  }
  private def deserializeOrderingRequestToValueClass(
      protocolVersion: ProtocolVersion
  ): ByteString => ParsingResult[SignedContent[BytestringWithCryptographicEvidence]] =
    SignedContent.fromByteString(protocolVersion)

  private def deserializeOrderingRequestSignedContent(protocolVersion: ProtocolVersion)(
      signedContentParsingResult: ParsingResult[SignedContent[BytestringWithCryptographicEvidence]]
  ): ParsingResult[SenderSigned[SubmissionRequest]] = for {
    signedContent <- signedContentParsingResult
    senderSignedRequest <- deserializeSenderSignedSubmissionRequest(protocolVersion)(signedContent)
  } yield senderSignedRequest

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
