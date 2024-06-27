// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent
import com.digitalasset.canton.domain.sequencing.sequencer.OrderingRequest
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.{
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
      originalPayloadSize: Int =
        0, // default is 0 for testing as this value is only used for metrics
  ) extends LedgerBlockEvent {
    lazy val signedSubmissionRequest = signedOrderingRequest.signedSubmissionRequest
  }
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
        } yield LedgerBlockEvent.Send(CantonTimestamp(timestamp), deserializedRequest, request.size)
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
      // This is the SignedContent signed by the submitting sequencer (so SignedContent[OrderingRequest[SignedContent[SubmissionRequest]]])
      // See explanation diagram in the [[Sequencer]] companion object
      sequencerSignedContent <- SignedContent
        .fromByteString(protocolVersion)(submissionRequestBytes)
      signedOrderingRequest <- sequencerSignedContent
        .deserializeContent(
          // Now we deserialize the first inner content as an OrderingRequest[_]
          (OrderingRequest.fromByteString(protocolVersion) _)
            .andThen(
              // Then deserialize the ordering request's content as a SignedContent[_] (signed by the sending participant this time)
              _.flatMap(
                _.deserializeContent(
                  (SignedContent.fromByteString(protocolVersion) _)
                    // And then deserialize the final inner content as SubmissionRequest
                    .andThen(
                      _.flatMap(
                        _.deserializeContent(
                          // ... and we're done!
                          SubmissionRequest.fromByteString(protocolVersion)(
                            MaxRequestSizeToDeserialize.NoLimit
                          )
                        )
                      )
                    )
                )
              )
            )
        )
    } yield signedOrderingRequest
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
