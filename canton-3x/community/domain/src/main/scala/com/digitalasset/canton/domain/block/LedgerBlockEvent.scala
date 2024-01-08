// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent
import com.digitalasset.canton.domain.sequencing.sequencer.EthereumAccount
import com.digitalasset.canton.logging.HasLoggerName
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  MaxRequestSizeToDeserialize,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
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
      signedSubmissionRequest: SignedContent[SubmissionRequest],
  ) extends LedgerBlockEvent
  final case class AddMember(member: Member) extends LedgerBlockEvent
  final case class AuthorizedAccount(account: EthereumAccount) extends LedgerBlockEvent
  final case class DisableMember(member: Member) extends LedgerBlockEvent
  final case class Acknowledgment(request: SignedContent[AcknowledgeRequest])
      extends LedgerBlockEvent
  final case class Prune(timestamp: CantonTimestamp) extends LedgerBlockEvent

  def fromRawBlockEvent(
      protocolVersion: ProtocolVersion
  )(blockEvent: RawBlockEvent): ParsingResult[LedgerBlockEvent] =
    blockEvent match {
      case RawBlockEvent.Send(request, microsecondsSinceEpoch) =>
        for {
          deserializedRequest <- deserializeSignedSubmissionRequest(protocolVersion)(request)
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

  def deserializeSignedSubmissionRequest(
      protocolVersion: ProtocolVersion
  )(submissionRequestBytes: ByteString): ParsingResult[SignedContent[SubmissionRequest]] = {

    // TODO(i10428) Prevent zip bombing when decompressing the request
    SignedContent
      .fromByteString(protocolVersion)(
        submissionRequestBytes
      )
      .flatMap(
        _.deserializeContent(
          SubmissionRequest.fromByteString(protocolVersion)(MaxRequestSizeToDeserialize.NoLimit)
        )
      )

  }

  def deserializeSignedAcknowledgeRequest(protocolVersion: ProtocolVersion)(
      ackRequestBytes: ByteString
  ): ParsingResult[SignedContent[AcknowledgeRequest]] =
    SignedContent
      .fromByteString(protocolVersion)(ackRequestBytes)
      .flatMap(
        _.deserializeContent(AcknowledgeRequest.fromByteString(protocolVersion))
      )
}

final case class BlockEvents(height: Long, events: Seq[Traced[LedgerBlockEvent]])
