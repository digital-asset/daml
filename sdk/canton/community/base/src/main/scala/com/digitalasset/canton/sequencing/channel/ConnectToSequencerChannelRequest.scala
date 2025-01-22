// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.channel

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.AsymmetricEncrypted
import com.digitalasset.canton.protocol.ProtocolSymmetricKey
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencer.api.v30.ConnectToSequencerChannelRequest.Request as v30_ChannelRequest
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelId,
  SequencerChannelMetadata,
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyAck,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

final case class ConnectToSequencerChannelRequest(
    request: ConnectToSequencerChannelRequest.Request,
    traceContext: TraceContext,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ConnectToSequencerChannelRequest.type
    ]
) extends HasProtocolVersionedWrapper[ConnectToSequencerChannelRequest] {
  @transient override protected lazy val companionObj: ConnectToSequencerChannelRequest.type =
    ConnectToSequencerChannelRequest

  def toProtoV30: v30.ConnectToSequencerChannelRequest =
    v30.ConnectToSequencerChannelRequest(
      request.toProtoV30,
      Some(SerializableTraceContext(traceContext).toProtoV30),
    )
}

object ConnectToSequencerChannelRequest
    extends HasProtocolVersionedCompanion[ConnectToSequencerChannelRequest] {
  override val name: String = "ConnectToSequencerChannelRequest"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.dev)(
      v30.ConnectToSequencerChannelRequest
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  sealed trait Request {
    def toProtoV30: v30_ChannelRequest
  }
  final case class Metadata(metadata: SequencerChannelMetadata) extends Request {
    def toProtoV30: v30_ChannelRequest =
      v30_ChannelRequest.Metadata(
        metadata.toProtoV30
      )
  }
  final case class SessionKey(sessionKey: SequencerChannelSessionKey) extends Request {
    def toProtoV30: v30_ChannelRequest =
      v30_ChannelRequest.SessionKey(sessionKey.toProtoV30)

  }
  final case class SessionKeyAck(ack: SequencerChannelSessionKeyAck) extends Request {
    def toProtoV30: v30_ChannelRequest =
      v30_ChannelRequest.SessionKeyAcknowledgement(
        ack.toProtoV30
      )
  }
  final case class Payload(payload: ByteString) extends Request {
    def toProtoV30: v30_ChannelRequest =
      v30_ChannelRequest.Payload(
        payload
      )
  }
  object Request {
    def fromProtoV30(
        request: v30_ChannelRequest
    ): ParsingResult[Request] =
      request match {
        case v30_ChannelRequest.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("request"))
        case v30_ChannelRequest.Metadata(metadataP) =>
          SequencerChannelMetadata.fromProtoV30(metadataP).map(Metadata.apply)
        case v30_ChannelRequest.SessionKey(keyP) =>
          SequencerChannelSessionKey.fromProtoV30(keyP).map(SessionKey.apply)
        case v30_ChannelRequest.SessionKeyAcknowledgement(encrypted) =>
          SequencerChannelSessionKeyAck.fromProtoV30(encrypted).map(SessionKeyAck.apply)
        case v30_ChannelRequest.Payload(payload) =>
          Right(Payload(payload))
      }
  }

  def apply(
      request: Request,
      traceContext: TraceContext,
      protocolVersion: ProtocolVersion,
  ): ConnectToSequencerChannelRequest =
    ConnectToSequencerChannelRequest(request, traceContext)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def metadata(
      channelId: SequencerChannelId,
      initiatingMember: Member,
      connectToMember: Member,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): ConnectToSequencerChannelRequest =
    ConnectToSequencerChannelRequest(
      Metadata(
        SequencerChannelMetadata(channelId, initiatingMember, connectToMember)(
          SequencerChannelMetadata.protocolVersionRepresentativeFor(protocolVersion)
        )
      ),
      traceContext,
    )(
      ConnectToSequencerChannelRequest.protocolVersionRepresentativeFor(protocolVersion)
    )

  def payload(payload: ByteString, protocolVersion: ProtocolVersion)(implicit
      traceContext: TraceContext
  ): ConnectToSequencerChannelRequest =
    ConnectToSequencerChannelRequest(Payload(payload), traceContext)(
      ConnectToSequencerChannelRequest.protocolVersionRepresentativeFor(protocolVersion)
    )

  def sessionKey(key: AsymmetricEncrypted[ProtocolSymmetricKey], protocolVersion: ProtocolVersion)(
      implicit traceContext: TraceContext
  ): ConnectToSequencerChannelRequest =
    ConnectToSequencerChannelRequest(
      SessionKey(SequencerChannelSessionKey(key, protocolVersion)),
      traceContext,
    )(ConnectToSequencerChannelRequest.protocolVersionRepresentativeFor(protocolVersion))

  def sessionKeyAck(protocolVersion: ProtocolVersion)(implicit
      traceContext: TraceContext
  ): ConnectToSequencerChannelRequest =
    ConnectToSequencerChannelRequest(
      SessionKeyAck(SequencerChannelSessionKeyAck(protocolVersion)),
      traceContext,
    )(ConnectToSequencerChannelRequest.protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV30(
      connectRequest: v30.ConnectToSequencerChannelRequest
  ): ParsingResult[ConnectToSequencerChannelRequest] = {
    val v30.ConnectToSequencerChannelRequest(requestP, traceContextP) = connectRequest
    for {
      request <- Request.fromProtoV30(requestP)
      traceContext <- SerializableTraceContext.fromProtoV30Opt(traceContextP).map(_.unwrap)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield ConnectToSequencerChannelRequest(request, traceContext)(rpv)
  }
}
