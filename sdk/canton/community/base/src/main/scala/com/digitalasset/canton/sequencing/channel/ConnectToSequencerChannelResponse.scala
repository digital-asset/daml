// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.channel

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelConnectedToAllEndpoints,
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyAck,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import v30.ConnectToSequencerChannelResponse.Response as v30_ChannelResponse

final case class ConnectToSequencerChannelResponse(
    response: ConnectToSequencerChannelResponse.Response,
    traceContext: TraceContext,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ConnectToSequencerChannelResponse.type
    ]
) extends HasProtocolVersionedWrapper[ConnectToSequencerChannelResponse] {
  @transient override protected lazy val companionObj: ConnectToSequencerChannelResponse.type =
    ConnectToSequencerChannelResponse

  def toProtoV30: v30.ConnectToSequencerChannelResponse =
    v30.ConnectToSequencerChannelResponse(
      response.toProtoV30,
      Some(SerializableTraceContext(traceContext).toProtoV30),
    )
}

object ConnectToSequencerChannelResponse
    extends HasProtocolVersionedCompanion[ConnectToSequencerChannelResponse] {
  override val name: String = "ConnectToSequencerChannelResponse"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.dev)(
      v30.ConnectToSequencerChannelResponse
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  sealed trait Response {
    def toProtoV30: v30_ChannelResponse
  }
  final case object Connected extends Response {
    def toProtoV30: v30_ChannelResponse =
      v30_ChannelResponse.Connected(
        v30.SequencerChannelConnectedToAllEndpoints()
      )
  }
  final case class SessionKey(sessionKey: SequencerChannelSessionKey) extends Response {
    def toProtoV30: v30_ChannelResponse =
      v30_ChannelResponse.SessionKey(sessionKey.toProtoV30)

  }
  final case object SessionKeyAck extends Response {
    def toProtoV30: v30_ChannelResponse =
      v30_ChannelResponse.SessionKeyAcknowledgement(
        v30.SequencerChannelSessionKeyAck()
      )
  }
  final case class Payload(payload: ByteString) extends Response {
    def toProtoV30: v30_ChannelResponse =
      v30_ChannelResponse.Payload(
        payload
      )
  }
  object Response {
    def fromProtoV30(
        response: v30_ChannelResponse
    ): ParsingResult[Response] =
      response match {
        case v30_ChannelResponse.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("response"))
        case v30_ChannelResponse.Connected(connectedP) =>
          SequencerChannelConnectedToAllEndpoints.fromProtoV30(connectedP).map(_ => Connected)
        case v30_ChannelResponse.SessionKey(keyP) =>
          SequencerChannelSessionKey.fromProtoV30(keyP).map(SessionKey.apply)
        case v30_ChannelResponse.SessionKeyAcknowledgement(keyAckP) =>
          SequencerChannelSessionKeyAck.fromProtoV30(keyAckP).map(_ => SessionKeyAck)
        case v30_ChannelResponse.Payload(payloadP) =>
          Right(Payload(payloadP))
      }
  }

  def apply(
      response: Response,
      traceContext: TraceContext,
      protocolVersion: ProtocolVersion,
  ): ConnectToSequencerChannelResponse =
    ConnectToSequencerChannelResponse(response, traceContext)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def connectedToAllEndpoints(protocolVersion: ProtocolVersion)(implicit
      traceContext: TraceContext
  ): ConnectToSequencerChannelResponse =
    ConnectToSequencerChannelResponse(Connected, traceContext)(
      ConnectToSequencerChannelResponse.protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      requestP: v30.ConnectToSequencerChannelResponse
  ): ParsingResult[ConnectToSequencerChannelResponse] = {
    val v30.ConnectToSequencerChannelResponse(responseP, traceContextP) = requestP
    for {
      response <- Response.fromProtoV30(responseP)
      traceContext <- SerializableTraceContext.fromProtoV30Opt(traceContextP).map(_.unwrap)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield ConnectToSequencerChannelResponse(response, traceContext)(rpv)
  }
}
