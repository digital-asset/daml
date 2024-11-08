// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.channel

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.sequencing.protocol.SequencerChannelConnectedToAllEndpoints
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

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
      _.toProtoV30.toByteString,
    ),
  )

  sealed trait Response {
    def toProtoV30: v30.ConnectToSequencerChannelResponse.Response
  }
  final case object Connected extends Response {
    def toProtoV30: v30.ConnectToSequencerChannelResponse.Response =
      v30.ConnectToSequencerChannelResponse.Response.Connected(
        v30.SequencerChannelConnectedToAllEndpoints()
      )
  }
  final case class Payload(payload: ByteString) extends Response {
    def toProtoV30: v30.ConnectToSequencerChannelResponse.Response =
      v30.ConnectToSequencerChannelResponse.Response.Payload(
        payload
      )
  }
  object Response {
    def fromProtoV30(
        response: v30.ConnectToSequencerChannelResponse.Response
    ): ParsingResult[Response] =
      response match {
        case v30.ConnectToSequencerChannelResponse.Response.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("response"))
        case v30.ConnectToSequencerChannelResponse.Response.Connected(connected) =>
          SequencerChannelConnectedToAllEndpoints.fromProtoV30(connected).map(_ => Connected)
        case v30.ConnectToSequencerChannelResponse.Response.Payload(payload) =>
          Right(Payload(payload))
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
