// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

sealed trait HandshakeResponse {
  val serverProtocolVersion: ProtocolVersion

  def toProtoV30: v30.Handshake.Response
}

object HandshakeResponse {
  final case class Success(serverProtocolVersion: ProtocolVersion) extends HandshakeResponse {
    override def toProtoV30: v30.Handshake.Response =
      v30.Handshake.Response(
        serverProtocolVersion.toProtoPrimitiveS,
        v30.Handshake.Response.Value.Success(v30.Handshake.Success()),
      )
  }
  final case class Failure(serverProtocolVersion: ProtocolVersion, reason: String)
      extends HandshakeResponse {
    override def toProtoV30: v30.Handshake.Response =
      v30.Handshake
        .Response(
          serverProtocolVersion.toProtoPrimitiveS,
          v30.Handshake.Response.Value.Failure(v30.Handshake.Failure(reason)),
        )
  }

  def fromProtoV30(
      responseP: v30.Handshake.Response
  ): ParsingResult[HandshakeResponse] =
    responseP.value match {
      case v30.Handshake.Response.Value.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("Handshake.Response.value"))
      case v30.Handshake.Response.Value.Success(_success) =>
        ProtocolVersion.fromProtoPrimitiveHandshake(responseP.serverProtocolVersion).map(Success)
      case v30.Handshake.Response.Value.Failure(failure) =>
        ProtocolVersion
          .fromProtoPrimitiveHandshake(responseP.serverProtocolVersion)
          .map(Failure(_, failure.reason))
    }
}
