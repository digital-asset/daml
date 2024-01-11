// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

sealed trait HandshakeResponse {
  val serverProtocolVersion: ProtocolVersion

  def toProtoV0: v0.Handshake.Response
}

object HandshakeResponse {
  final case class Success(serverProtocolVersion: ProtocolVersion) extends HandshakeResponse {
    override def toProtoV0: v0.Handshake.Response =
      v0.Handshake.Response(
        serverProtocolVersion.toProtoPrimitiveS,
        v0.Handshake.Response.Value.Success(v0.Handshake.Success()),
      )
  }
  final case class Failure(serverProtocolVersion: ProtocolVersion, reason: String)
      extends HandshakeResponse {
    override def toProtoV0: v0.Handshake.Response =
      v0.Handshake
        .Response(
          serverProtocolVersion.toProtoPrimitiveS,
          v0.Handshake.Response.Value.Failure(v0.Handshake.Failure(reason)),
        )
  }

  def fromProtoV0(
      responseP: v0.Handshake.Response
  ): ParsingResult[HandshakeResponse] =
    responseP.value match {
      case v0.Handshake.Response.Value.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("Handshake.Response.value"))
      case v0.Handshake.Response.Value.Success(_success) =>
        ProtocolVersion.fromProtoPrimitiveS(responseP.serverProtocolVersion).map(Success)
      case v0.Handshake.Response.Value.Failure(failure) =>
        ProtocolVersion
          .fromProtoPrimitiveS(responseP.serverProtocolVersion)
          .map(Failure(_, failure.reason))
    }
}
