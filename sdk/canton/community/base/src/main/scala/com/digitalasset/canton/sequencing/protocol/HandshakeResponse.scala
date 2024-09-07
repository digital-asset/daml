// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

sealed trait HandshakeResponse {
  val serverProtocolVersion: ProtocolVersion
}

object HandshakeResponse {
  final case class Success(serverProtocolVersion: ProtocolVersion) extends HandshakeResponse
  final case class Failure(serverProtocolVersion: ProtocolVersion, reason: String)
      extends HandshakeResponse

  def fromProtoV30(
      responseP: v30.SequencerConnect.HandshakeResponse
  ): ParsingResult[HandshakeResponse] =
    responseP.value match {
      case v30.SequencerConnect.HandshakeResponse.Value.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("HandshakeResponse.value"))
      case v30.SequencerConnect.HandshakeResponse.Value.Success(_success) =>
        ProtocolVersion.fromProtoPrimitive(responseP.serverProtocolVersion).map(Success)
      case v30.SequencerConnect.HandshakeResponse.Value.Failure(failure) =>
        ProtocolVersion
          .fromProtoPrimitive(responseP.serverProtocolVersion)
          .map(Failure(_, failure.reason))
    }
}
