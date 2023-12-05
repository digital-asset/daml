// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

final case class HandshakeRequest(
    clientProtocolVersions: Seq[ProtocolVersion],
    minimumProtocolVersion: Option[ProtocolVersion],
) {

  // IMPORTANT: changing the version handshakes can lead to issues with upgrading domains - be very careful
  // when changing the handshake message format
  def toProtoV0: v0.Handshake.Request =
    v0.Handshake.Request(
      clientProtocolVersions.map(_.toProtoPrimitiveS),
      minimumProtocolVersion.map(_.toProtoPrimitiveS),
    )

  // IMPORTANT: changing the version handshakes can lead to issues with upgrading domains - be very careful
  // when changing the handshake message format
  def toByteArrayV0: Array[Byte] = toProtoV0.toByteArray

}

object HandshakeRequest {
  def fromProtoV0(
      requestP: v0.Handshake.Request
  ): ParsingResult[HandshakeRequest] =
    for {
      clientProtocolVersions <- requestP.clientProtocolVersions.traverse(version =>
        ProtocolVersion.fromProtoPrimitiveS(version)
      )
      minimumProtocolVersion <- requestP.minimumProtocolVersion.traverse(
        ProtocolVersion.fromProtoPrimitiveS(_)
      )
    } yield HandshakeRequest(clientProtocolVersions, minimumProtocolVersion)
}
