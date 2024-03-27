// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

final case class HandshakeRequest(
    clientProtocolVersions: Seq[ProtocolVersion],
    minimumProtocolVersion: Option[ProtocolVersion],
) {

  // IMPORTANT: changing the version handshakes can lead to issues with upgrading domains - be very careful
  // when changing the handshake message format
  def toProtoV30: v30.Handshake.Request =
    v30.Handshake.Request(
      clientProtocolVersions.map(_.toProtoPrimitiveS),
      minimumProtocolVersion.map(_.toProtoPrimitiveS),
    )

  // IMPORTANT: changing the version handshakes can lead to issues with upgrading domains - be very careful
  // when changing the handshake message format
  def toByteArrayV0: Array[Byte] = toProtoV30.toByteArray

}

object HandshakeRequest {
  def fromProtoV30(
      requestP: v30.Handshake.Request
  ): ParsingResult[HandshakeRequest] =
    for {
      clientProtocolVersions <- requestP.clientProtocolVersions.traverse(version =>
        ProtocolVersion.fromProtoPrimitiveHandshake(version)
      )
      minimumProtocolVersion <- requestP.minimumProtocolVersion.traverse(
        ProtocolVersion.fromProtoPrimitiveHandshake
      )
    } yield HandshakeRequest(clientProtocolVersions, minimumProtocolVersion)
}
