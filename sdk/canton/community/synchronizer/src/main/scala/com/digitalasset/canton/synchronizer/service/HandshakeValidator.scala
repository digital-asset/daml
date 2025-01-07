// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.service

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionCompatibility}

object HandshakeValidator {

  /** Verify that a generic server and a generic client support the same protocol version.
    * In practice, this class is used for all handshakes (e.g. the participant-domain one) except the
    * sequencer client-sequencer handshake.
    */
  def clientIsCompatible(
      serverVersion: ProtocolVersion,
      clientVersionsP: Seq[Int],
      minClientVersionP: Option[Int],
  ): Either[String, Unit] =
    for {
      // Client may mention a protocol version which is not known to the domain
      clientVersions <- clientVersionsP.traverse(ProtocolVersion.parseUnchecked)
      minClientVersion <- minClientVersionP.traverse(ProtocolVersion.parseUnchecked)

      _ <- ProtocolVersionCompatibility
        .canClientConnectToServer(clientVersions, serverVersion, minClientVersion)
        .leftMap(_.description)
    } yield ()
}
