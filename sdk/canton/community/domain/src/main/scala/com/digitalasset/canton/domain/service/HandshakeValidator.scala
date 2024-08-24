// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionCompatibility}

/** Class that is used to verify that a generic server and a generic client support the same protocol version.
  * In practice, this class is used for all handshakes (e.g. the participant-domain one) except the
  * sequencer client-sequencer handshake.
  */
class HandshakeValidator(
    serverVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  def clientIsCompatible(
      clientVersionsP: Seq[String],
      minClientVersionP: Option[String],
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
