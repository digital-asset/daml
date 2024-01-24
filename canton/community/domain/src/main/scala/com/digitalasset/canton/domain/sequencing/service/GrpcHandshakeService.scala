// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import com.digitalasset.canton.domain.service.HandshakeValidator
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.Future

trait GrpcHandshakeService {
  protected def loggerFactory: NamedLoggerFactory
  protected def serverProtocolVersion: ProtocolVersion

  private lazy val handshakeValidator = new HandshakeValidator(serverProtocolVersion, loggerFactory)

  /** The handshake will check whether the client's version is compatible with the one of this domain.
    * This should be called before attempting to connect to the domain to make sure they can operate together.
    */
  def handshake(
      request: com.digitalasset.canton.protocol.v30.Handshake.Request
  ): Future[com.digitalasset.canton.protocol.v30.Handshake.Response] = {
    import com.digitalasset.canton.protocol.v30
    import v30.Handshake.*

    val response = handshakeValidation(request).fold[Response.Value](
      failure => Response.Value.Failure(Failure(failure)),
      _ => Response.Value.Success(Success()),
    )
    Future.successful(
      Response(serverProtocolVersion = serverProtocolVersion.toProtoPrimitiveS, response)
    )
  }

  private def handshakeValidation(
      request: com.digitalasset.canton.protocol.v30.Handshake.Request
  ): Either[String, Unit] =
    handshakeValidator.clientIsCompatible(
      request.clientProtocolVersions,
      request.minimumProtocolVersion,
    )
}
