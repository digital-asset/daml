// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import com.digitalasset.canton.domain.service.HandshakeValidator
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.version.ProtocolVersion

trait GrpcHandshakeService {
  protected def loggerFactory: NamedLoggerFactory
  protected def serverProtocolVersion: ProtocolVersion

  /** The handshake will check whether the client's version is compatible with the one of this domain.
    * This should be called before attempting to connect to the domain to make sure they can operate together.
    */
  def handshake(
      clientProtocolVersions: Seq[String],
      minimumProtocolVersion: Option[String],
  ): com.digitalasset.canton.protocol.v30.Handshake.Response = {
    import com.digitalasset.canton.protocol.v30
    import v30.Handshake.*

    val response = HandshakeValidator
      .clientIsCompatible(
        serverProtocolVersion,
        clientProtocolVersions,
        minimumProtocolVersion,
      )
      .fold[Response.Value](
        failure => Response.Value.Failure(Failure(failure)),
        _ => Response.Value.Success(Success()),
      )
    Response(serverProtocolVersion = serverProtocolVersion.toProtoPrimitiveS, response)
  }

}
