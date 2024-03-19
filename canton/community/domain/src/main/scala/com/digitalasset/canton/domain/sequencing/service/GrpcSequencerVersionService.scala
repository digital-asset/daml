// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencer.admin.v30.SequencerVersion
import com.digitalasset.canton.sequencer.admin.v30.SequencerVersionServiceGrpc.SequencerVersionService
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.Future

class GrpcSequencerVersionService(
    protected val serverProtocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
) extends SequencerVersionService
    with GrpcHandshakeService
    with NamedLogging {

  override def handshake(
      request: SequencerVersion.HandshakeRequest
  ): Future[SequencerVersion.HandshakeResponse] =
    Future.successful {
      SequencerVersion.HandshakeResponse(
        Some(
          handshake(
            request.getHandshakeRequest.clientProtocolVersions,
            request.getHandshakeRequest.minimumProtocolVersion,
          )
        )
      )
    }

}
