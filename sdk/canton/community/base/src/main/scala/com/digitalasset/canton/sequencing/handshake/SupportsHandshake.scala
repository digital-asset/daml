// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handshake

import cats.data.EitherT
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Shared interface for checking the version of the sequencer service that a client providing sequencer operations will be calling. */
trait SupportsHandshake {

  /** Attempt to obtain a handshake response from the sequencer server.
    * Can indicate with the error if the error is transient and may be retried by the caller.
    */
  def handshake(request: HandshakeRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, HandshakeRequestError, HandshakeResponse]
}
