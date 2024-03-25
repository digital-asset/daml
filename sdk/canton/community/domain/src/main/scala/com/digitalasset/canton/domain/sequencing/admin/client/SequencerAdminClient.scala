// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.client

import cats.data.EitherT
import com.digitalasset.canton.domain.sequencing.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerResponse,
}
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.sequencing.handshake.SupportsHandshake
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Interface for performing administrative operations against a sequencer */
trait SequencerAdminClient extends FlagCloseable with SupportsHandshake {

  /** Called during domain initialization with appropriate identities and public keys for the sequencer to initialize itself.
    * If successful the sequencer is expected to return its PublicKey and be immediately ready to accept requests.
    */
  def initialize(request: InitializeSequencerRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, InitializeSequencerResponse]
}
