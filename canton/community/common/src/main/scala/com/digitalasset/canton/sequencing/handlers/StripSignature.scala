// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.sequencing.protocol.Envelope
import com.digitalasset.canton.sequencing.{OrdinaryApplicationHandler, UnsignedApplicationHandler}
import com.digitalasset.canton.tracing.Traced

/** Removes the [[com.digitalasset.canton.sequencing.protocol.SignedContent]] wrapper before providing to a handler */
object StripSignature {
  def apply[Env <: Envelope[_]](
      handler: UnsignedApplicationHandler[Env]
  ): OrdinaryApplicationHandler[Env] =
    handler.replace(events =>
      handler(events.map(_.map(e => Traced(e.signedEvent.content)(e.traceContext))))
    )
}
