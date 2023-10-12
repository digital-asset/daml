// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.protocol.{Deliver, Envelope}
import com.digitalasset.canton.sequencing.{
  EnvelopeHandler,
  HandlerResult,
  OrdinaryApplicationHandler,
  UnsignedApplicationHandler,
  UnsignedEnvelopeBox,
}
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.ExecutionContext

/** Removes the [[com.digitalasset.canton.sequencing.protocol.SignedContent]] wrapper before providing to a handler */
object StripSignature {
  def apply[Env <: Envelope[_]](
      handler: UnsignedApplicationHandler[Env]
  ): OrdinaryApplicationHandler[Env] =
    handler.replace(events =>
      handler(events.map(_.map(e => Traced(e.signedEvent.content)(e.traceContext))))
    )
}

object StripOrdinaryEnvelopeBox {
  def apply(
      handler: EnvelopeHandler
  )(implicit ec: ExecutionContext): OrdinaryApplicationHandler[DefaultOpenEnvelope] = {
    StripSignature(
      handler.replace[UnsignedEnvelopeBox, DefaultOpenEnvelope](box =>
        MonadUtil.sequentialTraverseMonoid(box.value)(
          _.withTraceContext { implicit traceContext =>
            {
              case Deliver(_, _, _, _, batch) => handler(Traced(batch.envelopes))
              case _ => HandlerResult.done
            }
          }
        )
      )
    )
  }

}
