// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SerializedEventOrErrorHandler
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

/** Factory for creating resilient subscriptions directly to an in-process [[sequencer.Sequencer]] */
class DirectSequencerSubscriptionFactory(
    sequencer: Sequencer,
    timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends NamedLogging {

  /** Create a subscription for an event handler to observe sequencer events for this member.
    * By connecting from `startingAt` it is assumed that all prior events have been successfully read by the member (and may be removed by a separate administrative process).
    * Closing the returned subscription should disconnect the handler.
    * If member is unknown by the sequencer a [[sequencer.errors.CreateSubscriptionError.UnknownMember]] error will be returned.
    * If the counter is invalid (currently will only happen if counter <0) a [[sequencer.errors.CreateSubscriptionError.InvalidCounter]] error will be returned.
    *
    * @param startingAt Counter of the next event to observe. (e.g. 0 will return the first event when it is available)
    * @param member Member to subscribe on behalf of.
    * @param handler The handler to invoke with sequencer events
    * @return A running subscription
    */
  def create[E](
      startingAt: SequencerCounter,
      member: Member,
      handler: SerializedEventOrErrorHandler[E],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, SequencerSubscription[E]] = {
    logger.debug(show"Creating subscription for $member from $startingAt...")
    for {
      source <- sequencer.read(member, startingAt)
    } yield {
      val subscription =
        new DirectSequencerSubscription[E](member, source, handler, timeouts, loggerFactory)
      logger.debug(
        show"Created sequencer subscription for $member from $startingAt (may still be starting)"
      )
      subscription
    }
  }
}
