// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SerializedEventOrErrorHandler
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.synchronizer.sequencer.Sequencer
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext

/** Factory for creating resilient subscriptions directly to an in-process [[sequencer.Sequencer]]
  */
class DirectSequencerSubscriptionFactory(
    sequencer: Sequencer,
    timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends NamedLogging {

  /** Create a subscription for an event handler to observe sequencer events for this member. By
    * connecting from inclusive `timestamp` it is assumed that all prior events have been
    * successfully read by the member (and may be removed by a separate administrative process).
    * Closing the returned subscription should disconnect the handler. If member is unknown by the
    * sequencer a [[sequencer.errors.CreateSubscriptionError.UnknownMember]] error will be returned.
    *
    * @param timestamp
    *   Inclusive starting timestamp or None to start from the very beginning.
    * @param member
    *   Member to subscribe on behalf of.
    * @param handler
    *   The handler to invoke with sequencer events
    * @return
    *   A running subscription
    */
  def createV2[E](
      timestamp: Option[CantonTimestamp],
      member: Member,
      handler: SerializedEventOrErrorHandler[E],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, SequencerSubscription[E]] = {
    val timestampString = timestamp.map(_.toString).getOrElse("the beginning")
    logger.debug(show"Creating subscription for $member from $timestampString...")
    for {
      source <- sequencer.readV2(member, timestamp)
    } yield {
      val subscription =
        new DirectSequencerSubscription[E](member, source, handler, timeouts, loggerFactory)
      logger.debug(
        show"Created sequencer subscription for $member from $timestampString (may still be starting)"
      )
      subscription
    }
  }
}
