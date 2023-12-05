// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import cats.syntax.alternative.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.Envelope
import com.digitalasset.canton.sequencing.{
  BoxedEnvelope,
  HandlerResult,
  OrdinaryApplicationHandler,
  PossiblyIgnoredApplicationHandler,
  PossiblyIgnoredEnvelopeBox,
  SubscriptionStart,
}
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  OrdinarySequencedEvent,
}
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.tracing.TraceContext

/** Forwards only [[com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent]]s
  * to the given [[com.digitalasset.canton.sequencing.ApplicationHandler]].
  *
  * This must only be used on code paths where there cannot be other types of events by construction.
  * Otherwise, the application handler will not be informed about ignored event and cannot tick any of the trackers,
  * including the [[com.digitalasset.canton.topology.processing.TopologyTransactionProcessor]].
  */
class DiscardIgnoredEvents[Env <: Envelope[_]](
    handler: OrdinaryApplicationHandler[Env],
    override protected val loggerFactory: NamedLoggerFactory,
) extends PossiblyIgnoredApplicationHandler[Env]
    with NamedLogging {

  override def name: String = handler.name

  override def subscriptionStartsAt(start: SubscriptionStart, domainTimeTracker: DomainTimeTracker)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = handler.subscriptionStartsAt(start, domainTimeTracker)

  override def apply(
      tracedEvents: BoxedEnvelope[PossiblyIgnoredEnvelopeBox, Env]
  ): HandlerResult = {
    val filtered = tracedEvents.mapWithTraceContext { implicit batchTraceContext => events =>
      val classified = events.map {
        case e: OrdinarySequencedEvent[Env] => Right(e)
        case e: IgnoredSequencedEvent[Env] => Left(e)
      }
      val (ignored, ordinary) = classified.separate
      // We merely log a warning for now rather than fail the application handler.
      // This way, we'll notice when we actually add commands for ignoring events on code paths that discard them,
      // but an expert can still manually repair broken deployments by ignoring events directly in the DB
      if (ignored.nonEmpty) {
        logger.warn(
          s"Ignored events with counters ${ignored.map(_.counter)} are not passed to application handler. This may cause problems when validating subsequent events."
        )
      }
      ordinary
    }
    handler(filtered)
  }
}

object DiscardIgnoredEvents {
  def apply[Env <: Envelope[_]](loggerFactory: NamedLoggerFactory)(
      handler: OrdinaryApplicationHandler[Env]
  ): PossiblyIgnoredApplicationHandler[Env] = new DiscardIgnoredEvents[Env](handler, loggerFactory)
}
