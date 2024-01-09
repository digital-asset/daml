// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.sequencing.{
  BoxedEnvelope,
  OrdinaryApplicationHandler,
  OrdinaryEnvelopeBox,
  OrdinarySerializedEvent,
}
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.SingletonTraverse.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, SingletonTraverse}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.concurrent.{ExecutionContext, Future}

/** Transformer for [[com.digitalasset.canton.sequencing.OrdinaryApplicationHandler]]
  * that stores all event batches in the [[com.digitalasset.canton.store.SequencedEventStore]]
  * before passing them on to the given handler. Complains if events have the wrong domain ID.
  */
class StoreSequencedEvent(
    store: SequencedEventStore,
    domainId: DomainId,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, closeContext: CloseContext)
    extends NamedLogging {

  def flow[F[_]](implicit F: SingletonTraverse[F]): Flow[
    F[Traced[Seq[OrdinarySerializedEvent]]],
    F[Traced[Seq[OrdinarySerializedEvent]]],
    NotUsed,
  ] = Flow[F[Traced[Seq[OrdinarySerializedEvent]]]]
    // Store the events as part of the flow
    .mapAsync(parallelism = 1)(_.traverseSingleton {
      // TODO(#13789) Properly deal with exceptions
      (_, tracedEvents) => storeBatch(tracedEvents).map((_: Unit) => tracedEvents)
    })

  def apply(
      handler: OrdinaryApplicationHandler[ClosedEnvelope]
  ): OrdinaryApplicationHandler[ClosedEnvelope] =
    handler.replace(tracedEvents =>
      FutureUnlessShutdown.outcomeF(storeBatch(tracedEvents)).flatMap(_ => handler(tracedEvents))
    )

  private def storeBatch(
      tracedEvents: BoxedEnvelope[OrdinaryEnvelopeBox, ClosedEnvelope]
  ): Future[Unit] = {
    tracedEvents.withTraceContext { implicit batchTraceContext => events =>
      val wrongDomainEvents = events.filter(_.signedEvent.content.domainId != domainId)
      ErrorUtil.requireArgument(
        wrongDomainEvents.isEmpty, {
          val wrongDomainIds = wrongDomainEvents.map(_.signedEvent.content.domainId).distinct
          val wrongDomainCounters = wrongDomainEvents.map(_.signedEvent.content.counter)
          show"Cannot store sequenced events from domains $wrongDomainIds in store for domain $domainId\nSequencer counters: $wrongDomainCounters"
        },
      )
      // The events must be stored before we call the handler
      // so that during crash recovery the `SequencerClient` can use the first event in the
      // `SequencedEventStore` as the beginning of the resubscription even if that event is not known to be clean.
      store.store(events)
    }
  }
}

object StoreSequencedEvent {
  def apply(store: SequencedEventStore, domainId: DomainId, loggerFactory: NamedLoggerFactory)(
      implicit
      ec: ExecutionContext,
      closeContext: CloseContext,
  ): StoreSequencedEvent =
    new StoreSequencedEvent(store, domainId, loggerFactory)
}
