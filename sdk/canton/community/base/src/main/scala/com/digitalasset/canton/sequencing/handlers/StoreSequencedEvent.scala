// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.SingletonTraverse.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, SingletonTraverse}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.concurrent.ExecutionContext

/** Transformer for [[com.digitalasset.canton.sequencing.OrdinaryApplicationHandler]]
  * that stores all event batches in the [[com.digitalasset.canton.store.SequencedEventStore]]
  * before passing them on to the given handler. Complains if events have the wrong synchronizer id.
  */
class StoreSequencedEvent(
    store: SequencedEventStore,
    synchronizerId: SynchronizerId,
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
      (_, tracedEvents) =>
        storeBatch(tracedEvents)
          .failOnShutdownToAbortException("StoreSequencedEvent store batch")
          .map((_: Unit) => tracedEvents)
    })

  def apply(
      handler: OrdinaryApplicationHandler[ClosedEnvelope]
  ): OrdinaryApplicationHandler[ClosedEnvelope] =
    handler.replace(tracedEvents => storeBatch(tracedEvents).flatMap(_ => handler(tracedEvents)))

  private def storeBatch(
      tracedEvents: BoxedEnvelope[OrdinaryEnvelopeBox, ClosedEnvelope]
  ): FutureUnlessShutdown[Unit] =
    tracedEvents.withTraceContext { implicit batchTraceContext => events =>
      val wrongDomainEvents = events.filter(_.signedEvent.content.synchronizerId != synchronizerId)
      ErrorUtil.requireArgument(
        wrongDomainEvents.isEmpty, {
          val wrongsynchronizerIds =
            wrongDomainEvents.map(_.signedEvent.content.synchronizerId).distinct
          val wrongDomainCounters = wrongDomainEvents.map(_.signedEvent.content.counter)
          show"Cannot store sequenced events from synchronizers $wrongsynchronizerIds in store for synchronizer $synchronizerId\nSequencer counters: $wrongDomainCounters"
        },
      )
      // The events must be stored before we call the handler
      // so that during crash recovery the `SequencerClient` can use the first event in the
      // `SequencedEventStore` as the beginning of the resubscription even if that event is not known to be clean.
      store.store(events)
    }
}

object StoreSequencedEvent {
  def apply(
      store: SequencedEventStore,
      synchronizerId: SynchronizerId,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      closeContext: CloseContext,
  ): StoreSequencedEvent =
    new StoreSequencedEvent(store, synchronizerId, loggerFactory)
}
