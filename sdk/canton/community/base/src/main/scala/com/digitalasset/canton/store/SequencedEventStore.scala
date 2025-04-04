// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.EitherT
import cats.implicits.showInterpolator
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, ProtocolMessage}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.pruning.PruningStatus
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  OrdinarySerializedEvent,
  PossiblyIgnoredProtocolEvent,
  PossiblyIgnoredSerializedEvent,
  SequencedSerializedEvent,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.*
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent.dbTypeOfEvent
import com.digitalasset.canton.store.db.DbSequencedEventStore
import com.digitalasset.canton.store.db.DbSequencedEventStore.SequencedEventDbType
import com.digitalasset.canton.store.memory.InMemorySequencedEventStore
import com.digitalasset.canton.tracing.{
  HasTraceContext,
  SerializableTraceContext,
  TraceContext,
  Traced,
}
import com.digitalasset.canton.util.{ErrorUtil, Thereafter}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, blocking}
import scala.math.Ordered.orderingToOrdered
import scala.util.Failure

/** Persistent store for [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]]s received
  * from the sequencer. The store may assume that sequencer counters strictly increase with
  * timestamps without checking this precondition.
  */
trait SequencedEventStore
    extends PrunableByTime
    with NamedLogging
    with FlagCloseable
    with HasCloseContext {

  /** Semaphore to prevent concurrent writes to the db. Concurrent calls can be problematic because
    * they may introduce gaps in the stored sequencer counters. The methods [[store]],
    * [[storeSequenced]], [[ignoreEvents]] and [[unignoreEvents]], are not meant to be executed
    * concurrently.
    */
  private[this] val semaphore: Semaphore = new Semaphore(1)

  protected[this] def withLock[F[_], A](caller: String)(body: => F[A])(implicit
      thereafter: Thereafter[F],
      traceContext: TraceContext,
  ): F[A] = {
    import Thereafter.syntax.*
    // Avoid unnecessary call to blocking, if a permit is available right away.
    if (!semaphore.tryAcquire()) {
      // This should only occur when the caller is ignoring events, so ok to log with info level.
      logger.info(s"Delaying call to $caller, because another write is in progress.")
      blocking(semaphore.acquireUninterruptibly())
    }
    body.thereafter(_ => semaphore.release())
  }

  protected[this] def fetchLastCounterAndTimestamp(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CounterAndTimestamp]]

  private[this] val lowerBound: AtomicReference[Option[CounterAndTimestamp]] = new AtomicReference(
    None
  )

  /** Initializes the sequencer counter allocator and timestamp lower bound with data from the store
    * itself. The parameter `counterIfEmpty` is intended for tests only, where we start with an
    * empty store and will be ignored if the store is not empty.
    */
  def reinitializeFromDbOrSetLowerBound(
      counterIfEmpty: SequencerCounter =
        SequencerCounter.Genesis - 1 // to start from 0 we need to subtract 1
  )(implicit tc: TraceContext): FutureUnlessShutdown[CounterAndTimestamp] =
    fetchLastCounterAndTimestamp.map { fromDb =>
      val newLowerBound =
        fromDb.getOrElse(CounterAndTimestamp(counterIfEmpty, CantonTimestamp.MinValue))
      logger.debug(s"Initialized the lower bound from the database: $newLowerBound")
      lowerBound.set(Some(newLowerBound))
      newLowerBound
    }

  /** Calls `f` with the current `lowerBound`, updates it only on success with the returned value.
    */
  private def withLowerBoundUpdate[T](
      f: CounterAndTimestamp => FutureUnlessShutdown[(CounterAndTimestamp, T)]
  )(implicit tc: TraceContext): FutureUnlessShutdown[T] =
    for {
      lowerBoundBefore <- lowerBound
        .get()
        .fold(reinitializeFromDbOrSetLowerBound())(FutureUnlessShutdown.pure)
      (lowerBoundAfter, result) <- f(lowerBoundBefore).transform {
        case failure @ Failure(_) =>
          // In case of failure we set the lower bound to the `None` to force its reinitialization
          // on a subsequent call to allow the db failure retry to sync with the actual state in the db
          logger.debug(
            s"SequencedEventStore.store operation has failed, the state has been reset to reinitialize from db",
            failure.exception,
          )
          lowerBound.set(None)
          failure
        case x => x
      }
    } yield {
      if (lowerBoundBefore < lowerBoundAfter) {
        val storedCount = lowerBoundAfter.lastCounter - lowerBoundBefore.lastCounter
        lowerBound.set(Some(lowerBoundAfter))
        logger.debug(
          s"Successfully stored $storedCount events and updated the lower bound from $lowerBoundBefore to $lowerBoundAfter"
        )
      } else if (lowerBoundBefore > lowerBoundAfter) {
        ErrorUtil.invalidState(
          s"SequencedEventStore's lower bound is not expected to decrease, observed a decrease: $lowerBoundBefore -> $lowerBoundAfter"
        )
      }
      result
    }

  import SequencedEventStore.SearchCriterion

  implicit val ec: ExecutionContext
  protected def kind: String = "sequenced events"

  /** Assigns counters & stores the given
    * [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]]s. If an event with the same
    * timestamp already exist, the event may remain unchanged or overwritten.
    */
  def store(signedEvents: Seq[OrdinarySerializedEvent])(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] =
    storeSequenced(signedEvents.map(_.asSequencedSerializedEvent))(
      traceContext,
      externalCloseContext,
    )
      .map(_ => ())

  /** Assigns counters & stores the given
    * [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]]s. If an event with the same
    * timestamp already exist, the event may remain unchanged or overwritten.
    */
  def storeSequenced(signedEvents: Seq[SequencedSerializedEvent])(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): FutureUnlessShutdown[Seq[OrdinarySerializedEvent]] =
    if (signedEvents.isEmpty) FutureUnlessShutdown.pure(Seq.empty)
    else {
      withLock(functionFullName) {
        CloseContext.withCombinedContext(closeContext, externalCloseContext, timeouts, logger) {
          combinedCloseContext =>
            withLowerBoundUpdate { lowerBound =>
              val CounterAndTimestamp(lastCounter, lastTimestamp) = lowerBound
              val (skippedEvents, eventsToStore) = signedEvents.partition(
                _.value.content.timestamp <= lastTimestamp
              )
              if (skippedEvents.nonEmpty) {
                logger.warn(
                  s"Skipping ${skippedEvents.size} events with timestamp <= $lastTimestamp (presumed already processed)"
                )
              }
              val noUpdates =
                FutureUnlessShutdown.pure((lowerBound, Seq.empty[OrdinarySerializedEvent]))
              NonEmpty.from(eventsToStore).fold(noUpdates) { eventsToStoreNE =>
                val eventsWithCounters =
                  eventsToStoreNE.zipWithIndex.map { case (signedEvent, idx) =>
                    val counter = lastCounter + 1 + idx
                    OrdinarySequencedEvent(counter, signedEvent.value)(
                      signedEvent.traceContext
                    )
                  }
                logger.debug(
                  show"Storing delivery events from ${eventsWithCounters.head1.timestamp} / ${eventsWithCounters.head1.counter} to ${eventsWithCounters.last1.timestamp} / ${eventsWithCounters.last1.counter}."
                )
                val storeEventsF =
                  storeEventsInternal(eventsWithCounters)(traceContext, combinedCloseContext)
                val updatedLowerBound =
                  CounterAndTimestamp(
                    eventsWithCounters.last1.counter,
                    eventsWithCounters.last1.timestamp,
                  )
                storeEventsF.map(_ => (updatedLowerBound, eventsWithCounters))
              }
            }
        }
      }
    }

  /** The actual store operation implementation to perform on the database.
    */
  protected def storeEventsInternal(
      eventsNE: NonEmpty[Seq[OrdinarySerializedEvent]]
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): FutureUnlessShutdown[Unit]

  /** Looks up an event by the given criterion.
    *
    * @return
    *   [[SequencedEventNotFoundError]] if no stored event meets the criterion.
    */
  def find(criterion: SearchCriterion)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencedEventNotFoundError, PossiblyIgnoredSerializedEvent]

  /** Looks up a set of sequenced events within the given range.
    *
    * @param limit
    *   The maximum number of elements in the returned iterable, if set.
    */
  def findRange(criterion: RangeCriterion, limit: Option[Int])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencedEventRangeOverlapsWithPruning, Seq[
    PossiblyIgnoredSerializedEvent
  ]]

  def sequencedEvents(limit: Option[Int] = None)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[PossiblyIgnoredSerializedEvent]]

  /** Marks events between `from` and `to` as ignored. Fills any gap between `from` and `to` by
    * empty ignored events, i.e. ignored events without any underlying real event.
    *
    * @return
    *   [[ChangeWouldResultInGap]] if there would be a gap between the highest sequencer counter in
    *   the store and `from`.
    */
  final def ignoreEvents(fromInclusive: SequencerCounter, toInclusive: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ChangeWouldResultInGap, Unit] =
    ignoreEventsInternal(fromInclusive, toInclusive)
      .flatMap(_ => EitherT.right[ChangeWouldResultInGap](reinitializeFromDbOrSetLowerBound()))
      .map(_ => ())

  protected def ignoreEventsInternal(
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ChangeWouldResultInGap, Unit]

  /** Removes the ignored status from all events between `from` and `to`.
    *
    * @return
    *   [[ChangeWouldResultInGap]] if deleting empty ignored events between `from` and `to` would
    *   result in a gap in sequencer counters.
    */
  final def unignoreEvents(fromInclusive: SequencerCounter, toInclusive: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ChangeWouldResultInGap, Unit] =
    unignoreEventsInternal(fromInclusive, toInclusive)
      .flatMap(_ => EitherT.right[ChangeWouldResultInGap](reinitializeFromDbOrSetLowerBound()))
      .map(_ => ())

  protected def unignoreEventsInternal(
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ChangeWouldResultInGap, Unit]

  /** Deletes all events with sequencer counter greater than or equal to `from`.
    */
  final private[canton] def delete(fromInclusive: SequencerCounter)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    deleteInternal(fromInclusive)
      .flatMap(_ => reinitializeFromDbOrSetLowerBound())
      .map(_ => ())

  protected def deleteInternal(fromInclusive: SequencerCounter)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Purges all data from the store.
    */
  final def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    delete(
      SequencerCounter.Genesis
    ).flatMap(_ => reinitializeFromDbOrSetLowerBound())
      .map(_ => ())

  /** Look up a TraceContext for a sequenced event
    *
    * @param sequencedTimestamp
    *   The timestemp which uniquely identifies the sequenced event
    * @return
    *   The TraceContext or None if the sequenced event cannot be found
    */
  def traceContext(sequencedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TraceContext]]
}

object SequencedEventStore {

  def apply[Env <: Envelope[_]](
      storage: Storage,
      indexedSynchronizer: IndexedSynchronizer,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SequencedEventStore =
    storage match {
      case _: MemoryStorage => new InMemorySequencedEventStore(loggerFactory, timeouts)
      case dbStorage: DbStorage =>
        new DbSequencedEventStore(
          dbStorage,
          indexedSynchronizer,
          protocolVersion,
          timeouts,
          loggerFactory,
        )
    }

  final case class CounterAndTimestamp(
      lastCounter: SequencerCounter,
      lastTimestamp: CantonTimestamp,
  )

  object CounterAndTimestamp {
    implicit val ord: Ordering[CounterAndTimestamp] =
      Ordering.by(x => (x.lastTimestamp, x.lastCounter))
  }

  sealed trait SearchCriterion extends Product with Serializable

  /** Find the event with the given timestamp */
  final case class ByTimestamp(timestamp: CantonTimestamp) extends SearchCriterion

  /** Finds the event with the highest timestamp before or at `inclusive` */
  final case class LatestUpto(inclusive: CantonTimestamp) extends SearchCriterion

  object SearchCriterion {
    val Latest: SearchCriterion = LatestUpto(CantonTimestamp.MaxValue)
  }

  /** Finds a sequence of events within a range */
  sealed trait RangeCriterion extends Product with Serializable with PrettyPrinting

  /** Finds all events with timestamps within the given range.
    *
    * @param lowerInclusive
    *   The lower bound, inclusive. Must not be after `upperInclusive`
    * @param upperInclusive
    *   The upper bound, inclusive. Must not be before `lowerInclusive`
    * @throws java.lang.IllegalArgumentException
    *   if `lowerInclusive` is after `upperInclusive`
    */
  final case class ByTimestampRange(
      lowerInclusive: CantonTimestamp,
      upperInclusive: CantonTimestamp,
  ) extends RangeCriterion {
    require(
      lowerInclusive <= upperInclusive,
      s"Lower bound timestamp $lowerInclusive is after upper bound $upperInclusive",
    )

    override protected def pretty: Pretty[ByTimestampRange] = prettyOfClass(
      param("lower inclusive", _.lowerInclusive),
      param("upper inclusive", _.upperInclusive),
    )
  }

  type SequencedEventWithTraceContext[+Env <: Envelope[_]] =
    Traced[SignedContent[SequencedEvent[Env]]]

  /** Encapsulates an event stored in the SequencedEventStore.
    */
  sealed trait PossiblyIgnoredSequencedEvent[+Env <: Envelope[_]]
      extends HasTraceContext
      with PrettyPrinting
      with Product
      with Serializable {

    def previousTimestamp: Option[CantonTimestamp]

    def timestamp: CantonTimestamp

    def counter: SequencerCounter

    def underlyingEventBytes: Array[Byte]

    private[store] def dbType: SequencedEventDbType

    def isIgnored: Boolean

    def underlying: Option[SignedContent[SequencedEvent[Env]]]

    def asIgnoredEvent: IgnoredSequencedEvent[Env]

    def asOrdinaryEvent: PossiblyIgnoredSequencedEvent[Env]

    def toProtoV30: v30.PossiblyIgnoredSequencedEvent =
      v30.PossiblyIgnoredSequencedEvent(
        counter = counter.toProtoPrimitive,
        timestamp = timestamp.toProtoPrimitive,
        traceContext = Some(SerializableTraceContext(traceContext).toProtoV30),
        isIgnored = isIgnored,
        underlying = underlying.map(_.toByteString),
      )
  }

  /** Encapsulates an ignored event, i.e., an event that should not be processed.
    *
    * If an ordinary sequenced event `oe` is later converted to an ignored event `ie`, the actual
    * event `oe.signedEvent` is retained as `ie.underlying` so that no information gets discarded by
    * ignoring events. If an ignored event `ie` is inserted as a placeholder for an event that has
    * not been received, the underlying event `ie.underlying` is left empty.
    */
  final case class IgnoredSequencedEvent[+Env <: Envelope[?]](
      override val timestamp: CantonTimestamp,
      override val counter: SequencerCounter,
      override val underlying: Option[SignedContent[SequencedEvent[Env]]],
      // TODO(#11834): Hardcoded to previousTimestamp=None, need to make sure that previousTimestamp
      //   works with ignored events and repair service
      override val previousTimestamp: Option[CantonTimestamp] = None,
  )(override val traceContext: TraceContext)
      extends PossiblyIgnoredSequencedEvent[Env] {

    override def underlyingEventBytes: Array[Byte] = Array.empty

    private[store] override def dbType: SequencedEventDbType =
      underlying.fold[SequencedEventDbType](SequencedEventDbType.IgnoredEvent)(e =>
        dbTypeOfEvent(e.content)
      )

    override def isIgnored: Boolean = true

    override def asIgnoredEvent: IgnoredSequencedEvent[Env] = this

    override def asOrdinaryEvent: PossiblyIgnoredSequencedEvent[Env] = underlying match {
      case Some(event) => OrdinarySequencedEvent(event)(traceContext)
      case None => this
    }

    override protected def pretty: Pretty[IgnoredSequencedEvent[Envelope[?]]] =
      prettyOfClass(
        param("timestamp", _.timestamp),
        param("counter", _.counter),
        paramIfDefined("underlying", _.underlying),
      )
  }

  object IgnoredSequencedEvent {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def openEnvelopes(
        event: IgnoredSequencedEvent[ClosedEnvelope]
    )(
        protocolVersion: ProtocolVersion,
        hashOps: HashOps,
    ): WithOpeningErrors[IgnoredSequencedEvent[DefaultOpenEnvelope]] =
      event.underlying match {
        case Some(signedEvent) =>
          SignedContent
            .openEnvelopes(signedEvent)(protocolVersion, hashOps)
            .map(evt => event.copy(underlying = Some(evt))(event.traceContext))
        case None =>
          NoOpeningErrors(event.asInstanceOf[IgnoredSequencedEvent[DefaultOpenEnvelope]])
      }
  }

  /** Encapsulates an event received by the sequencer. It has been signed by the sequencer and
    * contains a trace context.
    */
  final case class OrdinarySequencedEvent[+Env <: Envelope[_]](
      override val counter: SequencerCounter,
      signedEvent: SignedContent[SequencedEvent[Env]],
  )(
      override val traceContext: TraceContext
  ) extends PossiblyIgnoredSequencedEvent[Env] {
    require(
      counter == signedEvent.content.counter,
      s"For event at timestamp $timestamp, counter $counter doesn't match the underlying SequencedEvent's counter ${signedEvent.content.counter}",
    )

    override def previousTimestamp: Option[CantonTimestamp] = signedEvent.content.previousTimestamp

    override def timestamp: CantonTimestamp = signedEvent.content.timestamp

    override def underlyingEventBytes: Array[Byte] = signedEvent.toByteArray

    private[store] override def dbType: SequencedEventDbType = dbTypeOfEvent(signedEvent.content)

    override def isIgnored: Boolean = false

    def isTombstone: Boolean = signedEvent.content.isTombstone

    override def underlying: Some[SignedContent[SequencedEvent[Env]]] = Some(signedEvent)

    override def asIgnoredEvent: IgnoredSequencedEvent[Env] =
      IgnoredSequencedEvent(timestamp, counter, Some(signedEvent))(traceContext)

    override def asOrdinaryEvent: PossiblyIgnoredSequencedEvent[Env] = this

    def asSequencedSerializedEvent: SequencedEventWithTraceContext[Env] =
      Traced(signedEvent)(traceContext)

    override protected def pretty: Pretty[OrdinarySequencedEvent[Envelope[_]]] = prettyOfClass(
      param("signedEvent", _.signedEvent)
    )
  }

  object OrdinarySequencedEvent {

    // #TODO(#11834): This is an old constructor when we used counter from the SequencedEvent,
    //   to be removed once the counter is gone from the SequencedEvent
    def apply[Env <: Envelope[_]](signedEvent: SignedContent[SequencedEvent[Env]])(
        traceContext: TraceContext
    ): OrdinarySequencedEvent[Env] =
      OrdinarySequencedEvent(signedEvent.content.counter, signedEvent)(traceContext)

    def openEnvelopes(event: OrdinarySequencedEvent[ClosedEnvelope])(
        protocolVersion: ProtocolVersion,
        hashOps: HashOps,
    ): WithOpeningErrors[OrdinarySequencedEvent[DefaultOpenEnvelope]] =
      SignedContent
        .openEnvelopes(event.signedEvent)(protocolVersion, hashOps)
        .map(evt => event.copy(signedEvent = evt)(event.traceContext))
  }

  object PossiblyIgnoredSequencedEvent {

    private[store] def dbTypeOfEvent(content: SequencedEvent[?]): SequencedEventDbType =
      content match {
        case _: DeliverError => SequencedEventDbType.DeliverError
        case _: Deliver[_] => SequencedEventDbType.Deliver
      }

    def fromProtoV30(protocolVersion: ProtocolVersion, hashOps: HashOps)(
        possiblyIgnoredSequencedEventP: v30.PossiblyIgnoredSequencedEvent
    ): ParsingResult[PossiblyIgnoredProtocolEvent] = {
      val v30.PossiblyIgnoredSequencedEvent(
        counter,
        timestampP,
        traceContextPO,
        isIgnored,
        underlyingPO,
      ) = possiblyIgnoredSequencedEventP

      val sequencerCounter = SequencerCounter(counter)

      for {
        underlyingO <- underlyingPO.traverse(
          SignedContent
            .fromByteString(protocolVersion, _)
            .flatMap(
              _.deserializeContent(SequencedEvent.fromByteStringOpen(hashOps, protocolVersion))
            )
        )
        timestamp <- CantonTimestamp.fromProtoPrimitive(timestampP)
        traceContext <- ProtoConverter
          .required("trace_context", traceContextPO)
          .flatMap(SerializableTraceContext.fromProtoV30)
        possiblyIgnoredSequencedEvent <-
          if (isIgnored) {
            Right(
              IgnoredSequencedEvent(timestamp, sequencerCounter, underlyingO)(
                traceContext.unwrap
              )
            )
          } else
            ProtoConverter
              .required("underlying", underlyingO)
              .map(
                OrdinarySequencedEvent(_)(
                  traceContext.unwrap
                )
              )
      } yield possiblyIgnoredSequencedEvent
    }

    def openEnvelopes(event: PossiblyIgnoredSequencedEvent[ClosedEnvelope])(
        protocolVersion: ProtocolVersion,
        hashOps: HashOps,
    ): WithOpeningErrors[PossiblyIgnoredSequencedEvent[OpenEnvelope[ProtocolMessage]]] =
      event match {
        case evt: OrdinarySequencedEvent[ClosedEnvelope] =>
          OrdinarySequencedEvent.openEnvelopes(evt)(protocolVersion, hashOps)
        case evt: IgnoredSequencedEvent[ClosedEnvelope] =>
          IgnoredSequencedEvent.openEnvelopes(evt)(protocolVersion, hashOps)
      }
  }
}

sealed trait SequencedEventStoreError extends Product with Serializable

final case class SequencedEventNotFoundError(criterion: SequencedEventStore.SearchCriterion)
    extends SequencedEventStoreError

final case class SequencedEventRangeOverlapsWithPruning(
    criterion: RangeCriterion,
    pruningStatus: PruningStatus,
    foundEvents: Seq[PossiblyIgnoredSerializedEvent],
) extends SequencedEventStoreError
    with PrettyPrinting {
  override protected def pretty: Pretty[SequencedEventRangeOverlapsWithPruning.this.type] =
    prettyOfClass(
      param("criterion", _.criterion),
      param("pruning status", _.pruningStatus),
      param("found events", _.foundEvents),
    )
}

final case class ChangeWouldResultInGap(from: SequencerCounter, to: SequencerCounter)
    extends SequencedEventStoreError {
  override def toString: String =
    s"Unable to perform operation, because that would result in a sequencer counter gap between $from and $to."
}
