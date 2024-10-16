// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
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
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.*
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent.dbTypeOfEvent
import com.digitalasset.canton.store.db.DbSequencedEventStore
import com.digitalasset.canton.store.db.DbSequencedEventStore.SequencedEventDbType
import com.digitalasset.canton.store.memory.InMemorySequencedEventStore
import com.digitalasset.canton.tracing.{HasTraceContext, SerializableTraceContext, TraceContext}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

/** Persistent store for [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]]s received from the sequencer.
  * The store may assume that sequencer counters strictly increase with timestamps
  * without checking this precondition.
  */
trait SequencedEventStore extends PrunableByTime with NamedLogging with AutoCloseable {

  import SequencedEventStore.SearchCriterion

  implicit val ec: ExecutionContext
  protected def kind: String = "sequenced events"

  /** Stores the given [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]]s.
    * If an event with the same timestamp already exist, the event may remain unchanged or overwritten.
    */
  def store(signedEvents: Seq[OrdinarySerializedEvent])(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): Future[Unit]

  /** Looks up an event by the given criterion.
    *
    * @return [[SequencedEventNotFoundError]] if no stored event meets the criterion.
    */
  def find(criterion: SearchCriterion)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencedEventNotFoundError, PossiblyIgnoredSerializedEvent]

  /** Looks up a set of sequenced events within the given range.
    *
    * @param limit The maximum number of elements in the returned iterable, if set.
    */
  def findRange(criterion: RangeCriterion, limit: Option[Int])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencedEventRangeOverlapsWithPruning, Seq[PossiblyIgnoredSerializedEvent]]

  def sequencedEvents(limit: Option[Int] = None)(implicit
      traceContext: TraceContext
  ): Future[Seq[PossiblyIgnoredSerializedEvent]]

  /** Marks events between `from` and `to` as ignored.
    * Fills any gap between `from` and `to` by empty ignored events, i.e. ignored events without any underlying real event.
    *
    * @return [[ChangeWouldResultInGap]] if there would be a gap between the highest sequencer counter in the store and `from`.
    */
  def ignoreEvents(fromInclusive: SequencerCounter, toInclusive: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit]

  /** Removes the ignored status from all events between `from` and `to`.
    *
    * @return [[ChangeWouldResultInGap]] if deleting empty ignored events between `from` and `to` would result in a gap in sequencer counters.
    */
  def unignoreEvents(fromInclusive: SequencerCounter, toInclusive: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit]

  /** Deletes all events with sequencer counter greater than or equal to `from`.
    */
  @VisibleForTesting
  private[canton] def delete(fromInclusive: SequencerCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Purges all data from the store.
    */
  def purge()(implicit traceContext: TraceContext): Future[Unit] = delete(SequencerCounter.Genesis)
}

object SequencedEventStore {

  def apply[Env <: Envelope[_]](
      storage: Storage,
      indexedDomain: IndexedDomain,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SequencedEventStore =
    storage match {
      case _: MemoryStorage => new InMemorySequencedEventStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbSequencedEventStore(
          dbStorage,
          indexedDomain,
          protocolVersion,
          timeouts,
          loggerFactory,
        )
    }

  sealed trait SearchCriterion extends Product with Serializable

  /** Find the event with the given timestamp */
  final case class ByTimestamp(timestamp: CantonTimestamp) extends SearchCriterion

  /** Finds the event with the highest timestamp before or at `inclusive` */
  final case class LatestUpto(inclusive: CantonTimestamp) extends SearchCriterion

  /** Finds a sequence of events within a range */
  sealed trait RangeCriterion extends Product with Serializable with PrettyPrinting

  /** Finds all events with timestamps within the given range.
    *
    * @param lowerInclusive The lower bound, inclusive. Must not be after `upperInclusive`
    * @param upperInclusive The upper bound, inclusive. Must not be before `lowerInclusive`
    * @throws java.lang.IllegalArgumentException if `lowerInclusive` is after `upperInclusive`
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

  /** Encapsulates an event stored in the SequencedEventStore.
    */
  sealed trait PossiblyIgnoredSequencedEvent[+Env <: Envelope[_]]
      extends HasTraceContext
      with PrettyPrinting
      with Product
      with Serializable {
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
    * If an ordinary sequenced event `oe` is later converted to an ignored event `ie`,
    * the actual event `oe.signedEvent` is retained as `ie.underlying` so that no information gets discarded by ignoring events.
    * If an ignored event `ie` is inserted as a placeholder for an event that has not been received, the underlying
    * event `ie.underlying` is left empty.
    */
  final case class IgnoredSequencedEvent[+Env <: Envelope[?]](
      override val timestamp: CantonTimestamp,
      override val counter: SequencerCounter,
      override val underlying: Option[SignedContent[SequencedEvent[Env]]],
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

  /** Encapsulates an event received by the sequencer.
    * It has been signed by the sequencer and contains a trace context.
    */
  final case class OrdinarySequencedEvent[+Env <: Envelope[_]](
      signedEvent: SignedContent[SequencedEvent[Env]]
  )(
      override val traceContext: TraceContext
  ) extends PossiblyIgnoredSequencedEvent[Env] {

    override def timestamp: CantonTimestamp = signedEvent.content.timestamp

    override def counter: SequencerCounter = signedEvent.content.counter

    override def underlyingEventBytes: Array[Byte] = signedEvent.toByteArray

    private[store] override def dbType: SequencedEventDbType = dbTypeOfEvent(signedEvent.content)

    override def isIgnored: Boolean = false

    def isTombstone: Boolean = signedEvent.content.isTombstone

    override def underlying: Some[SignedContent[SequencedEvent[Env]]] = Some(signedEvent)

    override def asIgnoredEvent: IgnoredSequencedEvent[Env] =
      IgnoredSequencedEvent(timestamp, counter, Some(signedEvent))(traceContext)

    override def asOrdinaryEvent: PossiblyIgnoredSequencedEvent[Env] = this

    override protected def pretty: Pretty[OrdinarySequencedEvent[Envelope[_]]] = prettyOfClass(
      param("signedEvent", _.signedEvent)
    )
  }

  object OrdinarySequencedEvent {
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
            .fromByteString(protocolVersion)(_)
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
