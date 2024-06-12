// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, PossiblyIgnoredSerializedEvent}
import com.digitalasset.canton.store.SequencedEventStore.*
import com.digitalasset.canton.store.{
  ChangeWouldResultInGap,
  SequencedEventNotFoundError,
  SequencedEventRangeOverlapsWithPruning,
  SequencedEventStore,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

/** In memory implementation of a [[SequencedEventStore]].
  */
class InMemorySequencedEventStore(protected val loggerFactory: NamedLoggerFactory)(implicit
    val ec: ExecutionContext
) extends SequencedEventStore
    with NamedLogging
    with InMemoryPrunableByTime {

  private val lock = new Object()

  /** Invariant:
    * The sequenced event stored at timestamp `ts` has timestamp `ts`.
    */
  private val eventByTimestamp: mutable.SortedMap[CantonTimestamp, PossiblyIgnoredSerializedEvent] =
    mutable.SortedMap.empty

  /** Invariants:
    * - The value set equals the key set of `eventsByTimestamp`.
    * - `eventsByTimestamp(timestampOfCounter(sc))` has sequencer counter `sc`
    */
  private val timestampOfCounter: mutable.SortedMap[SequencerCounter, CantonTimestamp] =
    mutable.SortedMap.empty

  def store(
      events: Seq[OrdinarySerializedEvent]
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[Unit] =
    NonEmpty.from(events).fold(Future.unit) { events =>
      logger.debug(
        show"Storing delivery events from ${events.head1.timestamp} to ${events.last1.timestamp}."
      )

      blocking(lock.synchronized {
        events.foreach { e =>
          eventByTimestamp.getOrElseUpdate(e.timestamp, e).discard
          timestampOfCounter.getOrElseUpdate(e.counter, e.timestamp).discard
        }
      })
      Future.unit
    }

  override def find(criterion: SequencedEventStore.SearchCriterion)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencedEventNotFoundError, PossiblyIgnoredSerializedEvent] = {

    logger.debug(s"Looking to retrieve delivery event $criterion")
    val resO = blocking(lock.synchronized {
      criterion match {
        case ByTimestamp(timestamp) =>
          eventByTimestamp.get(timestamp)
        case LatestUpto(inclusive) =>
          eventByTimestamp.rangeTo(inclusive).lastOption.map { case (_, event) => event }
      }
    })
    EitherT(Future.successful(resO.toRight(SequencedEventNotFoundError(criterion))))
  }

  override def findRange(criterion: RangeCriterion, limit: Option[Int])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencedEventRangeOverlapsWithPruning, Seq[
    PossiblyIgnoredSerializedEvent
  ]] = {
    logger.debug(s"Looking to retrieve delivery event $criterion")
    val res = blocking(lock.synchronized {
      criterion match {
        case ByTimestampRange(lowerInclusive, upperInclusive) =>
          val valuesInRangeIterable =
            eventByTimestamp.rangeFrom(lowerInclusive).rangeTo(upperInclusive).values
          // Copy the elements, as the returned iterator will otherwise explode if the underlying collection is modified.
          val result = limit.fold(valuesInRangeIterable)(valuesInRangeIterable.take).toList

          pruningStatusF.get match {
            case Some(pruningStatus) if pruningStatus.timestamp >= lowerInclusive =>
              Left(SequencedEventRangeOverlapsWithPruning(criterion, pruningStatus, result))
            case _ =>
              Right(result)
          }
      }
    })
    EitherT.fromEither[Future](res)
  }

  override def sequencedEvents(
      limit: Option[Int] = None
  )(implicit traceContext: TraceContext): Future[Seq[PossiblyIgnoredSerializedEvent]] =
    Future.successful(blocking(lock.synchronized {
      // Always copy the elements, as the returned iterator will otherwise explode if the underlying collection is modified.
      eventByTimestamp.values.take(limit.getOrElse(Int.MaxValue)).toList
    }))

  override def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Int] = Future.successful {
    val counter = new AtomicInteger(0)
    blocking(lock.synchronized {
      eventByTimestamp.rangeTo(beforeAndIncluding).foreach { case (ts, e) =>
        counter.incrementAndGet()
        eventByTimestamp.remove(ts).discard
        timestampOfCounter.remove(e.counter).discard
      }
    })
    counter.get()
  }

  override def ignoreEvents(fromInclusive: SequencerCounter, toInclusive: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    EitherT.fromEither {
      blocking(lock.synchronized {
        for {
          _ <- appendEmptyIgnoredEvents(fromInclusive, toInclusive)
        } yield {
          setIgnoreStatus(fromInclusive, toInclusive, ignore = true)
        }
      })
    }

  private def appendEmptyIgnoredEvents(from: SequencerCounter, to: SequencerCounter)(implicit
      traceContext: TraceContext
  ): Either[ChangeWouldResultInGap, Unit] = {
    val lastScAndTs = timestampOfCounter.lastOption

    val (firstSc, firstTs) = lastScAndTs match {
      case Some((lastSc, lastTs)) => (lastSc + 1, lastTs.immediateSuccessor)
      case None =>
        // Starting with MinValue.immediateSuccessor, because elsewhere we assume that MinValue is a strict lower bound on event timestamps.
        (from, CantonTimestamp.MinValue.immediateSuccessor)
    }

    if (from <= firstSc) {
      val timestamps = (firstSc to to).map { sc =>
        val ts = firstTs.addMicros(sc - firstSc)
        sc -> ts
      }.toMap
      timestampOfCounter.addAll(timestamps)

      val events = timestamps.map { case (sc, ts) =>
        ts -> IgnoredSequencedEvent(ts, sc, None, None)(traceContext)
      }
      eventByTimestamp.addAll(events)

      Right(())
    } else if (from > to) {
      Right(())
    } else {
      Left(ChangeWouldResultInGap(firstSc, from - 1))
    }
  }

  private def setIgnoreStatus(from: SequencerCounter, to: SequencerCounter, ignore: Boolean): Unit =
    if (from <= to) {
      val timestamps = timestampOfCounter.rangeFrom(from).rangeTo(to).values

      val newEvents = timestamps.map { ts =>
        val oldEvent = eventByTimestamp(ts)
        val newEvent = if (ignore) oldEvent.asIgnoredEvent else oldEvent.asOrdinaryEvent
        ts -> newEvent
      }.toMap

      eventByTimestamp.addAll(newEvents)
    }

  override def unignoreEvents(fromInclusive: SequencerCounter, toInclusive: SequencerCounter)(
      implicit traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    EitherT.fromEither {
      blocking(lock.synchronized {
        for {
          _ <- deleteEmptyIgnoredEvents(fromInclusive, toInclusive)
        } yield setIgnoreStatus(fromInclusive, toInclusive, ignore = false)
      })
    }

  private def deleteEmptyIgnoredEvents(
      from: SequencerCounter,
      to: SequencerCounter,
  ): Either[ChangeWouldResultInGap, Unit] = {
    val lastNonEmptyEventSc =
      timestampOfCounter
        .filter { case (_, ts) => eventByTimestamp(ts).underlying.isDefined }
        .lastOption
        .map { case (sc, _) => sc }

    val fromEffective = lastNonEmptyEventSc.fold(from)(c => (c + 1).max(from))

    val lastSc = timestampOfCounter.lastOption.map { case (sc, _) => sc }

    if (fromEffective <= to) {
      if (lastSc.forall(_ <= to)) {
        timestampOfCounter.rangeFrom(fromEffective).rangeTo(to).foreach { case (sc, ts) =>
          eventByTimestamp.remove(ts).discard
          timestampOfCounter.remove(sc).discard
        }
        Right(())

      } else {
        Left(ChangeWouldResultInGap(fromEffective, to))
      }
    } else {
      Right(())
    }
  }

  private[canton] override def delete(
      from: SequencerCounter
  )(implicit traceContext: TraceContext): Future[Unit] = {
    timestampOfCounter.rangeFrom(from).foreach { case (sc, ts) =>
      timestampOfCounter.remove(sc).discard
      eventByTimestamp.remove(ts).discard
    }

    Future.unit
  }

  override def close(): Unit = ()
}
