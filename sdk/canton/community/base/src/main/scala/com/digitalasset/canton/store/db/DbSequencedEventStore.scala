// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.CantonRequireTypes.String3
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.{SequencedEvent, SignedContent}
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, PossiblyIgnoredSerializedEvent}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.store.db.DbSequencedEventStore.*
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.{EitherTUtil, Thereafter}
import com.digitalasset.canton.version.ProtocolVersion
import slick.jdbc.{GetResult, SetParameter}

import java.util.concurrent.Semaphore
import scala.concurrent.{ExecutionContext, blocking}

class DbSequencedEventStore(
    override protected val storage: DbStorage,
    indexedSynchronizer: IndexedSynchronizer,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SequencedEventStore
    with DbStore
    with DbPrunableByTime {

  override protected[this] val partitionKey: IndexedSynchronizer = indexedSynchronizer

  override protected[this] def pruning_status_table: String = "common_sequenced_event_store_pruning"

  /** Semaphore to prevent concurrent writes to the db.
    * Concurrent calls can be problematic because they may introduce gaps in the stored sequencer counters.
    * The methods [[ignoreEvents]] and [[unignoreEvents]] are not meant to be executed concurrently.
    */
  private val semaphore: Semaphore = new Semaphore(1)

  private def withLock[F[_], A](caller: String)(body: => F[A])(implicit
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

  import com.digitalasset.canton.store.SequencedEventStore.*
  import storage.api.*
  import storage.converters.*

  implicit val getResultPossiblyIgnoredSequencedEvent: GetResult[PossiblyIgnoredSerializedEvent] =
    GetResult { r =>
      val typ = r.<<[SequencedEventDbType]
      val sequencerCounter = r.<<[SequencerCounter]
      val timestamp = r.<<[CantonTimestamp]
      val eventBytes = r.<<[Array[Byte]]
      val traceContext: TraceContext = r.<<[SerializableTraceContext].unwrap
      val ignore = r.<<[Boolean]

      typ match {
        case SequencedEventDbType.IgnoredEvent =>
          IgnoredSequencedEvent(timestamp, sequencerCounter, None)(
            traceContext
          )
        case _ =>
          val signedEvent = SignedContent
            .fromTrustedByteArray(eventBytes)
            .flatMap(
              _.deserializeContent(SequencedEvent.fromByteString(protocolVersion, _))
            )
            .valueOr(err =>
              throw new DbDeserializationException(s"Failed to deserialize sequenced event: $err")
            )
          if (ignore) {
            IgnoredSequencedEvent(
              timestamp,
              sequencerCounter,
              Some(signedEvent),
            )(
              traceContext
            )
          } else {
            OrdinarySequencedEvent(signedEvent)(
              traceContext
            )
          }
      }
    }

  private implicit val traceContextSetParameter: SetParameter[SerializableTraceContext] =
    SerializableTraceContext.getVersionedSetParameter(protocolVersion)

  override def store(
      events: Seq[OrdinarySerializedEvent]
  )(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] =
    if (events.isEmpty) FutureUnlessShutdown.unit
    else {
      withLock(functionFullName) {
        CloseContext.withCombinedContext(closeContext, externalCloseContext, timeouts, logger) {
          combinedCloseContext =>
            storage
              .queryAndUpdate(bulkInsertQuery(events), functionFullName)(
                traceContext,
                combinedCloseContext,
              )
              .void
        }
      }
    }

  private def bulkInsertQuery(
      events: Seq[PossiblyIgnoredSerializedEvent]
  )(implicit traceContext: TraceContext): DBIOAction[Unit, NoStream, Effect.All] = {
    val insertSql =
      "insert into common_sequenced_events (synchronizer_idx, ts, sequenced_event, type, sequencer_counter, trace_context, ignore) " +
        "values (?, ?, ?, ?, ?, ?, ?) " +
        "on conflict do nothing"
    DbStorage.bulkOperation_(insertSql, events, storage.profile) { pp => event =>
      pp >> partitionKey
      pp >> event.timestamp
      pp >> event.underlyingEventBytes
      pp >> event.dbType
      pp >> event.counter
      pp >> SerializableTraceContext(event.traceContext)
      pp >> event.isIgnored
    }
  }

  override def find(criterion: SequencedEventStore.SearchCriterion)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencedEventNotFoundError, PossiblyIgnoredSerializedEvent] = {
    val query = criterion match {
      case ByTimestamp(timestamp) =>
        // The implementation assumes that we timestamps on sequenced events increases monotonically with the sequencer counter
        // It therefore is fine to take the first event that we find.
        sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore from common_sequenced_events
                where synchronizer_idx = $partitionKey and ts = $timestamp"""
      case LatestUpto(inclusive) =>
        sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore from common_sequenced_events
                where synchronizer_idx = $partitionKey and ts <= $inclusive
                order by ts desc #${storage.limit(1)}"""
    }

    storage
      .querySingle(
        query.as[PossiblyIgnoredSerializedEvent].headOption,
        functionFullName,
      )
      .toRight(SequencedEventNotFoundError(criterion))
  }

  override def findRange(criterion: SequencedEventStore.RangeCriterion, limit: Option[Int])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencedEventRangeOverlapsWithPruning, Seq[
    PossiblyIgnoredSerializedEvent
  ]] =
    EitherT {
      criterion match {
        case ByTimestampRange(lowerInclusive, upperInclusive) =>
          for {
            events <- storage.query(
              sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore from common_sequenced_events
                    where synchronizer_idx = $partitionKey and $lowerInclusive <= ts  and ts <= $upperInclusive
                    order by ts #${limit.fold("")(storage.limit(_))}"""
                .as[PossiblyIgnoredSerializedEvent],
              functionFullName,
            )
            // check for pruning after we've read the events so that we certainly catch the case
            // if pruning is started while we're reading (as we're not using snapshot isolation here)
            pruningO <- pruningStatus
          } yield pruningO match {
            case Some(pruningStatus) if pruningStatus.timestamp >= lowerInclusive =>
              Left(SequencedEventRangeOverlapsWithPruning(criterion, pruningStatus, events))
            case _ =>
              Right(events)
          }
      }
    }

  override def sequencedEvents(
      limit: Option[Int] = None
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[PossiblyIgnoredSerializedEvent]] =
    storage.query(
      sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore from common_sequenced_events
              where synchronizer_idx = $partitionKey
              order by ts #${limit.fold("")(storage.limit(_))}"""
        .as[PossiblyIgnoredSerializedEvent],
      functionFullName,
    )

  override protected[canton] def doPrune(
      untilInclusive: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    val query =
      sqlu"delete from common_sequenced_events where synchronizer_idx = $partitionKey and ts <= $untilInclusive"
    storage
      .queryAndUpdate(query, functionFullName)
      .map { nrPruned =>
        logger.info(
          s"Pruned at least $nrPruned entries from the sequenced event store of synchronizer_idx $partitionKey older or equal to $untilInclusive"
        )
        nrPruned
      }
  }

  override def ignoreEvents(fromInclusive: SequencerCounter, toInclusive: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ChangeWouldResultInGap, Unit] =
    withLock(functionFullName) {
      for {
        _ <- appendEmptyIgnoredEvents(fromInclusive, toInclusive)
        _ <- EitherT.right(setIgnoreStatus(fromInclusive, toInclusive, ignore = true))
      } yield ()
    }

  private def appendEmptyIgnoredEvents(
      fromInclusive: SequencerCounter,
      untilInclusive: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ChangeWouldResultInGap, Unit] =
    for {
      lastSequencerCounterAndTimestampO <- EitherT.right(
        storage.query(
          sql"""select sequencer_counter, ts from common_sequenced_events where synchronizer_idx = $partitionKey
               order by sequencer_counter desc #${storage.limit(1)}"""
            .as[(SequencerCounter, CantonTimestamp)]
            .headOption,
          functionFullName,
        )
      )

      (firstSc, firstTs) = lastSequencerCounterAndTimestampO match {
        case Some((lastSc, lastTs)) => (lastSc + 1, lastTs.immediateSuccessor)
        case None =>
          // Starting with MinValue.immediateSuccessor, because elsewhere we assume that MinValue is a strict lower bound on event timestamps.
          (fromInclusive, CantonTimestamp.MinValue.immediateSuccessor)
      }

      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        fromInclusive <= firstSc || fromInclusive > untilInclusive,
        ChangeWouldResultInGap(firstSc, fromInclusive - 1),
      )

      events = ((firstSc max fromInclusive) to untilInclusive).map { sc =>
        val ts = firstTs.addMicros(sc - firstSc)
        IgnoredSequencedEvent(ts, sc, None)(traceContext)
      }

      _ <- EitherT.right(
        storage.queryAndUpdate(bulkInsertQuery(events), functionFullName)
      )
    } yield ()

  private def setIgnoreStatus(
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      ignore: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"update common_sequenced_events set ignore = $ignore where synchronizer_idx = $partitionKey and $fromInclusive <= sequencer_counter and sequencer_counter <= $toInclusive",
      functionFullName,
    )

  override def unignoreEvents(fromInclusive: SequencerCounter, toInclusive: SequencerCounter)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ChangeWouldResultInGap, Unit] =
    withLock(functionFullName) {
      for {
        _ <- deleteEmptyIgnoredEvents(fromInclusive, toInclusive)
        _ <- EitherT.right(setIgnoreStatus(fromInclusive, toInclusive, ignore = false))
      } yield ()
    }

  private def deleteEmptyIgnoredEvents(from: SequencerCounter, to: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ChangeWouldResultInGap, Unit] =
    for {
      lastNonEmptyEventSequencerCounter <- EitherT.right(
        storage.query(
          sql"""select sequencer_counter from common_sequenced_events
              where synchronizer_idx = $partitionKey and type != ${SequencedEventDbType.IgnoredEvent}
              order by sequencer_counter desc #${storage.limit(1)}"""
            .as[SequencerCounter]
            .headOption,
          functionFullName,
        )
      )

      fromEffective = lastNonEmptyEventSequencerCounter.fold(from)(c => (c + 1) max from)

      lastSequencerCounter <- EitherT.right(
        storage.query(
          sql"""select sequencer_counter from common_sequenced_events
              where synchronizer_idx = $partitionKey
              order by sequencer_counter desc #${storage.limit(1)}"""
            .as[SequencerCounter]
            .headOption,
          functionFullName,
        )
      )

      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        lastSequencerCounter.forall(_ <= to) || fromEffective > to,
        ChangeWouldResultInGap(fromEffective, to),
      )

      _ <- EitherT.right(
        storage.update(
          sqlu"""delete from common_sequenced_events
               where synchronizer_idx = $partitionKey and type = ${SequencedEventDbType.IgnoredEvent}
                 and $fromEffective <= sequencer_counter and sequencer_counter <= $to""",
          functionFullName,
        )
      )
    } yield ()

  private[canton] override def delete(
      fromInclusive: SequencerCounter
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"delete from common_sequenced_events where synchronizer_idx = $partitionKey and sequencer_counter >= $fromInclusive",
      functionFullName,
    )

  override def traceContext(sequencedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TraceContext]] = {
    val query =
      sql"""select trace_context
            from common_sequenced_events
             where synchronizer_idx = $partitionKey
               and ts = $sequencedTimestamp"""
    storage
      .querySingle(
        query.as[SerializableTraceContext].headOption,
        functionFullName,
      )
      .map(_.unwrap)
      .value
  }
}

object DbSequencedEventStore {
  sealed trait SequencedEventDbType {
    val name: String3
  }

  object SequencedEventDbType {

    case object Deliver extends SequencedEventDbType {
      override val name: String3 = String3.tryCreate("del")
    }

    case object DeliverError extends SequencedEventDbType {
      override val name: String3 = String3.tryCreate("err")
    }

    case object IgnoredEvent extends SequencedEventDbType {
      override val name: String3 = String3.tryCreate("ign")
    }

    implicit val setParameterSequencedEventType: SetParameter[SequencedEventDbType] = (v, pp) =>
      pp >> v.name

    implicit val getResultSequencedEventType: GetResult[SequencedEventDbType] = GetResult(r =>
      r.nextString() match {
        case Deliver.name.str => Deliver
        case DeliverError.name.str => DeliverError
        case IgnoredEvent.name.str => IgnoredEvent
        case unknown =>
          throw new DbDeserializationException(s"Unknown sequenced event type [$unknown]")
      }
    )
  }
}
