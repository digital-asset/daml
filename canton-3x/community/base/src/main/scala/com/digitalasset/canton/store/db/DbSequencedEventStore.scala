// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.{
  SequencedEvent,
  SequencedEventTrafficState,
  SignedContent,
}
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, PossiblyIgnoredSerializedEvent}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.*
import com.digitalasset.canton.store.db.DbSequencedEventStore.*
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.{EitherTUtil, Thereafter}
import com.digitalasset.canton.version.{ProtocolVersion, UntypedVersionedMessage, VersionedMessage}
import slick.jdbc.{GetResult, SetParameter}

import java.util.concurrent.Semaphore
import scala.concurrent.{ExecutionContext, Future, blocking}

class DbSequencedEventStore(
    override protected val storage: DbStorage,
    client: SequencerClientDiscriminator,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SequencedEventStore
    with DbStore
    with DbPrunableByTime[SequencerClientDiscriminator] {

  override protected[this] val partitionKey: SequencerClientDiscriminator = client

  override protected[this] def partitionColumn: String = "client"

  override protected[this] def pruning_status_table: String = "sequenced_event_store_pruning"

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

  override protected[this] implicit def setParameterDiscriminator
      : SetParameter[SequencerClientDiscriminator] =
    SequencerClientDiscriminator.setClientDiscriminatorParameter

  import com.digitalasset.canton.store.SequencedEventStore.*
  import storage.api.*
  import storage.converters.*

  override protected val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("sequenced-event-store")

  implicit val getResultPossiblyIgnoredSequencedEvent: GetResult[PossiblyIgnoredSerializedEvent] =
    GetResult { r =>
      val typ = r.<<[SequencedEventDbType]
      val sequencerCounter = r.<<[SequencerCounter]
      val timestamp = r.<<[CantonTimestamp]
      val eventBytes = r.<<[Array[Byte]]
      val traceContext: TraceContext = r.<<[SerializableTraceContext].unwrap
      val ignore = r.<<[Boolean]

      val getTrafficState =
        SequencedEventTrafficState.sequencedEventTrafficStateGetResult(r)

      typ match {
        case SequencedEventDbType.IgnoredEvent =>
          IgnoredSequencedEvent(timestamp, sequencerCounter, None, getTrafficState)(
            traceContext
          )
        case _ =>
          val signedEvent = ProtoConverter
            .protoParserArray(UntypedVersionedMessage.parseFrom)(eventBytes)
            .map(VersionedMessage.apply)
            .flatMap(SignedContent.fromProtoVersioned(_))
            .flatMap(_.deserializeContent(SequencedEvent.fromByteString))
            .valueOr(err =>
              throw new DbDeserializationException(s"Failed to deserialize sequenced event: $err")
            )
          if (ignore) {
            IgnoredSequencedEvent(
              timestamp,
              sequencerCounter,
              Some(signedEvent),
              getTrafficState,
            )(
              traceContext
            )
          } else {
            OrdinarySequencedEvent(signedEvent, getTrafficState)(
              traceContext
            )
          }
      }
    }

  private implicit val traceContextSetParameter: SetParameter[SerializableTraceContext] =
    SerializableTraceContext.getVersionedSetParameter(protocolVersion)

  override def store(
      events: Seq[OrdinarySerializedEvent]
  )(implicit traceContext: TraceContext, externalCloseContext: CloseContext): Future[Unit] = {

    if (events.isEmpty) Future.unit
    else
      processingTime.event {
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
  }

  private def bulkInsertQuery(
      events: Seq[PossiblyIgnoredSerializedEvent]
  )(implicit traceContext: TraceContext): DBIOAction[Unit, NoStream, Effect.All] = {
    val insertSql = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( sequenced_events ( ts, client ) ) */
            |into sequenced_events
            |using (select ? client, ? ts from dual) input
            |on (sequenced_events.ts = input.ts and sequenced_events.client = input.client)
            |when not matched then
            |  insert (client, ts, sequenced_event, type, sequencer_counter, trace_context, ignore, extra_traffic_remainder, extra_traffic_consumed)
            |  values (input.client, input.ts, ?, ?, ?, ?, ?, ?, ?)""".stripMargin

      case _ =>
        "insert into sequenced_events (client, ts, sequenced_event, type, sequencer_counter, trace_context, ignore, extra_traffic_remainder, extra_traffic_consumed) " +
          "values (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
          "on conflict do nothing"
    }
    DbStorage.bulkOperation_(insertSql, events, storage.profile) { pp => event =>
      pp >> partitionKey
      pp >> event.timestamp
      pp >> event.underlyingEventBytes
      pp >> event.dbType
      pp >> event.counter
      pp >> SerializableTraceContext(event.traceContext)
      pp >> event.isIgnored
      pp >> event.trafficState.map(_.extraTrafficRemainder)
      pp >> event.trafficState.map(_.extraTrafficConsumed)
    }
  }

  override def find(criterion: SequencedEventStore.SearchCriterion)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencedEventNotFoundError, PossiblyIgnoredSerializedEvent] =
    processingTime.eitherTEvent {
      val query = criterion match {
        case ByTimestamp(timestamp) =>
          // The implementation assumes that we timestamps on sequenced events increases monotonically with the sequencer counter
          // It therefore is fine to take the first event that we find.
          sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore, extra_traffic_remainder, extra_traffic_consumed from sequenced_events
                where client = $partitionKey and ts = $timestamp"""
        case LatestUpto(inclusive) =>
          sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore, extra_traffic_remainder, extra_traffic_consumed from sequenced_events
                where client = $partitionKey and ts <= $inclusive
                order by ts desc #${storage.limit(1)}"""
      }

      storage
        .querySingle(query.as[PossiblyIgnoredSerializedEvent].headOption, functionFullName)
        .toRight(SequencedEventNotFoundError(criterion))
    }

  override def findRange(criterion: SequencedEventStore.RangeCriterion, limit: Option[Int])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencedEventRangeOverlapsWithPruning, Seq[PossiblyIgnoredSerializedEvent]] =
    EitherT(processingTime.event {
      criterion match {
        case ByTimestampRange(lowerInclusive, upperInclusive) =>
          for {
            events <- storage.query(
              sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore, extra_traffic_remainder, extra_traffic_consumed from sequenced_events
                    where client = $partitionKey and $lowerInclusive <= ts  and ts <= $upperInclusive
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
    })

  override def sequencedEvents(
      limit: Option[Int] = None
  )(implicit traceContext: TraceContext): Future[Seq[PossiblyIgnoredSerializedEvent]] = {
    processingTime.event {
      storage.query(
        sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore, extra_traffic_remainder, extra_traffic_consumed from sequenced_events
              where client = $partitionKey
              order by ts #${limit.fold("")(storage.limit(_))}"""
          .as[PossiblyIgnoredSerializedEvent],
        functionFullName,
      )
    }
  }

  override protected[canton] def doPrune(
      untilInclusive: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
      val query =
        sqlu"delete from sequenced_events where client = $partitionKey and ts <= $untilInclusive"
      storage
        .update(query, functionFullName)
        .map { nrPruned =>
          logger.info(
            s"Pruned at least $nrPruned entries from the sequenced event store of client $partitionKey older or equal to $untilInclusive"
          )
        }
    }

  override def ignoreEvents(fromInclusive: SequencerCounter, untilInclusive: SequencerCounter)(
      implicit traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    withLock(functionFullName) {
      for {
        _ <- appendEmptyIgnoredEvents(fromInclusive, untilInclusive)
        _ <- EitherT.right(setIgnoreStatus(fromInclusive, untilInclusive, ignore = true))
      } yield ()
    }

  private def appendEmptyIgnoredEvents(
      fromInclusive: SequencerCounter,
      untilInclusive: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    processingTime.eitherTEvent {
      for {
        lastSequencerCounterAndTimestampO <- EitherT.right(
          storage.query(
            sql"""select sequencer_counter, ts from sequenced_events where client = $partitionKey
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

        _ <- EitherTUtil.condUnitET[Future](
          fromInclusive <= firstSc || fromInclusive > untilInclusive,
          ChangeWouldResultInGap(firstSc, fromInclusive - 1),
        )

        events = ((firstSc max fromInclusive) to untilInclusive).map { sc =>
          val ts = firstTs.addMicros(sc - firstSc)
          IgnoredSequencedEvent(ts, sc, None, None)(traceContext)
        }

        _ <- EitherT.right(storage.queryAndUpdate(bulkInsertQuery(events), functionFullName))
      } yield ()
    }

  private def setIgnoreStatus(
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      ignore: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = processingTime.event {
    storage.update_(
      sqlu"update sequenced_events set ignore = $ignore where client = $partitionKey and $fromInclusive <= sequencer_counter and sequencer_counter <= $toInclusive",
      functionFullName,
    )
  }

  override def unignoreEvents(from: SequencerCounter, to: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    withLock(functionFullName) {
      for {
        _ <- deleteEmptyIgnoredEvents(from, to)
        _ <- EitherT.right(setIgnoreStatus(from, to, ignore = false))
      } yield ()
    }

  private def deleteEmptyIgnoredEvents(from: SequencerCounter, to: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    processingTime.eitherTEvent {
      for {
        lastNonEmptyEventSequencerCounter <- EitherT.right(
          storage.query(
            sql"""select sequencer_counter from sequenced_events
              where client = $partitionKey and type != ${SequencedEventDbType.IgnoredEvent}
              order by sequencer_counter desc #${storage.limit(1)}"""
              .as[SequencerCounter]
              .headOption,
            functionFullName,
          )
        )

        fromEffective = lastNonEmptyEventSequencerCounter.fold(from)(c => (c + 1) max from)

        lastSequencerCounter <- EitherT.right(
          storage.query(
            sql"""select sequencer_counter from sequenced_events
              where client = $partitionKey
              order by sequencer_counter desc #${storage.limit(1)}"""
              .as[SequencerCounter]
              .headOption,
            functionFullName,
          )
        )

        _ <- EitherTUtil.condUnitET[Future](
          lastSequencerCounter.forall(_ <= to) || fromEffective > to,
          ChangeWouldResultInGap(fromEffective, to),
        )

        _ <- EitherT.right(
          storage.update(
            sqlu"""delete from sequenced_events
               where client = $partitionKey and type = ${SequencedEventDbType.IgnoredEvent}
                 and $fromEffective <= sequencer_counter and sequencer_counter <= $to""",
            functionFullName,
          )
        )
      } yield ()
    }

  private[canton] override def delete(
      from: SequencerCounter
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
      storage.update_(
        sqlu"delete from sequenced_events where client = $partitionKey and sequencer_counter >= $from",
        functionFullName,
      )
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
