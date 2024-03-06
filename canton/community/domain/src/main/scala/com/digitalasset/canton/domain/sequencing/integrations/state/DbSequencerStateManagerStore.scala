// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.EphemeralState
import com.digitalasset.canton.domain.protocol.v30
import com.digitalasset.canton.domain.sequencing.integrations.state.SequencerStateManagerStore.PruningResult
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.store.SaveLowerBoundError
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.DbAction.ReadOnly
import com.digitalasset.canton.resource.DbStorage.{DbAction, dbEitherT}
import com.digitalasset.canton.resource.IdempotentInsert.insertVerifyingConflicts
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.{Member, UnauthenticatedMemberId}
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.{ErrorUtil, RangeUtil}
import com.digitalasset.canton.version.*
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import slick.jdbc.{SetParameter, TransactionIsolation}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/** Database store for server side sequencer data.
  * If you need more than one sequencer running on the same db, you can isolate them using
  * different sequencerStoreIds. This is useful for tests and for sequencer applications that implement multiple domains.
  */
class DbSequencerStateManagerStore(
    override protected val storage: DbStorage,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    maxBatchSize: Int = 1000,
)(implicit ec: ExecutionContext)
    extends SequencerStateManagerStore
    with DbStore {

  import DbSequencerStateManagerStore.*
  import DbStorage.Implicits.*
  import Member.DbStorageImplicits.*
  import storage.api.*
  import storage.converters.*

  private implicit val setParameterTraceContext: SetParameter[SerializableTraceContext] =
    SerializableTraceContext.getVersionedSetParameter(protocolVersion)

  override def readAtBlockTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[EphemeralState] =
    storage.query(readAtBlockTimestampDBIO(timestamp), functionFullName)

  /** Compute the state up until (inclusive) the given timestamp. */
  def readAtBlockTimestampDBIO(
      timestamp: CantonTimestamp
  ): DBIOAction[EphemeralState, NoStream, Effect.Read with Effect.Transactional] = {
    val membersQ =
      sql"select member, added_at, enabled, latest_acknowledgement from seq_state_manager_members where added_at <= $timestamp order by added_at asc, member asc"
        .as[(Member, CantonTimestamp, Boolean, Option[CantonTimestamp])]

    /* We have an index on (storeId, member, counter) so fetching the max counter for each member in this fashion
     * is significantly quicker than using a window function or another approach.
     */
    val countersWithTrafficStateQ = {
      (sql"""select latest_counters.member, latest_counters.counter, latest_counters.extra_traffic_remainder, latest_counters.extra_traffic_consumed, latest_counters.base_traffic_remainder, latest_counters.ts
             from (
                select members.member,
                       (select counter from seq_state_manager_events
                        where member = members.member and ts <= $timestamp
                        order by counter desc #${storage.limit(1)}) counter,
                        (select extra_traffic_remainder from seq_state_manager_events
                        where member = members.member and ts <= $timestamp
                        order by counter desc #${storage.limit(1)}) extra_traffic_remainder,
                        (select extra_traffic_consumed from seq_state_manager_events
                        where member = members.member and ts <= $timestamp
                        order by counter desc #${storage.limit(1)}) extra_traffic_consumed,
                        (select base_traffic_remainder from seq_state_manager_events
                        where member = members.member and ts <= $timestamp
                        order by counter desc #${storage.limit(1)}) base_traffic_remainder,
                        (select ts from seq_state_manager_events
                        where member = members.member and ts <= $timestamp
                        order by counter desc #${storage.limit(1)}) ts
                from seq_state_manager_members members
             ) latest_counters
             where latest_counters.counter is not null
          """)
        .as[(Member, SequencerCounter, Option[TrafficState])]
    }

    // Prefer `zip` over a `for` comprehension to tell Slick that all queries are independent of each other.
    fetchLowerBoundDBIO()
      .zip(membersQ)
      .zip(countersWithTrafficStateQ)
      .zip(readAggregationsAtBlockTimestamp(timestamp))
      .map { case (((lowerBound, members), countersAndMaybeTraffic), inFlightAggregations) =>
        val counterMap = countersAndMaybeTraffic.map { case (member, counter, _) =>
          member -> counter
        }.toMap
        val trafficMap = countersAndMaybeTraffic.flatMap { case (member, _, traffic) =>
          traffic.map(member -> _)
        }.toMap
        EphemeralState(
          counterMap,
          inFlightAggregations,
          InternalSequencerPruningStatus(
            lowerBound.getOrElse(CantonTimestamp.Epoch),
            toMemberStatusSeq(members),
          ),
          trafficState = trafficMap,
        )
      }
      // we don't expect the sequencer to be writing at the point this query is done, but it can't hurt
      .transactionally
  }

  private[domain] def readAggregationsAtBlockTimestamp(
      timestamp: CantonTimestamp
  ): DbAction.ReadOnly[InFlightAggregations] = {
    val aggregationsQ =
      sql"""
            select seq_in_flight_aggregation.aggregation_id,
                   seq_in_flight_aggregation.max_sequencing_time,
                   seq_in_flight_aggregation.aggregation_rule,
                   seq_in_flight_aggregated_sender.sender,
                   seq_in_flight_aggregated_sender.sequencing_timestamp,
                   seq_in_flight_aggregated_sender.signatures
            from seq_in_flight_aggregation inner join seq_in_flight_aggregated_sender on seq_in_flight_aggregation.aggregation_id = seq_in_flight_aggregated_sender.aggregation_id
            where seq_in_flight_aggregation.max_sequencing_time > $timestamp and seq_in_flight_aggregated_sender.sequencing_timestamp <= $timestamp
          """.as[
        (
            AggregationId,
            CantonTimestamp,
            AggregationRule,
            Member,
            CantonTimestamp,
            AggregatedSignaturesOfSender,
        )
      ]
    aggregationsQ.map { aggregations =>
      val byAggregationId = aggregations.groupBy { case (aggregationId, _, _, _, _, _) =>
        aggregationId
      }
      byAggregationId.fmap { aggregationsForId =>
        val aggregationsNE = NonEmptyUtil.fromUnsafe(aggregationsForId)
        val (_, maxSequencingTimestamp, aggregationRule, _, _, _) =
          aggregationsNE.head1
        val aggregatedSenders = aggregationsNE.map {
          case (_, _, _, sender, sequencingTimestamp, signatures) =>
            sender -> AggregationBySender(sequencingTimestamp, signatures.signaturesByEnvelope)
        }.toMap
        InFlightAggregation
          .create(aggregatedSenders, maxSequencingTimestamp, aggregationRule)
          .valueOr(err => throw new DbDeserializationException(err))
      }
    }
  }

  override def readRange(
      member: Member,
      startInclusive: SequencerCounter,
      endExclusive: SequencerCounter,
  )(implicit traceContext: TraceContext): Source[OrdinarySerializedEvent, NotUsed] = {
    ErrorUtil.requireArgument(
      startInclusive < endExclusive,
      "startInclusive must be less than endExclusive",
    )

    Source(
      RangeUtil.partitionIndexRange(startInclusive.v, endExclusive.v, maxBatchSize.toLong)
    )
      .mapAsync(1) { case (batchStartInclusive, batchEndExclusive) =>
        storage.query(
          sql"""
            select counter, ts, content, trace_context, extra_traffic_remainder, extra_traffic_consumed
            from seq_state_manager_events
            where member = $member and counter >= $batchStartInclusive and counter < $batchEndExclusive
            order by counter asc"""
            .as[
              (
                  SequencerCounter,
                  CantonTimestamp,
                  Array[Byte],
                  SerializableTraceContext,
                  Option[SequencedEventTrafficState],
              )
            ],
          functionFullName,
        )
      }
      .mapConcat(identity)
      .map {
        case (
              counter,
              timestamp,
              serializedEvent,
              traceContext,
              sequencedEventTrafficStateOpt,
            ) =>
          val event = deserializeEvent(serializedEvent)

          // just sanity check the values in the serialized event match the counter and ts values stored alongside it in the database
          if (counter != event.content.counter)
            throw new DbDeserializationException(
              s"Serialized counter does not match db counter  [serialized:${event.content.counter},db:$counter]"
            )
          if (timestamp != event.content.timestamp)
            throw new DbDeserializationException(
              s"Serialized timestamp does not match db timestamp [serialized:${event.content.timestamp},db:$timestamp]"
            )

          OrdinarySequencedEvent(event, sequencedEventTrafficStateOpt)(
            traceContext.unwrap
          )
      }
  }

  private[this] def deserializeEvent(
      bytes: Array[Byte]
  ): SignedContent[SequencedEvent[ClosedEnvelope]] =
    SignedContent
      .fromByteArrayUnsafe(bytes)
      .flatMap(_.deserializeContent(SequencedEvent.fromByteString(protocolVersion)))
      .valueOr(err =>
        throw new DbDeserializationException(s"Failed to deserialize signed deliver event: $err")
      )

  private[domain] def readEventsInTimeRange(
      startTsExclusive: CantonTimestamp,
      endTsInclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Future[Map[Member, NonEmpty[Seq[OrdinarySerializedEvent]]]] = {
    val query =
      sql"""
            select member, content, trace_context
            from seq_state_manager_events
            where ts > $startTsExclusive and ts <= $endTsInclusive
        """.as[(Member, Array[Byte], SerializableTraceContext)]
    for {
      events <- (storage.query(query, functionFullName))
    } yield {
      events
        .map { case (member, bytes, eventTraceContext) =>
          member -> OrdinarySequencedEvent(deserializeEvent(bytes), None)(eventTraceContext.unwrap)
        }
        .groupBy(_._1)
        .fmap { eventsForMember => NonEmptyUtil.fromUnsafe(eventsForMember.map(_._2)) }
    }
  }

  private[domain] def readRegistrationsInTimeRange(
      startTsExclusive: CantonTimestamp,
      endTsInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Seq[(Member, CantonTimestamp)]] = {
    val query =
      sql"""
            select member, added_at from seq_state_manager_members
            where added_at > $startTsExclusive and added_at <= $endTsInclusive
           """.as[(Member, CantonTimestamp)]
    (storage.query(query, functionFullName))
  }

  override def addMember(member: Member, addedAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    storage.queryAndUpdate(addMemberDBIO(member, addedAt), functionFullName)
  }

  def addMemberDBIO(member: Member, addedAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): DbAction.All[Unit] =
    insertVerifyingConflicts(
      storage,
      "seq_state_manager_members ( member )",
      sql"seq_state_manager_members (member, added_at) values ($member, $addedAt)",
      sql"select added_at from seq_state_manager_members where member = $member"
        .as[CantonTimestamp]
        .head,
    )(
      _ == addedAt,
      existingAddedAt =>
        s"Member [$member] has existing added at value of [$existingAddedAt] but we are attempting to insert [$addedAt]",
    )

  override def addEvents(
      events: Map[Member, OrdinarySerializedEvent],
      trafficSate: Map[Member, TrafficState],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    storage.queryAndUpdate(addEventsDBIO(trafficSate)(events), functionFullName)
  }

  def addEventsDBIO(trafficState: Map[Member, TrafficState])(
      events: Map[Member, OrdinarySerializedEvent]
  )(implicit batchTraceContext: TraceContext): DbAction.All[Unit] = {
    ErrorUtil.requireArgument(
      events.values.map(_.counter).forall(_ >= SequencerCounter.Genesis),
      "all counters must be greater or equal to the genesis counter",
    )

    ErrorUtil.requireArgument(
      events.values.map(_.timestamp).toSet.sizeCompare(1) <= 0,
      "events should all be for the same timestamp",
    )

    def insertBuilder(member: Member, storedEvent: StoredEvent) = {
      val state = trafficState.get(member)
      sql"""seq_state_manager_events (member, counter, ts, content, trace_context, extra_traffic_remainder, extra_traffic_consumed, base_traffic_remainder)
              values (
                $member, ${storedEvent.counter}, ${storedEvent.timestamp}, ${storedEvent.content},
                ${SerializableTraceContext(storedEvent.traceContext)},
                ${state.map(_.extraTrafficRemainder.value)},
                ${state.map(_.extraTrafficConsumed.value)},
                ${state.map(_.baseTrafficRemainder.value)}
          )"""
    }

    val inserts = events.fmap(StoredEvent.create).map { case (member, storedEvent) =>
      insertVerifyingConflicts(
        storage,
        "seq_state_manager_events ( member, counter )",
        insertBuilder(member, storedEvent),
        sql"""select ts
              from seq_state_manager_events
              where  member = $member and counter = ${storedEvent.counter}"""
          .as[CantonTimestamp]
          .head,
      )(
        _ == storedEvent.timestamp,
        existingTimestamp =>
          s"Existing event for [$member@${storedEvent.counter}] has timestamp $existingTimestamp but attempting to insert ${storedEvent.timestamp}",
      )
    }

    DBIO.seq(inserts.toSeq*).transactionally
  }

  override def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = storage.queryAndUpdate(
    acknowledgeDBIO(member, timestamp),
    functionFullName,
  )

  def acknowledgeDBIO(member: Member, timestamp: CantonTimestamp): DbAction.WriteOnly[Unit] =
    for {
      _ <-
        sqlu"""
         update seq_state_manager_members set latest_acknowledgement = $timestamp
         where member = $member and (latest_acknowledgement < $timestamp or latest_acknowledgement is null)
         """
    } yield ()

  override def disableMember(member: Member)(implicit traceContext: TraceContext): Future[Unit] =
    storage.update_(
      disableMemberDBIO(member),
      functionFullName,
    )

  def disableMemberDBIO(member: Member): DbAction.WriteOnly[Unit] = for {
    _ <- sqlu"update seq_state_manager_members set enabled = ${false} where member = $member"
  } yield ()

  def unregisterUnauthenticatedMember(
      member: UnauthenticatedMemberId
  ): DbAction.WriteOnly[Unit] = for {
    _ <-
      sqlu"delete from seq_state_manager_events where member = $member"
    _ <-
      sqlu"delete from seq_state_manager_members where member = $member"
  } yield ()

  override def isEnabled(member: Member)(implicit traceContext: TraceContext): Future[Boolean] =
    storage
      .query(
        sql"select enabled from seq_state_manager_members where member = $member"
          .as[Boolean]
          .headOption,
        s"$functionFullName:isMemberEnabled",
      )
      .map(_.getOrElse(false))

  override protected[state] def latestAcknowledgements()(implicit
      traceContext: TraceContext
  ): Future[Map[Member, CantonTimestamp]] = storage
    .query(
      sql"""
                  select member, latest_acknowledgement
                  from seq_state_manager_members
           """.as[(Member, Option[CantonTimestamp])],
      functionFullName,
    )
    .map(_.collect { case (member, Some(timestamp)) => member -> timestamp })
    .map(_.toMap)

  private def fetchLowerBoundDBIO(): ReadOnly[Option[CantonTimestamp]] =
    sql"select ts from seq_state_manager_lower_bound".as[CantonTimestamp].headOption

  override def fetchLowerBound()(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    storage.querySingle(fetchLowerBoundDBIO(), "fetchLowerBound").value

  override def saveLowerBound(
      ts: CantonTimestamp,
      maybeOnboardingTopologyTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SaveLowerBoundError, Unit] = EitherT(
    storage.queryAndUpdate(
      saveLowerBoundDBIO(ts, maybeOnboardingTopologyTimestamp),
      "saveLowerBound",
    )
  )

  def saveLowerBoundDBIO(
      eventsReadableStartingAt: CantonTimestamp,
      maybeOnboardingTopologyTimestamp: Option[CantonTimestamp] = None,
  ): DbAction.All[Either[SaveLowerBoundError, Unit]] =
    (for {
      existingTsO <- dbEitherT(fetchLowerBoundDBIO())
      _ <- EitherT.fromEither[DBIO](
        existingTsO
          .filter(_ > eventsReadableStartingAt)
          .map(SaveLowerBoundError.BoundLowerThanExisting(_, eventsReadableStartingAt))
          .toLeft(())
      )
      _ <- dbEitherT[SaveLowerBoundError](
        existingTsO.fold(
          // The maybeOnboardingTopologyTimestamp is only ever inserted and never updated.
          // If we ever support onboarding the exact same sequencer multiple times, we may
          // want to also update the value, but until/unless that happens prize safety higher.
          sqlu"""insert into seq_state_manager_lower_bound (ts, ts_initial_topology)
                 values ($eventsReadableStartingAt, ${maybeOnboardingTopologyTimestamp})"""
        )(_ => sqlu"update seq_state_manager_lower_bound set ts = $eventsReadableStartingAt")
      )
    } yield ()).value.transactionally
      .withTransactionIsolation(TransactionIsolation.Serializable)

  override def status(
  )(implicit traceContext: TraceContext): Future[InternalSequencerPruningStatus] = storage.query(
    statusDBIO().transactionally.withTransactionIsolation(TransactionIsolation.Serializable),
    functionFullName,
  )

  private def statusDBIO(): ReadOnly[InternalSequencerPruningStatus] = for {
    lowerBoundO <- fetchLowerBoundDBIO()
    members <-
      sql"""
      select member, added_at, enabled, latest_acknowledgement from seq_state_manager_members"""
        .as[(Member, CantonTimestamp, Boolean, Option[CantonTimestamp])]
  } yield InternalSequencerPruningStatus(
    lowerBound = lowerBoundO.getOrElse(CantonTimestamp.Epoch),
    members = toMemberStatusSeq(members),
  )

  private def toMemberStatusSeq(
      members: Vector[(Member, CantonTimestamp, Boolean, Option[CantonTimestamp])]
  ): Seq[SequencerMemberStatus] = {
    members.map { case (member, addedAt, enabled, acknowledgedAt) =>
      SequencerMemberStatus(
        member,
        addedAt,
        lastAcknowledged = acknowledgedAt,
        enabled = enabled,
      )
    }
  }

  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[PruningResult] = for {
    numberOfEventsBefore <- numberOfEvents()
    _ <- pruneEvents(requestedTimestamp)
    min <- minCounters
    numberOfEventsAfter <- numberOfEvents()
    numberOfDeletions = numberOfEventsBefore - numberOfEventsAfter
  } yield PruningResult(numberOfDeletions, min)

  override protected[state] def numberOfEvents()(implicit
      traceContext: TraceContext
  ): Future[Long] =
    storage.query(
      sql"select count(*) from seq_state_manager_events".as[Long].head,
      functionFullName,
    )

  override def addInFlightAggregationUpdates(updates: InFlightAggregationUpdates)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storage.queryAndUpdate(
      addInFlightAggregationUpdatesDBIO(updates),
      functionFullName,
    )

  private[domain] def addInFlightAggregationUpdatesDBIO(updates: InFlightAggregationUpdates)(
      implicit traceContext: TraceContext
  ): DBIO[Unit] = {
    // First add all aggregation ids with their expiry timestamp and rules,
    // then add the information about the aggregated senders.

    val addAggregationIdsQ = storage.profile match {
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into seq_in_flight_aggregation(aggregation_id, max_sequencing_time, aggregation_rule)
           values (?, ?, ?)
           on conflict do nothing
           """
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( seq_in_flight_aggregation ( aggregation_id ) ) */
          into seq_in_flight_aggregation ifa
          using (select ? aggregation_id, ? max_sequencing_time, ? aggregation_rule from dual) input
          on (ifa.aggregation_id = input.aggregation_id)
          when not matched then
            insert (aggregation_id, max_sequencing_time, aggregation_rule)
            values (input.aggregation_id, input.max_sequencing_time, input.aggregation_rule)
          """
    }
    implicit val setParameterAggregationRule: SetParameter[AggregationRule] =
      AggregationRule.getVersionedSetParameter
    val freshAggregations = updates
      .to(immutable.Iterable)
      .flatMap { case (aggregationId, updateForId) =>
        updateForId.freshAggregation.map(aggregationId -> _).toList
      }
    val addAggregationIdsDbio =
      DbStorage.bulkOperation_(addAggregationIdsQ, freshAggregations, storage.profile) {
        pp => agg =>
          val (aggregationId, FreshInFlightAggregation(maxSequencingTimestamp, rule)) = agg
          pp.>>(aggregationId)
          pp.>>(maxSequencingTimestamp)
          pp.>>(rule)
      }

    val addSendersQ = storage.profile match {
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into seq_in_flight_aggregated_sender(aggregation_id, sender, sequencing_timestamp, signatures)
           values (?, ?, ?, ?)
           on conflict do nothing"""
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( seq_in_flight_aggregated_sender ( aggregation_id, sender ) ) */
           into seq_in_flight_aggregated_sender ifas
           using (select ? aggregation_id, ? sender, ? sequencing_timestamp, ? signatures from dual) input
           on (ifas.aggregation_id = input.aggregation_id and ifas.sender = input.sender)
           when not matched then
             insert (aggregation_id, sender, sequencing_timestamp, signatures)
             values (input.aggregation_id, input.sender, input.sequencing_timestamp, input.signatures)
       """
    }
    implicit val setParameterAggregatedSignaturesOfSender
        : SetParameter[AggregatedSignaturesOfSender] =
      AggregatedSignaturesOfSender.getVersionedSetParameter
    val aggregatedSenders =
      updates.to(immutable.Iterable).flatMap { case (aggregationId, updateForId) =>
        updateForId.aggregatedSenders.map(aggregationId -> _).iterator
      }
    val addSendersDbIO = DbStorage.bulkOperation_(addSendersQ, aggregatedSenders, storage.profile) {
      pp => item =>
        val (aggregationId, AggregatedSender(sender, aggregation)) = item
        pp.>>(aggregationId)
        pp.>>(sender)
        pp.>>(aggregation.sequencingTimestamp)
        pp.>>(
          AggregatedSignaturesOfSender(aggregation.signatures)(
            AggregatedSignaturesOfSender.protocolVersionRepresentativeFor(protocolVersion)
          )
        )
    }

    // Flatmap instead of zip because we first must insert the aggregations and only later the senders due to the foreign key constraint
    addAggregationIdsDbio.flatMap(_ => addSendersDbIO)
  }

  override def pruneExpiredInFlightAggregations(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    // It's enough to delete from `in_flight_aggregation` because deletion cascades to `in_flight_aggregated_sender`
    storage.update_(
      sqlu"delete from seq_in_flight_aggregation where max_sequencing_time <= $upToInclusive",
      functionFullName,
    )

  private def pruneEvents(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = storage
    .queryAndUpdate(
      for {
        _ <- sqlu"delete from seq_state_manager_events where ts < $requestedTimestamp"
        _ <- saveLowerBoundDBIO(requestedTimestamp)
      } yield (),
      functionFullName,
    )
    .map(_ => ())

  private def minCounters(implicit
      traceContext: TraceContext
  ): Future[Map[Member, SequencerCounter]] = storage
    .query(
      sql"""
                  select member, min(counter)
                  from seq_state_manager_events
                  group by member
           """.as[(Member, SequencerCounter)],
      functionFullName,
    )
    .map(_.toMap.filter { case (_, sequencerCounter) =>
      sequencerCounter > SequencerCounter.Genesis
    }) // filter out cases that did not get affected by pruning

  override def getInitialTopologySnapshotTimestamp(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    storage
      .querySingle(
        sql"select ts_initial_topology from seq_state_manager_lower_bound where ts_initial_topology is not null"
          .as[CantonTimestamp]
          .headOption,
        functionFullName,
      )
      .value
}

object DbSequencerStateManagerStore {
  private final case class AggregatedSignaturesOfSender(signaturesByEnvelope: Seq[Seq[Signature]])(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        AggregatedSignaturesOfSender.type
      ]
  ) extends HasProtocolVersionedWrapper[AggregatedSignaturesOfSender] {
    @transient override protected lazy val companionObj: AggregatedSignaturesOfSender.type =
      AggregatedSignaturesOfSender

    private def toProtoV30: v30.AggregatedSignaturesOfSender =
      v30.AggregatedSignaturesOfSender(
        signaturesByEnvelope = signaturesByEnvelope.map(sigs =>
          v30.AggregatedSignaturesOfSender.SignaturesForEnvelope(sigs.map(_.toProtoV30))
        )
      )
  }

  private object AggregatedSignaturesOfSender
      extends HasProtocolVersionedCompanion[AggregatedSignaturesOfSender]
      with ProtocolVersionedCompanionDbHelpers[AggregatedSignaturesOfSender] {
    override def name: String = "AggregatedSignaturesOfSender"

    override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter.storage(
        ReleaseProtocolVersion(ProtocolVersion.v30),
        v30.AggregatedSignaturesOfSender,
      )(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

    private def fromProtoV30(
        proto: v30.AggregatedSignaturesOfSender
    ): ParsingResult[AggregatedSignaturesOfSender] = {
      val v30.AggregatedSignaturesOfSender(sigsP) = proto
      for {
        sigs <- sigsP.traverse {
          case v30.AggregatedSignaturesOfSender.SignaturesForEnvelope(sigsForEnvelope) =>
            sigsForEnvelope.traverse(Signature.fromProtoV30)
        }
        rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      } yield AggregatedSignaturesOfSender(sigs)(rpv)
    }
  }

  private final case class StoredEvent(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      content: Array[Byte],
      traceContext: TraceContext,
      extraTrafficRemainder: Option[Long],
      extraTrafficConsumed: Option[Long],
  )

  private object StoredEvent {

    def create(
        event: OrdinarySerializedEvent
    ): StoredEvent =
      StoredEvent(
        event.counter,
        event.timestamp,
        event.signedEvent.toByteArray,
        event.traceContext,
        event.trafficState.map(_.extraTrafficRemainder.value),
        event.trafficState.map(_.extraTrafficConsumed.value),
      )
  }

}
