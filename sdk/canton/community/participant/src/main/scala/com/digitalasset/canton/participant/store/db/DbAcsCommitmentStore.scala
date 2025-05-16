// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.syntax.traverse.*
import com.daml.nameof.NameOf
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.setParameterLengthLimitedString
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.{BufferedAcsCommitment, CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.store.AcsCommitmentStore.ParticipantCommitmentData
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
  AcsCounterParticipantConfigStore,
  CommitmentQueue,
  IncrementalCommitmentStore,
}
import com.digitalasset.canton.protocol.StoredParties
import com.digitalasset.canton.protocol.messages.AcsCommitment.HashedCommitmentType
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  CommitmentPeriodState,
  SignedProtocolMessage,
}
import com.digitalasset.canton.resource.DbStorage.Implicits.BuilderChain.{
  mergeBuildersIntoChain,
  toSQLActionBuilderChain,
}
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.{DbDeserializationException, DbPrunableByTimeSynchronizer}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.collection.IterableUtil.Ops
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import slick.jdbc.TransactionIsolation.Serializable
import slick.jdbc.{
  GetResult,
  GetTupleResult,
  PositionedParameters,
  SetParameter,
  TransactionIsolation,
}

import scala.collection.immutable.SortedSet
import scala.concurrent.ExecutionContext

class DbAcsCommitmentStore(
    override protected val storage: DbStorage,
    override val indexedSynchronizer: IndexedSynchronizer,
    override val acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends AcsCommitmentStore
    with DbPrunableByTimeSynchronizer
    with DbStore {
  import storage.api.*
  import storage.converters.*

  override protected[this] val pruning_status_table = "par_commitment_pruning"

  implicit val getSignedCommitment: GetResult[SignedProtocolMessage[AcsCommitment]] = GetResult(r =>
    SignedProtocolMessage
      .fromTrustedByteString(protocolVersion)(ByteString.copyFrom(r.<<[Array[Byte]]))
      .fold(
        err =>
          throw new DbDeserializationException(
            s"Failed to deserialize signed ACS commitment: $err"
          ),
        m =>
          SignedProtocolMessage
            .signedMessageCast[AcsCommitment]
            .toKind(m)
            .getOrElse(
              throw new DbDeserializationException(
                s"Expected a signed ACS commitment, but got a $m"
              )
            ),
      )
  )

  override def getComputed(period: CommitmentPeriod, counterParticipant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[(CommitmentPeriod, AcsCommitment.HashedCommitmentType)]] = {
    val query = sql"""
        select from_exclusive, to_inclusive, commitment from par_computed_acs_commitments
          where synchronizer_idx = $indexedSynchronizer
            and counter_participant = $counterParticipant
            and from_exclusive < ${period.toInclusive}
            and to_inclusive > ${period.fromExclusive}
          order by from_exclusive asc"""
      .as[(CommitmentPeriod, AcsCommitment.HashedCommitmentType)]

    storage.query(query, operationName = "commitments: get computed")
  }

  override def storeComputed(
      items: NonEmpty[Seq[AcsCommitmentStore.ParticipantCommitmentData]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    // We want to detect if we try to overwrite an existing commitment with a different value, as this signals an error.
    // Still, for performance reasons, we want to do everything in a single, non-interactive statement.
    // We reconcile the two by an upsert that inserts only on a "conflict" where the values are equal, and
    // requiring that at least one value is written.

    def setData(pp: PositionedParameters)(item: ParticipantCommitmentData): Unit = {
      val ParticipantCommitmentData(counterParticipant, period, commitment) = item
      pp >> indexedSynchronizer
      pp >> counterParticipant
      pp >> period.fromExclusive
      pp >> period.toInclusive
      pp >> commitment
    }

    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        """merge into par_computed_acs_commitments cs
            using (
              select cast(? as int) synchronizer_idx, cast(? as varchar) counter_participant, cast(? as bigint) from_exclusive,
              cast(? as bigint) to_inclusive, cast(? as binary large object) commitment from dual)
            excluded on (cs.synchronizer_idx = excluded.synchronizer_idx and cs.counter_participant = excluded.counter_participant and
            cs.from_exclusive = excluded.from_exclusive and cs.to_inclusive = excluded.to_inclusive)
            when matched and cs.commitment = excluded.commitment
              then update set cs.commitment = excluded.commitment
            when not matched then
               insert (synchronizer_idx, counter_participant, from_exclusive, to_inclusive, commitment)
               values (excluded.synchronizer_idx, excluded.counter_participant, excluded.from_exclusive, excluded.to_inclusive, excluded.commitment)
            """
      case _: DbStorage.Profile.Postgres =>
        """insert into par_computed_acs_commitments(synchronizer_idx, counter_participant, from_exclusive, to_inclusive, commitment)
               values (?, ?, ?, ?, ?)
                  on conflict (synchronizer_idx, counter_participant, from_exclusive, to_inclusive) do
                    update set commitment = excluded.commitment
                    where par_computed_acs_commitments.commitment = excluded.commitment
          """
    }

    val bulkUpsert = DbStorage.bulkOperation(query, items.toList, storage.profile)(setData)

    storage.queryAndUpdate(bulkUpsert, "commitments: insert computed").map { rowCounts =>
      rowCounts.zip(items.toList).foreach { case (rowCount, item) =>
        val ParticipantCommitmentData(counterParticipant, period, commitment) = item
        // Underreporting of the affected rows should not matter here as the query is idempotent and updates the row even if the same values had been there before
        ErrorUtil.requireState(
          rowCount != 0,
          s"Commitment for synchronizer $indexedSynchronizer, counterparticipant $counterParticipant and period $period already computed with a different value; refusing to insert $commitment",
        )
      }
    }
  }

  override def markOutstanding(
      periods: NonEmpty[Set[CommitmentPeriod]],
      counterParticipants: NonEmpty[Set[ParticipantId]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    logger.debug(
      s"Marking $periods as outstanding for ${counterParticipants.size} remote participants"
    )
    def setParams(
        pp: PositionedParameters
    ): ((CommitmentPeriod, ParticipantId)) => Unit = { case (period, participant) =>
      pp >> indexedSynchronizer
      pp >> period.fromExclusive
      pp >> period.toInclusive
      pp >> participant
      pp >> CommitmentPeriodState.Outstanding
    }

    val crossProduct = periods.forgetNE.crossProductBy(counterParticipants)

    val insertOutstanding =
      """insert into par_outstanding_acs_commitments (synchronizer_idx, from_exclusive, to_inclusive, counter_participant, matching_state) values
        ( ?, ?, ?, ?, ?)
        on conflict do nothing"""

    storage.queryAndUpdate(
      DbStorage.bulkOperation_(insertOutstanding, crossProduct.toSeq, storage.profile)(setParams),
      operationName = "commitments: storeOutstanding",
    )
  }

  override def markComputedAndSent(
      period: CommitmentPeriod
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val timestamp = period.toInclusive
    val upsertQuery = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_last_computed_acs_commitments(synchronizer_idx, ts) values ($indexedSynchronizer, $timestamp)"""
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_last_computed_acs_commitments(synchronizer_idx, ts) values ($indexedSynchronizer, $timestamp)
                 on conflict (synchronizer_idx) do update set ts = $timestamp"""
    }

    storage.update_(upsertQuery, operationName = "commitments: markComputedAndSent")
  }

  override def outstanding(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipants: Seq[ParticipantId],
      includeMatchedPeriods: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)]] = {
    val participantFilter: SQLActionBuilderChain = counterParticipants match {
      case Seq() => sql""
      case list =>
        sql" AND counter_participant IN (" ++ list
          .map(part => sql"$part")
          .intercalate(sql", ") ++ sql")"
    }

    import DbStorage.Implicits.BuilderChain.*
    val query =
      sql"""select from_exclusive, to_inclusive, counter_participant, matching_state
                    from par_outstanding_acs_commitments where synchronizer_idx = $indexedSynchronizer and to_inclusive >= $start and from_exclusive < $end
                    and ($includeMatchedPeriods or matching_state != ${CommitmentPeriodState.Matched})
                """ ++ participantFilter

    storage.query(
      query
        .as[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)]
        .transactionally
        .withTransactionIsolation(TransactionIsolation.ReadCommitted),
      operationName = functionFullName,
    )
  }

  override def storeReceived(
      commitment: SignedProtocolMessage[AcsCommitment]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val sender = commitment.message.sender
    val from = commitment.message.period.fromExclusive
    val to = commitment.message.period.toInclusive
    val serialized = commitment.toByteArray

    val upsertQuery = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_received_acs_commitments
            using dual
            on synchronizer_idx = $indexedSynchronizer and sender = $sender and from_exclusive = $from and to_inclusive = $to and signed_commitment = $serialized
            when not matched then
               insert (synchronizer_idx, sender, from_exclusive, to_inclusive, signed_commitment)
               values ($indexedSynchronizer, $sender, $from, $to, $serialized)
            """
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_received_acs_commitments(synchronizer_idx, sender, from_exclusive, to_inclusive, signed_commitment)
               select $indexedSynchronizer, $sender, $from, $to, $serialized
               where not exists(
                 select * from par_received_acs_commitments
                 where synchronizer_idx = $indexedSynchronizer and sender = $sender and from_exclusive = $from and to_inclusive = $to and signed_commitment = $serialized)
          """
    }
    storage.update_(
      upsertQuery,
      operationName = "commitments: inserting ACS commitment",
    )
  }

  override def markPeriod(
      counterParticipant: ParticipantId,
      periods: NonEmpty[Set[CommitmentPeriod]],
      matchingState: CommitmentPeriodState,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {

    val upsertQuery = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        s"""merge into par_outstanding_acs_commitments AS target
            using (values(?,?,?,?,?)) as source (synchronizer_idx, from_exclusive, to_inclusive, counter_participant, matching_state)
              on (target.synchronizer_idx = source.synchronizer_idx
              and target.from_exclusive = source.from_exclusive
              and target.to_inclusive = source.to_inclusive
              and target.counter_participant = source.counter_participant)
            when matched then
              update set matching_state = case
                  when target.matching_state = ? then source.matching_state
                  when target.matching_state = ? and source.matching_state = ? then source.matching_state
                  else target.matching_state
                end
            when not matched then
                insert (synchronizer_idx, from_exclusive, to_inclusive, counter_participant, matching_state)
                values (source.synchronizer_idx, source.from_exclusive, source.to_inclusive, source.counter_participant, source.matching_state);
            """
      case _: DbStorage.Profile.Postgres =>
        s"""insert into par_outstanding_acs_commitments(synchronizer_idx, from_exclusive, to_inclusive, counter_participant, matching_state)
               values (?, ?, ?, ?, ?)
               on conflict (synchronizer_idx, from_exclusive, to_inclusive, counter_participant)
               do update set
                  matching_state = case
                      when par_outstanding_acs_commitments.matching_state = ? then excluded.matching_state
                      when par_outstanding_acs_commitments.matching_state = ? and excluded.matching_state = ? then excluded.matching_state
                      else par_outstanding_acs_commitments.matching_state
                  end;
          """
    }

    storage.queryAndUpdate(
      DbStorage.bulkOperation_(upsertQuery, periods, storage.profile) { pp => period =>
        pp >> indexedSynchronizer
        pp >> period.fromExclusive
        pp >> period.toInclusive
        pp >> counterParticipant
        pp >> matchingState
        // when par_outstanding_acs_commitments.matching_state = ? then excluded.matching_state
        pp >> CommitmentPeriodState.Outstanding
        //  when par_outstanding_acs_commitments.matching_state = ? and excluded.matching_state = ? then excluded.matching_state
        pp >> CommitmentPeriodState.Mismatched
        pp >> CommitmentPeriodState.Matched
      },
      operationName =
        s"commitments: marking until ${periods.last1.toInclusive} with state $matchingState for $counterParticipant",
    )
  }

  override def doPrune(
      before: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    val query1 =
      sqlu"delete from par_received_acs_commitments where synchronizer_idx=$indexedSynchronizer and to_inclusive < $before"
    val query2 =
      sqlu"delete from par_computed_acs_commitments where synchronizer_idx=$indexedSynchronizer and to_inclusive < $before"
    // we completely prune the outstanding table, the matching is "best effort" and the assumption is that we achieved unison with all counter-participants somewhere after pruning time.
    // we might lose old mismatches and outstanding, but we are not able to validate anything received before pruning time anyway (we cant recomputed, and we don't have it in the computed table).
    val query3 =
      sqlu"delete from par_outstanding_acs_commitments where synchronizer_idx=$indexedSynchronizer and to_inclusive < $before"
    storage
      .queryAndUpdate(
        query1.zip(query2.zip(query3)),
        operationName = "commitments: prune",
      )
      .map { case (q1, (q2, q3)) => q1 + q2 + q3 }
  }

  override def lastComputedAndSent(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestampSecond]] =
    storage.query(
      sql"select ts from par_last_computed_acs_commitments where synchronizer_idx = $indexedSynchronizer"
        .as[CantonTimestampSecond]
        .headOption,
      functionFullName,
    )

  override def noOutstandingCommitments(
      beforeOrAt: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    for {
      computed <- lastComputedAndSent
      adjustedTsOpt = computed.map(_.forgetRefinement.min(beforeOrAt))
      ignores <- acsCounterParticipantConfigStore
        .getAllActiveNoWaitCounterParticipants(Seq(indexedSynchronizer.synchronizerId), Seq.empty)
      outstandingOpt <- adjustedTsOpt.traverse { ts =>
        storage.query(
          sql"""select from_exclusive, to_inclusive, counter_participant, multi_hosted_cleared from par_outstanding_acs_commitments
               where synchronizer_idx=$indexedSynchronizer
               and from_exclusive < $ts
               and matching_state != ${CommitmentPeriodState.Matched}
               and multi_hosted_cleared = false"""
            .as[(CantonTimestamp, CantonTimestamp, ParticipantId, Boolean)]
            .withTransactionIsolation(Serializable),
          operationName = "commitments: compute no outstanding",
        )
      }
    } yield {
      for {
        ts <- adjustedTsOpt
        outstanding <- outstandingOpt.map { vector =>
          vector
            .filter { case (_, _, participantId, _) =>
              !ignores.exists(config => config.participantId == participantId)
            }
            .map { case (start, end, _, _) =>
              (start, end)
            }
        }
      } yield AcsCommitmentStore.latestCleanPeriod(ts, outstanding)
    }

  override def searchComputedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.HashedCommitmentType)]
  ] = {

    val participantFilter: SQLActionBuilderChain = counterParticipants match {
      case Seq() => sql""
      case list =>
        sql" AND counter_participant IN (" ++ list
          .map(part => sql"$part")
          .intercalate(sql", ") ++ sql")"
    }

    import DbStorage.Implicits.BuilderChain.*
    val query =
      sql"""select from_exclusive, to_inclusive, counter_participant, commitment
            from par_computed_acs_commitments
            where synchronizer_idx = $indexedSynchronizer and to_inclusive >= $start and from_exclusive < $end""" ++ participantFilter

    storage.query(
      query.as[(CommitmentPeriod, ParticipantId, AcsCommitment.HashedCommitmentType)],
      functionFullName,
    )
  }

  override def searchReceivedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipants: Seq[ParticipantId] = Seq.empty,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[SignedProtocolMessage[AcsCommitment]]] = {

    val participantFilter: SQLActionBuilderChain = counterParticipants match {
      case Seq() => sql""
      case list =>
        sql" AND sender IN (" ++ list
          .map(part => sql"$part")
          .intercalate(sql", ") ++ sql")"
    }

    import DbStorage.Implicits.BuilderChain.*
    val query =
      sql"""select signed_commitment
            from par_received_acs_commitments
            where synchronizer_idx = $indexedSynchronizer and to_inclusive >= $start and from_exclusive < $end""" ++ participantFilter

    storage.query(query.as[SignedProtocolMessage[AcsCommitment]], functionFullName)

  }

  override val runningCommitments: DbIncrementalCommitmentStore =
    new DbIncrementalCommitmentStore(
      storage,
      indexedSynchronizer,
      protocolVersion,
      timeouts,
      loggerFactory,
    )

  override val queue: DbCommitmentQueue =
    new DbCommitmentQueue(storage, indexedSynchronizer, timeouts, loggerFactory)

  override def onClosed(): Unit =
    LifeCycle.close(
      runningCommitments,
      queue,
    )(logger)

  override def markMultiHostedCleared(period: CommitmentPeriod)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"""update par_outstanding_acs_commitments
                set multi_hosted_cleared = true
                where synchronizer_idx = $indexedSynchronizer
                and from_exclusive = ${period.fromExclusive}
                and to_inclusive = ${period.toInclusive}""",
      "markMultiHostedCleared",
    )

}

class DbIncrementalCommitmentStore(
    override protected val storage: DbStorage,
    indexedSynchronizer: IndexedSynchronizer,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends IncrementalCommitmentStore
    with DbStore {

  import DbStorage.Implicits.*
  import storage.api.*
  import storage.converters.*

  private implicit val setParameterStoredParties: SetParameter[StoredParties] =
    StoredParties.getVersionedSetParameter(protocolVersion)

  override def get()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(RecordTime, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType])] =
    for {
      res <- storage.query(
        (for {
          tsWithTieBreaker <-
            sql"""select ts, tie_breaker from par_commitment_snapshot_time where synchronizer_idx = $indexedSynchronizer"""
              .as[(CantonTimestamp, Long)]
              .headOption
          snapshot <-
            sql"""select stakeholders, commitment from par_commitment_snapshot where synchronizer_idx = $indexedSynchronizer"""
              .as[(StoredParties, AcsCommitment.CommitmentType)]
        } yield (tsWithTieBreaker, snapshot)).transactionally
          .withTransactionIsolation(Serializable),
        operationName = "commitments: read commitments snapshot",
      )
    } yield {
      val (optTsWithTieBreaker, snapshot) = res
      optTsWithTieBreaker.fold(
        RecordTime.MinValue -> Map.empty[SortedSet[LfPartyId], AcsCommitment.CommitmentType]
      ) { case (ts, tieBreaker) =>
        RecordTime(ts, tieBreaker) -> snapshot.map { case (storedParties, commitment) =>
          storedParties.parties -> commitment
        }.toMap
      }
    }

  override def watermark(implicit traceContext: TraceContext): FutureUnlessShutdown[RecordTime] = {
    val query =
      sql"""select ts, tie_breaker from par_commitment_snapshot_time where synchronizer_idx=$indexedSynchronizer"""
        .as[(CantonTimestamp, Long)]
        .headOption
    storage
      .query(query, operationName = "commitments: read snapshot watermark")
      .map(_.fold(RecordTime.MinValue) { case (ts, tieBreaker) => RecordTime(ts, tieBreaker) })
  }

  @SuppressWarnings(Array("org.wartremover.warts.AnyVal"))
  def update(
      rt: RecordTime,
      updates: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      deletes: Set[SortedSet[LfPartyId]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def partySetHash(parties: SortedSet[LfPartyId]): String68 =
      Hash
        .digest(
          HashPurpose.Stakeholders,
          DeterministicEncoding.encodeSeqWith(parties.toList)(DeterministicEncoding.encodeParty),
          HashAlgorithm.Sha256,
        )
        .toLengthLimitedHexString
    def deleteCommitments(stakeholders: List[SortedSet[LfPartyId]]): DbAction.All[Unit] = {
      val deleteStatement =
        "delete from par_commitment_snapshot where synchronizer_idx = ? and stakeholders_hash = ?"
      DbStorage.bulkOperation_(deleteStatement, stakeholders, storage.profile) { pp => stkhs =>
        pp >> indexedSynchronizer
        pp >> partySetHash(stkhs)
      }
    }

    def storeUpdates(
        updates: List[(SortedSet[LfPartyId], AcsCommitment.CommitmentType)]
    ): DbAction.All[Unit] = {
      def setParams(
          pp: PositionedParameters
      ): ((SortedSet[LfPartyId], AcsCommitment.CommitmentType)) => Unit = {
        case (stkhs, commitment) =>
          pp >> indexedSynchronizer
          pp >> partySetHash(stkhs)
          pp >> StoredParties(stkhs)
          pp >> commitment
      }

      val statement = storage.profile match {
        case _: DbStorage.Profile.H2 =>
          """merge into par_commitment_snapshot (synchronizer_idx, stakeholders_hash, stakeholders, commitment)
                   values (?, ?, ?, ?)"""

        case _: DbStorage.Profile.Postgres =>
          """insert into par_commitment_snapshot (synchronizer_idx, stakeholders_hash, stakeholders, commitment)
                 values (?, ?, ?, ?) on conflict (synchronizer_idx, stakeholders_hash) do update set commitment = excluded.commitment"""
      }
      DbStorage.bulkOperation_(
        statement,
        updates,
        storage.profile,
      )(setParams)
    }

    def insertRt(rt: RecordTime): DbAction.WriteOnly[Int] =
      storage.profile match {
        case _: DbStorage.Profile.H2 =>
          sqlu"""merge into par_commitment_snapshot_time (synchronizer_idx, ts, tie_breaker) values ($indexedSynchronizer, ${rt.timestamp}, ${rt.tieBreaker})"""
        case _: DbStorage.Profile.Postgres =>
          sqlu"""insert into par_commitment_snapshot_time(synchronizer_idx, ts, tie_breaker) values ($indexedSynchronizer, ${rt.timestamp}, ${rt.tieBreaker})
                 on conflict (synchronizer_idx) do update set ts = ${rt.timestamp}, tie_breaker = ${rt.tieBreaker}"""
      }

    val updateList = updates.toList
    val deleteList = deletes.toList

    val operations = List(insertRt(rt), storeUpdates(updateList), deleteCommitments(deleteList))

    storage.queryAndUpdate(
      DBIO.seq(operations*).transactionally.withTransactionIsolation(Serializable),
      operationName = "commitments: incremental commitments snapshot update",
    )
  }
}

class DbCommitmentQueue(
    override protected val storage: DbStorage,
    indexedSynchronizer: IndexedSynchronizer,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends CommitmentQueue
    with DbStore {

  import storage.api.*

  private implicit val acsCommitmentReader: GetResult[BufferedAcsCommitment] =
    new GetTupleResult[(ParticipantId, ParticipantId, CommitmentPeriod, HashedCommitmentType)](
      GetResult[ParticipantId],
      GetResult[ParticipantId],
      GetResult[CommitmentPeriod],
      GetResult[Hash],
    ).andThen { case (sender, counterParticipant, period, commitment) =>
      BufferedAcsCommitment(
        indexedSynchronizer.synchronizerId,
        sender,
        counterParticipant,
        period,
        commitment,
      )
    }

  override def enqueue(
      commitment: AcsCommitment
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val insertAction =
      sqlu"""insert
             into par_commitment_queue(synchronizer_idx, sender, counter_participant, from_exclusive, to_inclusive, commitment)
             values($indexedSynchronizer, ${commitment.sender}, ${commitment.counterParticipant}, ${commitment.period.fromExclusive}, ${commitment.period.toInclusive}, ${commitment.commitment})
             on conflict do nothing"""

    storage.update_(insertAction, operationName = "enqueue commitment")
  }

  /** Returns all commitments whose period ends at or before the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThrough(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[BufferedAcsCommitment]] =
    storage
      .query(
        sql"""select sender, counter_participant, from_exclusive, to_inclusive, commitment
             from par_commitment_queue
             where synchronizer_idx = $indexedSynchronizer and to_inclusive <= $timestamp"""
          .as[BufferedAcsCommitment],
        operationName = NameOf.qualifiedNameOfCurrentFunc,
      )
      .map(_.toList)

  /** Returns all commitments whose period ends at or after the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThroughAtOrAfter(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[BufferedAcsCommitment]] =
    storage
      .query(
        sql"""select sender, counter_participant, from_exclusive, to_inclusive, commitment
                                            from par_commitment_queue
                                            where synchronizer_idx = $indexedSynchronizer and to_inclusive >= $timestamp"""
          .as[BufferedAcsCommitment],
        operationName = NameOf.qualifiedNameOfCurrentFunc,
      )

  def peekOverlapsForCounterParticipant(
      period: CommitmentPeriod,
      counterParticipant: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[BufferedAcsCommitment]] =
    storage
      .query(
        sql"""select sender, counter_participant, from_exclusive, to_inclusive, commitment
                 from par_commitment_queue
                 where synchronizer_idx = $indexedSynchronizer and sender = $counterParticipant
                 and to_inclusive > ${period.fromExclusive}
                 and from_exclusive < ${period.toInclusive} """
          .as[BufferedAcsCommitment],
        operationName = NameOf.qualifiedNameOfCurrentFunc,
      )

  /** Deletes all commitments whose period ends at or before the given timestamp. */
  override def deleteThrough(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"delete from par_commitment_queue where synchronizer_idx = $indexedSynchronizer and to_inclusive <= $timestamp",
      operationName = "delete queued commitments",
    )
}
