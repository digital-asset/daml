// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.syntax.traverse.*
import com.daml.nameof.NameOf
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.{
  SortedReconciliationIntervals,
  SortedReconciliationIntervalsProvider,
}
import com.digitalasset.canton.participant.store.AcsCommitmentStore.CommitmentData
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
  AcsCounterParticipantConfigStore,
  CommitmentQueue,
  IncrementalCommitmentStore,
}
import com.digitalasset.canton.protocol.StoredParties
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
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.db.{DbDeserializationException, DbPrunableByTimeDomain}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import slick.jdbc.TransactionIsolation.Serializable
import slick.jdbc.{GetResult, PositionedParameters, SetParameter, TransactionIsolation}

import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}

class DbAcsCommitmentStore(
    override protected val storage: DbStorage,
    override val indexedDomain: IndexedDomain,
    override val acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends AcsCommitmentStore
    with DbPrunableByTimeDomain
    with DbStore {
  import DbStorage.Implicits.*
  import storage.api.*
  import storage.converters.*

  override protected[this] val pruning_status_table = "par_commitment_pruning"

  private val markSafeQueue = new SimpleExecutionQueue(
    "db-acs-commitment-store-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  implicit val getSignedCommitment: GetResult[SignedProtocolMessage[AcsCommitment]] = GetResult(r =>
    SignedProtocolMessage
      .fromByteString(protocolVersion)(
        ByteString.copyFrom(r.<<[Array[Byte]])
      )
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
  ): FutureUnlessShutdown[Iterable[(CommitmentPeriod, AcsCommitment.CommitmentType)]] = {
    val query = sql"""
        select from_exclusive, to_inclusive, commitment from par_computed_acs_commitments
          where domain_idx = $indexedDomain
            and counter_participant = $counterParticipant
            and from_exclusive < ${period.toInclusive}
            and to_inclusive > ${period.fromExclusive}
          order by from_exclusive asc"""
      .as[(CommitmentPeriod, AcsCommitment.CommitmentType)]

    storage.queryUnlessShutdown(query, operationName = "commitments: get computed")
  }

  override def storeComputed(
      items: NonEmpty[Seq[AcsCommitmentStore.CommitmentData]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    // We want to detect if we try to overwrite an existing commitment with a different value, as this signals an error.
    // Still, for performance reasons, we want to do everything in a single, non-interactive statement.
    // We reconcile the two by an upsert that inserts only on a "conflict" where the values are equal, and
    // requiring that at least one value is written.

    def setData(pp: PositionedParameters)(item: CommitmentData): Unit = {
      val CommitmentData(counterParticipant, period, commitment) = item
      pp >> indexedDomain
      pp >> counterParticipant
      pp >> period.fromExclusive
      pp >> period.toInclusive
      pp >> commitment
    }

    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        """merge into par_computed_acs_commitments cs
            using (
              select cast(? as int) domain_idx, cast(? as varchar(300)) counter_participant, cast(? as bigint) from_exclusive,
              cast(? as bigint) to_inclusive, cast(? as binary large object) commitment from dual)
            excluded on (cs.domain_idx = excluded.domain_idx and cs.counter_participant = excluded.counter_participant and
            cs.from_exclusive = excluded.from_exclusive and cs.to_inclusive = excluded.to_inclusive)
            when matched and cs.commitment = excluded.commitment
              then update set cs.commitment = excluded.commitment
            when not matched then
               insert (domain_idx, counter_participant, from_exclusive, to_inclusive, commitment)
               values (excluded.domain_idx, excluded.counter_participant, excluded.from_exclusive, excluded.to_inclusive, excluded.commitment)
            """
      case _: DbStorage.Profile.Postgres =>
        """insert into par_computed_acs_commitments(domain_idx, counter_participant, from_exclusive, to_inclusive, commitment)
               values (?, ?, ?, ?, ?)
                  on conflict (domain_idx, counter_participant, from_exclusive, to_inclusive) do
                    update set commitment = excluded.commitment
                    where par_computed_acs_commitments.commitment = excluded.commitment
          """
    }

    val bulkUpsert = DbStorage.bulkOperation(query, items.toList, storage.profile)(setData)

    storage.queryAndUpdateUnlessShutdown(bulkUpsert, "commitments: insert computed").map {
      rowCounts =>
        rowCounts.zip(items.toList).foreach { case (rowCount, item) =>
          val CommitmentData(counterParticipant, period, commitment) = item
          // Underreporting of the affected rows should not matter here as the query is idempotent and updates the row even if the same values had been there before
          ErrorUtil.requireState(
            rowCount != 0,
            s"Commitment for domain $indexedDomain, counterparticipant $counterParticipant and period $period already computed with a different value; refusing to insert $commitment",
          )
        }
    }
  }

  override def markOutstanding(period: CommitmentPeriod, counterParticipants: Set[ParticipantId])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    logger.debug(
      s"Marking $period as outstanding for ${counterParticipants.size} remote participants"
    )
    if (counterParticipants.isEmpty) FutureUnlessShutdown.unit
    else {
      import DbStorage.Implicits.BuilderChain.*

      // Slick doesn't support bulk insertions by default, so we have to stitch our own
      val insertOutstanding =
        (sql"""insert into par_outstanding_acs_commitments (domain_idx, from_exclusive, to_inclusive, counter_participant, matching_state) values """ ++
          counterParticipants.toList
            .map(p =>
              sql"($indexedDomain, ${period.fromExclusive}, ${period.toInclusive}, $p,${CommitmentPeriodState.Outstanding})"
            )
            .intercalate(sql", ") ++ sql" on conflict do nothing").asUpdate

      storage.updateUnlessShutdown_(
        insertOutstanding,
        operationName = "commitments: storeOutstanding",
      )
    }
  }

  override def markComputedAndSent(
      period: CommitmentPeriod
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val timestamp = period.toInclusive
    val upsertQuery = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_last_computed_acs_commitments(domain_idx, ts) values ($indexedDomain, $timestamp)"""
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_last_computed_acs_commitments(domain_idx, ts) values ($indexedDomain, $timestamp)
                 on conflict (domain_idx) do update set ts = $timestamp"""
    }

    storage.updateUnlessShutdown_(upsertQuery, operationName = "commitments: markComputedAndSent")
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
                    from par_outstanding_acs_commitments where domain_idx = $indexedDomain and to_inclusive >= $start and from_exclusive < $end
                    and ($includeMatchedPeriods or matching_state != ${CommitmentPeriodState.Matched})
                """ ++ participantFilter

    storage.queryUnlessShutdown(
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
            on domain_idx = $indexedDomain and sender = $sender and from_exclusive = $from and to_inclusive = $to and signed_commitment = $serialized
            when not matched then
               insert (domain_idx, sender, from_exclusive, to_inclusive, signed_commitment)
               values ($indexedDomain, $sender, $from, $to, $serialized)
            """
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_received_acs_commitments(domain_idx, sender, from_exclusive, to_inclusive, signed_commitment)
               select $indexedDomain, $sender, $from, $to, $serialized
               where not exists(
                 select * from par_received_acs_commitments
                 where domain_idx = $indexedDomain and sender = $sender and from_exclusive = $from and to_inclusive = $to and signed_commitment = $serialized)
          """
    }
    storage.updateUnlessShutdown_(
      upsertQuery,
      operationName = "commitments: inserting ACS commitment",
    )
  }

  override def markPeriod(
      counterParticipant: ParticipantId,
      period: CommitmentPeriod,
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
      matchingState: CommitmentPeriodState,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    def dbQueries(
        sortedReconciliationIntervals: SortedReconciliationIntervals
    ): DBIOAction[Unit, NoStream, Effect.All] = {
      def containsTick(commitmentPeriod: CommitmentPeriod): Boolean =
        sortedReconciliationIntervals
          .containsTick(
            commitmentPeriod.fromExclusive.forgetRefinement,
            commitmentPeriod.toInclusive.forgetRefinement,
          )
          .getOrElse {
            logger.warn(s"Unable to determine whether $commitmentPeriod contains a tick.")
            true
          }

      val insertQuery =
        """insert into par_outstanding_acs_commitments (domain_idx, from_exclusive, to_inclusive, counter_participant, matching_state)
           values (?, ?, ?, ?, ?) on conflict do nothing"""

      val stateUpdateFilter: SQLActionBuilderChain =
        if (matchingState == CommitmentPeriodState.Matched)
          sql"""(matching_state = ${CommitmentPeriodState.Outstanding} or matching_state = ${CommitmentPeriodState.Mismatched}
               or matching_state = ${CommitmentPeriodState.Matched})"""
        else if (matchingState == CommitmentPeriodState.Mismatched)
          sql"""(matching_state = ${CommitmentPeriodState.Outstanding} or matching_state = ${CommitmentPeriodState.Mismatched})"""
        else
          sql"""matching_state = ${CommitmentPeriodState.Outstanding}"""
      /*
      This is a three steps process:
      - First, we select the outstanding intervals that are overlapping with the new one.
      - Then, we delete the overlapping intervals.
      - Finally, we insert what we need (non-empty intersections)
       */
      for {
        overlappingIntervals <-
          (sql"""select from_exclusive, to_inclusive, matching_state from par_outstanding_acs_commitments
          where domain_idx = $indexedDomain and counter_participant = $counterParticipant
          and from_exclusive < ${period.toInclusive} and to_inclusive > ${period.fromExclusive}
          and """ ++ stateUpdateFilter).toActionBuilder
            .as[(CantonTimestampSecond, CantonTimestampSecond, CommitmentPeriodState)]

        _ <-
          (sql"""delete from par_outstanding_acs_commitments
          where domain_idx = $indexedDomain and counter_participant = $counterParticipant
          and from_exclusive < ${period.toInclusive} and to_inclusive > ${period.fromExclusive}
          and """ ++ stateUpdateFilter).toActionBuilder.asUpdate

        newPeriods = overlappingIntervals
          .flatMap { case (from, to, oldState) =>
            val leftOverlap = CommitmentPeriod.create(from, period.fromExclusive)
            val rightOverlap = CommitmentPeriod.create(period.toInclusive, to)
            val inbetween = CommitmentPeriod.create(period.fromExclusive, period.toInclusive)
            leftOverlap.toSeq.map((_, oldState)) ++ rightOverlap.toSeq.map(
              (_, oldState)
            ) ++ inbetween.toSeq.map((_, matchingState))
          }
          .toList
          .distinct
          .filter(t => containsTick(t._1))

        _ <- DbStorage.bulkOperation_(insertQuery, newPeriods, storage.profile) { pp => newPeriod =>
          val (period, state) = newPeriod
          pp >> indexedDomain
          pp >> period.fromExclusive
          pp >> period.toInclusive
          pp >> counterParticipant
          pp >> state
        }

      } yield ()
    }

    markSafeQueue
      .executeUS(
        for {
          /*
          That could be wrong if a period is marked as outstanding between the point where we
          fetch the approximate timestamp of the topology client and the query for the sorted
          reconciliation intervals.
          Such a period would be kept as outstanding even if it contains no tick. On the other
          hand, only commitment periods around restarts could be "empty" (not contain any tick).
           */
          approxInterval <- sortedReconciliationIntervalsProvider.approximateReconciliationIntervals

          sortedReconciliationIntervals <-
            // the domain parameters at the approximate topology timestamp is recent enough for the period
            if (approxInterval.validUntil >= period.toInclusive.forgetRefinement)
              FutureUnlessShutdown.pure(approxInterval)
            else {
              // it is safe to wait for the topology timestamp period.toInclusive.forgetRefinement because we validate
              // that it is before the sequencing timestamp when we process incoming commitments
              sortedReconciliationIntervalsProvider.reconciliationIntervals(
                period.toInclusive.forgetRefinement
              )
            }

          _ <- storage.queryAndUpdateUnlessShutdown(
            dbQueries(sortedReconciliationIntervals).transactionally.withTransactionIsolation(
              TransactionIsolation.Serializable
            ),
            operationName =
              s"commitments: mark period safe (${period.fromExclusive}, ${period.toInclusive}]",
          )

        } yield (),
        "Run mark period safe DB query",
      )
  }

  override def doPrune(
      before: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Int] = {
    val query1 =
      sqlu"delete from par_received_acs_commitments where domain_idx=$indexedDomain and to_inclusive < $before"
    val query2 =
      sqlu"delete from par_computed_acs_commitments where domain_idx=$indexedDomain and to_inclusive < $before"
    val query3 =
      sqlu"delete from par_outstanding_acs_commitments where domain_idx=$indexedDomain and matching_state = ${CommitmentPeriodState.Matched} and to_inclusive < $before"
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
    storage.queryUnlessShutdown(
      sql"select ts from par_last_computed_acs_commitments where domain_idx = $indexedDomain"
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
        .getAllActiveNoWaitCounterParticipants(Seq(indexedDomain.domainId), Seq.empty)
      outstandingOpt <- adjustedTsOpt.traverse { ts =>
        storage.queryUnlessShutdown(
          sql"select from_exclusive, to_inclusive, counter_participant from par_outstanding_acs_commitments where domain_idx=$indexedDomain and from_exclusive < $ts and matching_state != ${CommitmentPeriodState.Matched}"
            .as[(CantonTimestamp, CantonTimestamp, ParticipantId)]
            .withTransactionIsolation(Serializable),
          operationName = "commitments: compute no outstanding",
        )
      }
    } yield {
      for {
        ts <- adjustedTsOpt
        outstanding <- outstandingOpt.map { vector =>
          vector
            .filter { case (_, _, participantId) =>
              !ignores.exists(config => config.participantId == participantId)
            }
            .map { case (start, end, _) =>
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
    Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)]
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
            where domain_idx = $indexedDomain and to_inclusive >= $start and from_exclusive < $end""" ++ participantFilter

    storage.queryUnlessShutdown(
      query.as[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)],
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
            where domain_idx = $indexedDomain and to_inclusive >= $start and from_exclusive < $end""" ++ participantFilter

    storage.queryUnlessShutdown(query.as[SignedProtocolMessage[AcsCommitment]], functionFullName)

  }

  override val runningCommitments: DbIncrementalCommitmentStore =
    new DbIncrementalCommitmentStore(
      storage,
      indexedDomain,
      protocolVersion,
      timeouts,
      loggerFactory,
    )

  override val queue: DbCommitmentQueue =
    new DbCommitmentQueue(storage, indexedDomain, protocolVersion, timeouts, loggerFactory)

  override def onClosed(): Unit =
    LifeCycle.close(
      runningCommitments,
      queue,
      markSafeQueue,
    )(logger)
}

class DbIncrementalCommitmentStore(
    override protected val storage: DbStorage,
    indexedDomain: IndexedDomain,
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
      res <- storage.queryUnlessShutdown(
        (for {
          tsWithTieBreaker <-
            sql"""select ts, tie_breaker from par_commitment_snapshot_time where domain_idx = $indexedDomain"""
              .as[(CantonTimestamp, Long)]
              .headOption
          snapshot <-
            sql"""select stakeholders, commitment from par_commitment_snapshot where domain_idx = $indexedDomain"""
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
      sql"""select ts, tie_breaker from par_commitment_snapshot_time where domain_idx=$indexedDomain"""
        .as[(CantonTimestamp, Long)]
        .headOption
    storage
      .queryUnlessShutdown(query, operationName = "commitments: read snapshot watermark")
      .map(_.fold(RecordTime.MinValue) { case (ts, tieBreaker) => RecordTime(ts, tieBreaker) })
  }

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
        "delete from par_commitment_snapshot where domain_idx = ? and stakeholders_hash = ?"
      DbStorage.bulkOperation_(deleteStatement, stakeholders, storage.profile) { pp => stkhs =>
        pp >> indexedDomain
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
          pp >> indexedDomain
          pp >> partySetHash(stkhs)
          pp >> StoredParties(stkhs)
          pp >> commitment
      }

      val statement = storage.profile match {
        case _: DbStorage.Profile.H2 =>
          """merge into par_commitment_snapshot (domain_idx, stakeholders_hash, stakeholders, commitment)
                   values (?, ?, ?, ?)"""

        case _: DbStorage.Profile.Postgres =>
          """insert into par_commitment_snapshot (domain_idx, stakeholders_hash, stakeholders, commitment)
                 values (?, ?, ?, ?) on conflict (domain_idx, stakeholders_hash) do update set commitment = excluded.commitment"""
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
          sqlu"""merge into par_commitment_snapshot_time (domain_idx, ts, tie_breaker) values ($indexedDomain, ${rt.timestamp}, ${rt.tieBreaker})"""
        case _: DbStorage.Profile.Postgres =>
          sqlu"""insert into par_commitment_snapshot_time(domain_idx, ts, tie_breaker) values ($indexedDomain, ${rt.timestamp}, ${rt.tieBreaker})
                 on conflict (domain_idx) do update set ts = ${rt.timestamp}, tie_breaker = ${rt.tieBreaker}"""
      }

    val updateList = updates.toList
    val deleteList = deletes.toList

    val operations = List(insertRt(rt), storeUpdates(updateList), deleteCommitments(deleteList))

    storage.queryAndUpdateUnlessShutdown(
      DBIO.seq(operations*).transactionally.withTransactionIsolation(Serializable),
      operationName = "commitments: incremental commitments snapshot update",
    )
  }
}

class DbCommitmentQueue(
    override protected val storage: DbStorage,
    indexedDomain: IndexedDomain,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends CommitmentQueue
    with DbStore {

  import DbStorage.Implicits.*
  import storage.api.*

  private implicit val acsCommitmentReader: GetResult[AcsCommitment] =
    AcsCommitment.getAcsCommitmentResultReader(indexedDomain.domainId, protocolVersion)

  override def enqueue(
      commitment: AcsCommitment
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val commitmentDbHash =
      Hash.digest(HashPurpose.AcsCommitmentDb, commitment.commitment, HashAlgorithm.Sha256)
    val insertAction =
      sqlu"""insert
             into par_commitment_queue(domain_idx, sender, counter_participant, from_exclusive, to_inclusive, commitment, commitment_hash)
             values($indexedDomain, ${commitment.sender}, ${commitment.counterParticipant}, ${commitment.period.fromExclusive}, ${commitment.period.toInclusive}, ${commitment.commitment}, $commitmentDbHash)
             on conflict do nothing"""

    storage.updateUnlessShutdown_(insertAction, operationName = "enqueue commitment")
  }

  /** Returns all commitments whose period ends at or before the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThrough(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[AcsCommitment]] =
    storage
      .queryUnlessShutdown(
        sql"""select sender, counter_participant, from_exclusive, to_inclusive, commitment
             from par_commitment_queue
             where domain_idx = $indexedDomain and to_inclusive <= $timestamp"""
          .as[AcsCommitment],
        operationName = NameOf.qualifiedNameOfCurrentFunc,
      )
      .map(_.toList)

  /** Returns all commitments whose period ends at or after the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThroughAtOrAfter(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[AcsCommitment]] =
    storage
      .queryUnlessShutdown(
        sql"""select sender, counter_participant, from_exclusive, to_inclusive, commitment
                                            from par_commitment_queue
                                            where domain_idx = $indexedDomain and to_inclusive >= $timestamp"""
          .as[AcsCommitment],
        operationName = NameOf.qualifiedNameOfCurrentFunc,
      )

  def peekOverlapsForCounterParticipant(
      period: CommitmentPeriod,
      counterParticipant: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[AcsCommitment]] =
    storage
      .queryUnlessShutdown(
        sql"""select sender, counter_participant, from_exclusive, to_inclusive, commitment
                 from par_commitment_queue
                 where domain_idx = $indexedDomain and sender = $counterParticipant
                 and to_inclusive > ${period.fromExclusive}
                 and from_exclusive < ${period.toInclusive} """
          .as[AcsCommitment],
        operationName = NameOf.qualifiedNameOfCurrentFunc,
      )

  /** Deletes all commitments whose period ends at or before the given timestamp. */
  override def deleteThrough(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.updateUnlessShutdown_(
      sqlu"delete from par_commitment_queue where domain_idx = $indexedDomain and to_inclusive <= $timestamp",
      operationName = "delete queued commitments",
    )
}
