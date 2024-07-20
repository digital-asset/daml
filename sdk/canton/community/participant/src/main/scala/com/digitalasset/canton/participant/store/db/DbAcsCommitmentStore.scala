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
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.{
  SortedReconciliationIntervals,
  SortedReconciliationIntervalsProvider,
}
import com.digitalasset.canton.participant.store.AcsCommitmentStore.CommitmentData
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
  CommitmentQueue,
  IncrementalCommitmentStore,
}
import com.digitalasset.canton.protocol.StoredParties
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
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
    override val domainId: IndexedDomain,
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
  ): Future[Iterable[(CommitmentPeriod, AcsCommitment.CommitmentType)]] = {
    val query = sql"""
        select from_exclusive, to_inclusive, commitment from par_computed_acs_commitments
          where domain_id = $domainId
            and counter_participant = $counterParticipant
            and from_exclusive < ${period.toInclusive}
            and to_inclusive > ${period.fromExclusive}
          order by from_exclusive asc"""
      .as[(CommitmentPeriod, AcsCommitment.CommitmentType)]

    storage.query(query, operationName = "commitments: get computed")
  }

  override def storeComputed(
      items: NonEmpty[Seq[AcsCommitmentStore.CommitmentData]]
  )(implicit traceContext: TraceContext): Future[Unit] = {

    // We want to detect if we try to overwrite an existing commitment with a different value, as this signals an error.
    // Still, for performance reasons, we want to do everything in a single, non-interactive statement.
    // We reconcile the two by an upsert that inserts only on a "conflict" where the values are equal, and
    // requiring that at least one value is written.

    def setData(pp: PositionedParameters)(item: CommitmentData): Unit = {
      val CommitmentData(counterParticipant, period, commitment) = item
      pp >> domainId
      pp >> counterParticipant
      pp >> period.fromExclusive
      pp >> period.toInclusive
      pp >> commitment
    }

    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        """merge into par_computed_acs_commitments cs
            using (
              select cast(? as int) domain_id, cast(? as varchar(300)) counter_participant, cast(? as bigint) from_exclusive,
              cast(? as bigint) to_inclusive, cast(? as binary large object) commitment from dual)
            excluded on (cs.domain_id = excluded.domain_id and cs.counter_participant = excluded.counter_participant and
            cs.from_exclusive = excluded.from_exclusive and cs.to_inclusive = excluded.to_inclusive)
            when matched and cs.commitment = excluded.commitment
              then update set cs.commitment = excluded.commitment
            when not matched then
               insert (domain_id, counter_participant, from_exclusive, to_inclusive, commitment)
               values (excluded.domain_id, excluded.counter_participant, excluded.from_exclusive, excluded.to_inclusive, excluded.commitment)
            """
      case _: DbStorage.Profile.Postgres =>
        """insert into par_computed_acs_commitments(domain_id, counter_participant, from_exclusive, to_inclusive, commitment)
               values (?, ?, ?, ?, ?)
                  on conflict (domain_id, counter_participant, from_exclusive, to_inclusive) do
                    update set commitment = excluded.commitment
                    where par_computed_acs_commitments.commitment = excluded.commitment
          """
      case _: DbStorage.Profile.Oracle =>
        """merge into par_computed_acs_commitments cs
                 using (
                      select ? domain_id, ? counter_participant, ? from_exclusive, ? to_inclusive, ? commitment from dual)
                    excluded on (cs.domain_id = excluded.domain_id and cs.counter_participant = excluded.counter_participant and
                    cs.from_exclusive = excluded.from_exclusive and cs.to_inclusive = excluded.to_inclusive)
                when matched then
                  update set cs.commitment = excluded.commitment
                  where dbms_lob.compare(cs.commitment, excluded.commitment) = 0
                when not matched then
                  insert (domain_id, counter_participant, from_exclusive, to_inclusive, commitment)
                  values (excluded.domain_id, excluded.counter_participant, excluded.from_exclusive, excluded.to_inclusive, excluded.commitment)
          """
    }

    val bulkUpsert = DbStorage.bulkOperation(query, items.toList, storage.profile)(setData)

    storage.queryAndUpdate(bulkUpsert, "commitments: insert computed").map { rowCounts =>
      rowCounts.zip(items.toList).foreach { case (rowCount, item) =>
        val CommitmentData(counterParticipant, period, commitment) = item
        // Underreporting of the affected rows should not matter here as the query is idempotent and updates the row even if the same values had been there before
        ErrorUtil.requireState(
          rowCount != 0,
          s"Commitment for domain $domainId, counterparticipant $counterParticipant and period $period already computed with a different value; refusing to insert $commitment",
        )
      }
    }
  }

  override def markOutstanding(period: CommitmentPeriod, counterParticipants: Set[ParticipantId])(
      implicit traceContext: TraceContext
  ): Future[Unit] = {
    logger.debug(
      s"Marking $period as outstanding for ${counterParticipants.size} remote participants"
    )
    if (counterParticipants.isEmpty) Future.unit
    else {
      import DbStorage.Implicits.BuilderChain.*

      // Slick doesn't support bulk insertions by default, so we have to stitch our own
      val insertOutstanding =
        storage.profile match {
          case _: DbStorage.Profile.Oracle =>
            (sql"""merge into par_outstanding_acs_commitments
                using (with updates as (""" ++
              counterParticipants.toList
                .map(p =>
                  sql"select $domainId did, ${period.fromExclusive} periodFrom, ${period.toInclusive} periodTo, $p counter_participant from dual"
                )
                .intercalate(sql" union all ") ++
              sql""") select * from updates) U on (
                     U.did = par_outstanding_acs_commitments.domain_id and
                     U.periodFrom = par_outstanding_acs_commitments.from_exclusive and
                     U.periodTo = par_outstanding_acs_commitments.to_inclusive and
                     U.counter_participant = par_outstanding_acs_commitments.counter_participant)
                     when not matched then
                     insert (domain_id, from_exclusive, to_inclusive, counter_participant)
                     values (U.did, U.periodFrom, U.periodTo, U.counter_participant)""").asUpdate
          case _ =>
            (sql"""insert into par_outstanding_acs_commitments (domain_id, from_exclusive, to_inclusive, counter_participant) values """ ++
              counterParticipants.toList
                .map(p => sql"($domainId, ${period.fromExclusive}, ${period.toInclusive}, $p)")
                .intercalate(sql", ") ++ sql" on conflict do nothing").asUpdate

        }
      storage.update_(insertOutstanding, operationName = "commitments: storeOutstanding")
    }
  }

  override def markComputedAndSent(
      period: CommitmentPeriod
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val timestamp = period.toInclusive
    val upsertQuery = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_last_computed_acs_commitments(domain_id, ts) values ($domainId, $timestamp)"""
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_last_computed_acs_commitments(domain_id, ts) values ($domainId, $timestamp)
                 on conflict (domain_id) do update set ts = $timestamp"""
      case _: DbStorage.Profile.Oracle =>
        sqlu"""merge into par_last_computed_acs_commitments lcac
                 using (
                  select
                    $domainId domain_id,
                    $timestamp ts
                    from dual
                  ) parameters
                 on (lcac.domain_id = parameters.domain_id)
                 when matched then
                  update set lcac.ts = parameters.ts
                 when not matched then
                  insert (domain_id, ts) values (parameters.domain_id, parameters.ts)
                  """
    }

    storage.update_(upsertQuery, operationName = "commitments: markComputedAndSent")
  }

  override def outstanding(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit traceContext: TraceContext): Future[Iterable[(CommitmentPeriod, ParticipantId)]] = {
    val participantFilter =
      counterParticipant.fold(sql"")(p => sql" and counter_participant = $p")
    import DbStorage.Implicits.BuilderChain.*
    val query =
      sql"""select from_exclusive, to_inclusive, counter_participant
                    from par_outstanding_acs_commitments where domain_id = $domainId and to_inclusive >= $start and from_exclusive < $end
                """ ++ participantFilter
    storage.query(
      query
        .as[(CommitmentPeriod, ParticipantId)]
        .transactionally
        .withTransactionIsolation(TransactionIsolation.ReadCommitted),
      operationName = functionFullName,
    )
  }

  override def storeReceived(
      commitment: SignedProtocolMessage[AcsCommitment]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val sender = commitment.message.sender
    val from = commitment.message.period.fromExclusive
    val to = commitment.message.period.toInclusive
    val serialized = commitment.toByteArray

    val upsertQuery = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_received_acs_commitments
            using dual
            on domain_id = $domainId and sender = $sender and from_exclusive = $from and to_inclusive = $to and signed_commitment = $serialized
            when not matched then
               insert (domain_id, sender, from_exclusive, to_inclusive, signed_commitment)
               values ($domainId, $sender, $from, $to, $serialized)
            """
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_received_acs_commitments(domain_id, sender, from_exclusive, to_inclusive, signed_commitment)
               select $domainId, $sender, $from, $to, $serialized
               where not exists(
                 select * from par_received_acs_commitments
                 where domain_id = $domainId and sender = $sender and from_exclusive = $from and to_inclusive = $to and signed_commitment = $serialized)
          """
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert into par_received_acs_commitments(domain_id, sender, from_exclusive, to_inclusive, signed_commitment)
               select $domainId, $sender, $from, $to, $serialized
               from dual
               where not exists(
                 select * from par_received_acs_commitments
                 where domain_id = $domainId and sender = $sender and from_exclusive = $from and to_inclusive = $to
                   and dbms_lob.compare(signed_commitment, $serialized) = 0)
          """
    }
    storage.update_(
      upsertQuery,
      operationName = "commitments: inserting ACS commitment",
    )
  }

  override def markSafe(
      counterParticipant: ParticipantId,
      period: CommitmentPeriod,
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    def dbQueries(
        sortedReconciliationIntervals: SortedReconciliationIntervals
    ): DBIOAction[Unit, NoStream, Effect.All] = {
      def containsTick(commitmentPeriod: CommitmentPeriod): Boolean = sortedReconciliationIntervals
        .containsTick(
          commitmentPeriod.fromExclusive.forgetRefinement,
          commitmentPeriod.toInclusive.forgetRefinement,
        )
        .getOrElse {
          logger.warn(s"Unable to determine whether $commitmentPeriod contains a tick.")
          true
        }

      val insertQuery = storage.profile match {
        case Profile.H2(_) | Profile.Postgres(_) =>
          """insert into par_outstanding_acs_commitments (domain_id, from_exclusive, to_inclusive, counter_participant)
               values (?, ?, ?, ?) on conflict do nothing"""

        case Profile.Oracle(_) =>
          """merge /*+ INDEX ( par_outstanding_acs_commitments ( counter_participant, domain_id, from_exclusive, to_inclusive ) ) */
            |into par_outstanding_acs_commitments t
            |using (select ? domain_id, ? from_exclusive, ? to_inclusive, ? counter_participant from dual) input
            |on (t.counter_participant = input.counter_participant and t.domain_id = input.domain_id and
            |    t.from_exclusive = input.from_exclusive and t.to_inclusive = input.to_inclusive)
            |when not matched then
            |  insert (domain_id, from_exclusive, to_inclusive, counter_participant)
            |  values (input.domain_id, input.from_exclusive, input.to_inclusive, input.counter_participant)""".stripMargin
      }

      /*
      This is a three steps process:
      - First, we select the outstanding intervals that are overlapping with the new one.
      - Then, we delete the overlapping intervals.
      - Finally, we insert what we need (non-empty intersections)
       */
      for {
        overlappingIntervals <-
          sql"""select from_exclusive, to_inclusive from par_outstanding_acs_commitments
          where domain_id = $domainId and counter_participant = $counterParticipant
          and from_exclusive < ${period.toInclusive} and to_inclusive > ${period.fromExclusive}"""
            .as[(CantonTimestampSecond, CantonTimestampSecond)]

        _ <-
          sqlu"""delete from par_outstanding_acs_commitments
          where domain_id = $domainId and counter_participant = $counterParticipant
          and from_exclusive < ${period.toInclusive} and to_inclusive > ${period.fromExclusive}"""

        newPeriods = overlappingIntervals
          .flatMap { case (from, to) =>
            val leftOverlap = CommitmentPeriod.create(from, period.fromExclusive)
            val rightOverlap = CommitmentPeriod.create(period.toInclusive, to)
            leftOverlap.toSeq ++ rightOverlap.toSeq
          }
          .toList
          .distinct
          .filter(containsTick)

        _ <- DbStorage.bulkOperation_(insertQuery, newPeriods, storage.profile) { pp => newPeriod =>
          pp >> domainId
          pp >> newPeriod.fromExclusive
          pp >> newPeriod.toInclusive
          pp >> counterParticipant
        }

      } yield ()
    }

    markSafeQueue
      .execute(
        {
          for {
            /*
          That could be wrong if a period is marked as outstanding between the point where we
          fetch the approximate timestamp of the topology client and the query for the sorted
          reconciliation intervals.
          Such a period would be kept as outstanding even if it contains no tick. On the other
          hand, only commitment periods around restarts could be "empty" (not contain any tick).
             */
            sortedReconciliationIntervals <-
              sortedReconciliationIntervalsProvider.approximateReconciliationIntervals
            _ <- storage.queryAndUpdate(
              dbQueries(sortedReconciliationIntervals).transactionally.withTransactionIsolation(
                TransactionIsolation.Serializable
              ),
              operationName =
                s"commitments: mark period safe (${period.fromExclusive}, ${period.toInclusive}]",
            )
          } yield ()
        },
        "Run mark period safe DB query",
      )
      .onShutdown(
        logger.debug(
          s"Aborted marking period safe (${period.fromExclusive}, ${period.toInclusive}] due to shutdown"
        )
      )
  }

  override def doPrune(
      before: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Int] = {
    val query1 =
      sqlu"delete from par_received_acs_commitments where domain_id=$domainId and to_inclusive < $before"
    val query2 =
      sqlu"delete from par_computed_acs_commitments where domain_id=$domainId and to_inclusive < $before"
    storage
      .queryAndUpdate(
        query1.zip(query2),
        operationName = "commitments: prune",
      )
      .map(x => x._1 + x._2)
  }

  override def lastComputedAndSent(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestampSecond]] = {
    storage.query(
      sql"select ts from par_last_computed_acs_commitments where domain_id = $domainId"
        .as[CantonTimestampSecond]
        .headOption,
      functionFullName,
    )
  }

  override def noOutstandingCommitments(beforeOrAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = {
    for {
      computed <- lastComputedAndSent
      adjustedTsOpt = computed.map(_.forgetRefinement.min(beforeOrAt))
      outstandingOpt <- adjustedTsOpt.traverse { ts =>
        storage.query(
          sql"select from_exclusive, to_inclusive from par_outstanding_acs_commitments where domain_id=$domainId and from_exclusive < $ts"
            .as[(CantonTimestamp, CantonTimestamp)]
            .withTransactionIsolation(Serializable),
          operationName = "commitments: compute no outstanding",
        )
      }
    } yield {
      for {
        ts <- adjustedTsOpt
        outstanding <- outstandingOpt
      } yield AcsCommitmentStore.latestCleanPeriod(ts, outstanding)
    }
  }

  override def searchComputedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): Future[Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)]] = {
    val query = counterParticipant match {
      case Some(p) =>
        sql"""select from_exclusive, to_inclusive, counter_participant, commitment
            from par_computed_acs_commitments
            where domain_id = $domainId and to_inclusive >= $start and from_exclusive < $end
                and counter_participant = $p"""
      case None =>
        sql"""select from_exclusive, to_inclusive, counter_participant, commitment
            from par_computed_acs_commitments
            where domain_id = $domainId and to_inclusive >= $start and from_exclusive < $end"""
    }
    storage.query(
      query.as[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)],
      functionFullName,
    )
  }

  override def searchReceivedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit traceContext: TraceContext): Future[Iterable[SignedProtocolMessage[AcsCommitment]]] = {
    val query = counterParticipant match {
      case Some(p) =>
        sql"""select signed_commitment
            from par_received_acs_commitments
            where domain_id = $domainId and to_inclusive >= $start and from_exclusive < $end
            and sender = $p"""
      case None =>
        sql"""select signed_commitment
            from par_received_acs_commitments
            where domain_id = $domainId and to_inclusive >= $start and from_exclusive < $end"""
    }
    storage.query(query.as[SignedProtocolMessage[AcsCommitment]], functionFullName)

  }

  override val runningCommitments: DbIncrementalCommitmentStore =
    new DbIncrementalCommitmentStore(storage, domainId, protocolVersion, timeouts, loggerFactory)

  override val queue: DbCommitmentQueue =
    new DbCommitmentQueue(storage, domainId, protocolVersion, timeouts, loggerFactory)

  override def onClosed(): Unit = {
    Lifecycle.close(
      runningCommitments,
      queue,
      markSafeQueue,
    )(logger)
  }
}

class DbIncrementalCommitmentStore(
    override protected val storage: DbStorage,
    domainId: IndexedDomain,
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
  ): Future[(RecordTime, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType])] = {
    for {
      res <- storage.query(
        (for {
          tsWithTieBreaker <-
            sql"""select ts, tie_breaker from par_commitment_snapshot_time where domain_id = $domainId"""
              .as[(CantonTimestamp, Long)]
              .headOption
          snapshot <-
            sql"""select stakeholders, commitment from par_commitment_snapshot where domain_id = $domainId"""
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
  }

  override def watermark(implicit traceContext: TraceContext): Future[RecordTime] = {
    val query =
      sql"""select ts, tie_breaker from par_commitment_snapshot_time where domain_id=$domainId"""
        .as[(CantonTimestamp, Long)]
        .headOption
    storage
      .query(query, operationName = "commitments: read snapshot watermark")
      .map(_.fold(RecordTime.MinValue) { case (ts, tieBreaker) => RecordTime(ts, tieBreaker) })
  }

  def update(
      rt: RecordTime,
      updates: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      deletes: Set[SortedSet[LfPartyId]],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    def partySetHash(parties: SortedSet[LfPartyId]): String68 = {
      Hash
        .digest(
          HashPurpose.Stakeholders,
          DeterministicEncoding.encodeSeqWith(parties.toList)(DeterministicEncoding.encodeParty),
          HashAlgorithm.Sha256,
        )
        .toLengthLimitedHexString
    }
    def deleteCommitments(stakeholders: List[SortedSet[LfPartyId]]): DbAction.All[Unit] = {
      val deleteStatement =
        "delete from par_commitment_snapshot where domain_id = ? and stakeholders_hash = ?"
      DbStorage.bulkOperation_(deleteStatement, stakeholders, storage.profile) { pp => stkhs =>
        pp >> domainId
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
          pp >> domainId
          pp >> partySetHash(stkhs)
          pp >> StoredParties(stkhs)
          pp >> commitment
      }

      val statement = storage.profile match {
        case _: DbStorage.Profile.H2 =>
          """merge into par_commitment_snapshot (domain_id, stakeholders_hash, stakeholders, commitment)
                   values (?, ?, ?, ?)"""

        case _: DbStorage.Profile.Postgres =>
          """insert into par_commitment_snapshot (domain_id, stakeholders_hash, stakeholders, commitment)
                 values (?, ?, ?, ?) on conflict (domain_id, stakeholders_hash) do update set commitment = excluded.commitment"""

        case _: DbStorage.Profile.Oracle =>
          """merge into par_commitment_snapshot cs
             using (
              select
                ? domain_id,
                ? stakeholders_hash,
                ? stakeholders,
                ? commitment
                from dual
              ) excluded
              on (cs.domain_id = excluded.domain_id and cs.stakeholders_hash = excluded.stakeholders_hash)
              when matched then
                update set cs.commitment = excluded.commitment
              when not matched then
                 insert (domain_id, stakeholders_hash, stakeholders, commitment)
                 values (excluded.domain_id, excluded.stakeholders_hash, excluded.stakeholders, excluded.commitment)"""
      }
      DbStorage.bulkOperation_(
        statement,
        updates,
        storage.profile,
      )(setParams)
    }

    def insertRt(rt: RecordTime): DbAction.WriteOnly[Int] = {
      storage.profile match {
        case _: DbStorage.Profile.H2 =>
          sqlu"""merge into par_commitment_snapshot_time (domain_id, ts, tie_breaker) values ($domainId, ${rt.timestamp}, ${rt.tieBreaker})"""
        case _: DbStorage.Profile.Postgres =>
          sqlu"""insert into par_commitment_snapshot_time(domain_id, ts, tie_breaker) values ($domainId, ${rt.timestamp}, ${rt.tieBreaker})
                on conflict (domain_id) do update set ts = ${rt.timestamp}, tie_breaker = ${rt.tieBreaker}"""
        case _: DbStorage.Profile.Oracle =>
          sqlu"""merge into par_commitment_snapshot_time cst
                 using dual
                on (cst.domain_id = $domainId)
                when matched then
                  update set ts = ${rt.timestamp}, tie_breaker = ${rt.tieBreaker}
                when not matched then
                  insert (domain_id, ts, tie_breaker) values ($domainId, ${rt.timestamp}, ${rt.tieBreaker})
                  """
      }
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
    domainId: IndexedDomain,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends CommitmentQueue
    with DbStore {

  import DbStorage.Implicits.*
  import storage.api.*

  private implicit val acsCommitmentReader: GetResult[AcsCommitment] =
    AcsCommitment.getAcsCommitmentResultReader(domainId.item, protocolVersion)

  override def enqueue(
      commitment: AcsCommitment
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val commitmentDbHash =
      Hash.digest(HashPurpose.AcsCommitmentDb, commitment.commitment, HashAlgorithm.Sha256)
    val insertAction = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
               /*+ IGNORE_ROW_ON_DUPKEY_INDEX ( PAR_COMMITMENT_QUEUE ( commitment_hash, domain_id, sender, counter_participant, from_exclusive, to_inclusive ) ) */
               into par_commitment_queue(domain_id, sender, counter_participant, from_exclusive, to_inclusive, commitment, commitment_hash)
               values($domainId, ${commitment.sender}, ${commitment.counterParticipant}, ${commitment.period.fromExclusive}, ${commitment.period.toInclusive}, ${commitment.commitment}, $commitmentDbHash)"""
      case _ =>
        sqlu"""insert
               into par_commitment_queue(domain_id, sender, counter_participant, from_exclusive, to_inclusive, commitment, commitment_hash)
               values($domainId, ${commitment.sender}, ${commitment.counterParticipant}, ${commitment.period.fromExclusive}, ${commitment.period.toInclusive}, ${commitment.commitment}, $commitmentDbHash)
               on conflict do nothing"""
    }

    {
      storage.update_(insertAction, operationName = "enqueue commitment")
    }
  }

  /** Returns all commitments whose period ends at or before the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThrough(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[List[AcsCommitment]] = {
    storage
      .query(
        sql"""select sender, counter_participant, from_exclusive, to_inclusive, commitment
             from par_commitment_queue
             where domain_id = $domainId and to_inclusive <= $timestamp"""
          .as[AcsCommitment],
        operationName = NameOf.qualifiedNameOfCurrentFunc,
      )
      .map(_.toList)
  }

  /** Returns all commitments whose period ends at or after the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThroughAtOrAfter(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Seq[AcsCommitment]] = {
    storage
      .query(
        sql"""select sender, counter_participant, from_exclusive, to_inclusive, commitment
                                            from par_commitment_queue
                                            where domain_id = $domainId and to_inclusive >= $timestamp"""
          .as[AcsCommitment],
        operationName = NameOf.qualifiedNameOfCurrentFunc,
      )
  }

  def peekOverlapsForCounterParticipant(
      period: CommitmentPeriod,
      counterParticipant: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[AcsCommitment]] = {
    storage
      .query(
        sql"""select sender, counter_participant, from_exclusive, to_inclusive, commitment
                 from par_commitment_queue
                 where domain_id = $domainId and sender = $counterParticipant
                 and to_inclusive > ${period.fromExclusive}
                 and from_exclusive < ${period.toInclusive} """
          .as[AcsCommitment],
        operationName = NameOf.qualifiedNameOfCurrentFunc,
      )
  }

  /** Deletes all commitments whose period ends at or before the given timestamp. */
  override def deleteThrough(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    storage.update_(
      sqlu"delete from par_commitment_queue where domain_id = $domainId and to_inclusive <= $timestamp",
      operationName = "delete queued commitments",
    )
  }
}
