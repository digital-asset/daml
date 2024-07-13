// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Monad
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.store.ContractKeyJournal
import com.digitalasset.canton.participant.store.ContractKeyJournal.ContractKeyJournalError
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfGlobalKey, LfHash}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbPrunableByTimeDomain
import com.digitalasset.canton.store.{IndexedDomain, PrunableByTimeParameters}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.{ExecutionContext, Future}

class DbContractKeyJournal(
    override protected val storage: DbStorage,
    override val domainId: IndexedDomain,
    batching: BatchingConfig,
    batchingParametersConfig: PrunableByTimeParameters,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override protected[this] implicit val ec: ExecutionContext)
    extends ContractKeyJournal
    with DbStore
    with DbPrunableByTimeDomain {

  import ContractKeyJournal.*
  import DbStorage.Implicits.*
  import storage.api.*

  override protected def batchingParameters: Option[PrunableByTimeParameters] = Some(
    batchingParametersConfig
  )
  override protected def kind: String = "contract keys"

  override protected val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("contract-key-journal")

  override protected[this] def pruning_status_table: String = "contract_key_pruning"

  override def fetchStates(
      keys: Iterable[LfGlobalKey]
  )(implicit traceContext: TraceContext): Future[Map[LfGlobalKey, ContractKeyState]] =
    if (keys.isEmpty) Future.successful(Map.empty)
    else {
      processingTime.event {
        import DbStorage.Implicits.BuilderChain.*
        NonEmpty.from(keys.toSeq) match {
          case None => Future.successful(Map.empty)
          case Some(keysNel) =>
            val inClauses = DbStorage.toInClauses_(
              "contract_key_hash",
              keysNel,
              batching.maxItemsInSqlClause,
            )
            val queries = inClauses.map { inClause =>
              val query =
                storage.profile match {
                  case _: DbStorage.Profile.Oracle =>
                    sql"""
              select contract_key_hash, status, ts, request_counter from
              (
                select contract_key_hash, status, ts, request_counter,
                   ROW_NUMBER() OVER (partition by domain_id, contract_key_hash order by ts desc, request_counter desc) as row_num
                 from contract_key_journal
                 where domain_id = $domainId and """ ++ inClause ++ sql"""
              ) ordered_changes
              where row_num = 1
              """
                  case _ =>
                    sql"""
              with ordered_changes(contract_key_hash, status, ts, request_counter, row_num) as (
                select contract_key_hash, status, ts, request_counter,
                   ROW_NUMBER() OVER (partition by domain_id, contract_key_hash order by ts desc, request_counter desc)
                 from contract_key_journal
                 where domain_id = $domainId and """ ++ inClause ++ sql"""
              )
              select contract_key_hash, status, ts, request_counter
              from ordered_changes
              where row_num = 1;
              """

                }
              query.as[(LfHash, ContractKeyState)]
            }
            for {
              foundKeysVector <- storage.sequentialQueryAndCombine(
                queries,
                functionFullName,
              )
            } yield {
              val foundKeys = foundKeysVector.toMap
              keys.to(LazyList).mapFilter(key => foundKeys.get(key.hash).map(key -> _)).toMap
            }
        }
      }
    }

  /** Add updates for the selected keys
    * @param updates All the updates
    * @param keys Keys to be updated
    * @return Number of rows updated
    */
  private def addUpdates(
      updates: Map[LfGlobalKey, (Status, TimeOfChange)],
      keys: LazyList[LfGlobalKey],
  )(implicit traceContext: TraceContext) = {
    import DbStorage.Implicits.BuilderChain.*

    val values: DbStorage.SQLActionBuilderChain =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          keys
            .map { key =>
              val (status, toc) = updates(key)
              sql"select $domainId domain_id, $key contract_key_hash, ${checked(
                  status
                )} status, ${toc.timestamp} ts, ${toc.rc} request_counter from dual"
            }
            .intercalate(sql" union all ")
        case _ =>
          keys
            .map { key =>
              val (status, toc) = updates(key)
              sql"""($domainId, $key, CAST(${checked(
                  status
                )} as key_status), ${toc.timestamp}, ${toc.rc})"""
            }
            .intercalate(sql", ")
      }

    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sql"""
                  merge into contract_key_journal as journal
                  using (select * from (values """ ++ values ++
          sql""") as upds(domain_id, contract_key_hash, status, ts, request_counter)) as source
                  on journal.domain_id = source.domain_id and
                     journal.contract_key_hash = source.contract_key_hash and
                     journal.ts = source.ts and
                     journal.request_counter = source.request_counter
                  when not matched then
                    insert (domain_id, contract_key_hash, status, ts, request_counter)
                     values (source.domain_id, source.contract_key_hash, source.status, source.ts, source.request_counter);
                  """
      case _: DbStorage.Profile.Postgres =>
        sql"""
                  insert into contract_key_journal(domain_id, contract_key_hash, status, ts, request_counter)
                  values """ ++ values ++
          sql"""
                  on conflict (domain_id, contract_key_hash, ts, request_counter) do nothing;
                  """
      case _: DbStorage.Profile.Oracle =>
        sql"""
                      merge into contract_key_journal ckj USING (with UPDATES as(""" ++ values ++
          sql""") select * from UPDATES) S on ( ckj.domain_id=S.domain_id and ckj.contract_key_hash=S.contract_key_hash and ckj.ts=S.ts and ckj.request_counter=S.request_counter)
                       when not matched then
                       insert (domain_id, contract_key_hash, status, ts, request_counter)
                       values (S.domain_id, S.contract_key_hash, S.status, S.ts, S.request_counter)
                     """
    }

    storage.update(query.asUpdate, functionFullName)
  }

  override def addKeyStateUpdates(updates: Map[LfGlobalKey, (Status, TimeOfChange)])(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] = {
    MonadUtil.batchedSequentialTraverse_(
      parallelism = batching.parallelism,
      chunkSize = batching.maxItemsInSqlClause,
    )(
      updates.keySet.toSeq
    ) { keyUpdates =>
      processingTime.eitherTEvent {
        import DbStorage.Implicits.BuilderChain.*
        // Keep trying to insert the updates until all updates are in the DB or an exception occurs or we've found an inconsistency.
        // This is not necessarily something we expect to happen but has been added here to catch programming
        // or other errors.
        val result =
          Monad[Future].tailRecM[LazyList[LfGlobalKey], Either[ContractKeyJournalError, Unit]](
            keyUpdates.to(LazyList)
          ) { remainingKeys =>
            if (remainingKeys.isEmpty) {
              Future.successful(Either.right(Either.right(())))
            } else {
              val updatesCount = remainingKeys.length

              addUpdates(updates, remainingKeys).flatMap { rowCount =>
                if (rowCount == updatesCount) Future.successful(Either.right(Either.right(())))
                else {
                  val keysQ = remainingKeys.map(key => sql"$key").intercalate(sql", ")
                  val (tss, rcs) =
                    remainingKeys
                      .collect(updates)
                      .map { case (_, toc) => (toc.timestamp, toc.rc) }
                      .unzip
                  val tsQ = tss.map(ts => sql"$ts").intercalate(sql", ")
                  val rcQ = rcs.map(rc => sql"$rc").intercalate(sql", ")
                  // We read all keys to be written rather than those keys mapped to a different value
                  // so that we find out if some key is still missing in the DB.

                  val keyStatesQ =
                    sql"""
                select contract_key_hash, status
                from contract_key_journal
                where domain_id = $domainId and contract_key_hash in (""" ++ keysQ ++
                      sql""") and ts in (""" ++ tsQ ++ sql""") and request_counter in (""" ++ rcQ ++
                      sql""")
                """
                  storage.query(keyStatesQ.as[(LfHash, Status)], functionFullName).map {
                    keysWithStatus =>
                      val found = keysWithStatus.map { case (keyHash, status) =>
                        keyHash -> status
                      }.toMap
                      val wrongOrMissing = remainingKeys.traverse { key =>
                        found.get(key.hash) match {
                          case None => Either.right(Some(key))
                          case Some(status) =>
                            val (newStatus, toc) = checked(updates(key))
                            Either.cond(
                              status == newStatus,
                              None,
                              InconsistentKeyAllocationStatus(key, toc, status, newStatus),
                            )
                        }
                      }
                      wrongOrMissing match {
                        case Left(wrong) => Either.right(Either.left(wrong))
                        case Right(missingO) =>
                          val missing = missingO.flattenOption
                          Either.cond(missing.isEmpty, Either.right(()), missing)
                      }
                  }
                }
              }
            }
          }

        EitherT(result)
      }
    }
  }

  override def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Int] =
    processingTime
      .event {
        val lastPruningTs = lastPruning.getOrElse(CantonTimestamp.MinValue)
        val query = storage.profile match {
          case _: DbStorage.Profile.H2 =>
            sqlu"""
          with ordered_changes(domain_id, contract_key_hash, status, ts, request_counter, row_num) as (
             select domain_id, contract_key_hash, status, ts, request_counter,
               ROW_NUMBER() OVER (partition by domain_id, contract_key_hash order by ts desc, request_counter desc)
             from contract_key_journal
             where domain_id = $domainId and ts <= $beforeAndIncluding
          )
          delete from contract_key_journal
          where
            (domain_id, contract_key_hash, ts, request_counter) in (
              select domain_id, contract_key_hash, ts, request_counter
              from ordered_changes
              where ordered_changes.row_num > 1 or ordered_changes.status = ${ContractKeyJournal.Unassigned}
            );
          """
          case _: DbStorage.Profile.Postgres =>
            // Delete old key journal entries for which we have a newer entry
            // We restrict the deletion to keys that have been updated since the last pruning as anything else remains unchanged,
            // so doesn't need to be considered.
            // Note that the last condition is <= and has a "unassigned" as a comparison in there
            // This means that if the last statement is unassigned, then we'll delete that too, as
            // Absent is equal to unassigned. If it is an assign statement, then it will be filtered
            // out and won't be deleted.
            sqlu"""
            with recent_changes(domain_id, contract_key_hash, status, ts, request_counter) as (
              select domain_id, contract_key_hash, status, ts, request_counter from contract_key_journal
                  where domain_id = $domainId
                  and ts <= $beforeAndIncluding
                  and ts >= $lastPruningTs
          )
          delete from contract_key_journal as ckj
          using recent_changes as rc where
              ckj.domain_id = rc.domain_id and
              ckj.contract_key_hash = rc.contract_key_hash and
              (ckj.ts, ckj.request_counter, ckj.status) <= (rc.ts, rc.request_counter, CAST('unassigned' as key_status));
          """

          case _: DbStorage.Profile.Oracle =>
            sqlu"""
          delete from contract_key_journal
          where
            (domain_id, contract_key_hash, ts, request_counter) in (
              select domain_id, contract_key_hash, ts, request_counter
              from (
                  select domain_id, contract_key_hash, status, ts, request_counter,
                   ROW_NUMBER() OVER (partition by domain_id, contract_key_hash order by ts desc, request_counter desc) as row_num
                 from contract_key_journal
                 where domain_id = $domainId and ts <= $beforeAndIncluding) ordered_changes
              where ordered_changes.row_num > 1 or ordered_changes.status = ${ContractKeyJournal.Unassigned}
            )
          """
        }
        storage.queryAndUpdate(query, functionFullName)
      }

  override def deleteSince(
      inclusive: TimeOfChange
  )(implicit traceContext: TraceContext): EitherT[Future, ContractKeyJournalError, Unit] =
    processingTime.eitherTEvent {
      EitherT.right(
        storage.update_(
          sqlu"""delete from contract_key_journal
               where domain_id = $domainId and ts > ${inclusive.timestamp} or (ts = ${inclusive.timestamp} and request_counter >= ${inclusive.rc})""",
          functionFullName,
        )
      )
    }

  override def countUpdates(key: LfGlobalKey)(implicit traceContext: TraceContext): Future[Int] =
    processingTime.event {
      storage
        .query(
          sql"select count(*) from contract_key_journal where domain_id = $domainId and contract_key_hash = $key"
            .as[Int]
            .headOption,
          functionFullName,
        )
        .map(_.getOrElse(0))
    }

}

object DbContractKeyJournal {
  final case class DbContractKeyJournalError(err: Throwable) extends ContractKeyJournalError {
    override def asThrowable: Throwable = err
  }
}
