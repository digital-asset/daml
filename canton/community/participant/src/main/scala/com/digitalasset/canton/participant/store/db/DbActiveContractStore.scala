// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{Chain, EitherT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String100
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.data.ActiveContractsData
import com.digitalasset.canton.participant.store.db.DbActiveContractStore.*
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractChange,
  ContractStore,
  StateChangeType,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{
  LfContractId,
  SourceDomainId,
  TargetDomainId,
  TransferDomainId,
}
import com.digitalasset.canton.resource.DbStorage.Implicits.BuilderChain.{
  fromSQLActionBuilderChain,
  toSQLActionBuilderChain,
}
import com.digitalasset.canton.resource.DbStorage.*
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.{DbDeserializationException, DbPrunableByTimeDomain}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore, PrunableByTimeParameters}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, IterableUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{RequestCounter, TransferCounter, TransferCounterO}
import slick.jdbc.*
import slick.jdbc.canton.SQLActionBuilder

import scala.Ordered.orderingToOrdered
import scala.annotation.nowarn
import scala.collection.View
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

/** Active contracts journal
  *
  * This database table has the following indexes to support scaling query performance:
  * - CREATE index idx_par_active_contracts_dirty_request_reset ON par_active_contracts (domain_id, request_counter)
  * used on startup of the SyncDomain to delete all dirty requests.
  * - CREATE index idx_par_active_contracts_contract_id ON par_active_contracts (contract_id)
  * used in conflict detection for point wise lookup of the contract status.
  * - CREATE index idx_par_active_contracts_ts_domain_id ON par_active_contracts (ts, domain_id)
  * used on startup by the SyncDomain to replay ACS changes to the ACS commitment processor.
  */
class DbActiveContractStore(
    override protected val storage: DbStorage,
    protected[this] override val domainId: IndexedDomain,
    enableAdditionalConsistencyChecks: Boolean,
    maxContractIdSqlInListSize: PositiveNumeric[Int],
    batchingParametersConfig: PrunableByTimeParameters,
    indexedStringStore: IndexedStringStore,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ActiveContractStore
    with DbStore
    with DbPrunableByTimeDomain {

  import ActiveContractStore.*
  import DbStorage.Implicits.*
  import storage.api.*

  override protected def batchingParameters: Option[PrunableByTimeParameters] = Some(
    batchingParametersConfig
  )

  protected[this] override val pruning_status_table = "par_active_contract_pruning"

  /*
  Consider the scenario where a contract is created on domain D1, then transferred to D2, then to D3 and is finally archived.
  We will have the corresponding entries in the ActiveContractStore:
  - On D1, remoteDomain will initially be None and then Some(D2) (after the transfer-out)
  - On D2, remoteDomain will initially be Some(D1) and then Some(D3) (after the transfer-out)
  - On D3, remoteDomain will initially be Some(D2) and then None (after the archival).
   */
  private case class StoredActiveContract(
      change: ChangeType,
      timestamp: CantonTimestamp,
      rc: RequestCounter,
      remoteDomainIdIndex: Option[Int],
      transferCounter: TransferCounterO,
  ) {
    def toContractState(implicit ec: ExecutionContext): Future[ContractState] = {
      val statusF = change match {
        case ChangeType.Activation =>
          Future.successful(Active(transferCounter))

        case ChangeType.Deactivation =>
          // In case of a deactivation, then `remoteDomainIdIndex` is non-empty iff it is a transfer-out,
          // in which case the corresponding domain is the target domain.
          // The same holds for `remoteDomainIdF`.
          remoteDomainIdF.map {
            case Some(domainId) => TransferredAway(TargetDomainId(domainId), transferCounter)
            case None => Archived
          }
      }
      statusF.map(ContractState(_, rc, timestamp))
    }

    private def remoteDomainIdF: Future[Option[DomainId]] = {
      remoteDomainIdIndex.fold(Future.successful(None: Option[DomainId])) { index =>
        import TraceContext.Implicits.Empty.*
        IndexedDomain
          .fromDbIndexOT("par_active_contracts remote domain index", indexedStringStore)(index)
          .map(_.domainId)
          .value
      }
    }

    def toTransferCounterAtChangeInfo: TransferCounterAtChangeInfo =
      TransferCounterAtChangeInfo(TimeOfChange(rc, timestamp), transferCounter)
  }

  private implicit val getResultStoredActiveContract: GetResult[StoredActiveContract] =
    GetResult(r =>
      StoredActiveContract(
        ChangeType.getResultChangeType(r),
        GetResult[CantonTimestamp].apply(r),
        GetResult[RequestCounter].apply(r),
        r.nextIntOption(),
        GetResult[TransferCounterO].apply(r),
      )
    )

  def markContractsActive(contracts: Seq[(LfContractId, TransferCounterO)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    {
      for {
        activeContractsData <- CheckedT.fromEitherT(
          EitherT.fromEither[Future](
            ActiveContractsData
              .create(protocolVersion, toc, contracts)
              .leftMap(errorMessage => ActiveContractsDataInvariantViolation(errorMessage))
          )
        )
        _ <- bulkInsert(
          activeContractsData.asMap,
          ChangeType.Activation,
          remoteDomain = None,
        )
        _ <-
          if (enableAdditionalConsistencyChecks) {
            performUnlessClosingCheckedT(
              "additional-consistency-check",
              Checked.result[AcsError, AcsWarning, Unit](
                logger.debug(
                  "Could not perform additional consistency check because node is shutting down"
                )
              ),
            ) {
              activeContractsData.asSeq.parTraverse_ { tc =>
                for {
                  _ <- checkCreateArchiveAtUnique(
                    tc.contractId,
                    activeContractsData.toc,
                    ChangeType.Activation,
                  )
                  _ <- checkChangesBeforeCreation(tc.contractId, activeContractsData.toc)
                  _ <- checkTocAgainstEarliestArchival(tc.contractId, activeContractsData.toc)
                } yield ()
              }
            }
          } else {
            CheckedT.resultT[Future, AcsError, AcsWarning](())
          }
      } yield ()
    }
  }

  def archiveContracts(contracts: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    for {
      _ <- bulkInsert(
        contracts.map(cid => (cid, (None: TransferCounterO, toc))).toMap,
        ChangeType.Deactivation,
        remoteDomain = None,
      )
      _ <-
        if (enableAdditionalConsistencyChecks) {
          performUnlessClosingCheckedT(
            "additional-consistency-check",
            Checked.result[AcsError, AcsWarning, Unit](
              logger.debug(
                "Could not perform additional consistency check because node is shutting down"
              )
            ),
          ) {
            contracts.parTraverse_ { contractId =>
              for {
                _ <- checkCreateArchiveAtUnique(contractId, toc, ChangeType.Deactivation)
                _ <- checkChangesAfterArchival(contractId, toc)
                _ <- checkTocAgainstLatestCreation(contractId, toc)
              } yield ()
            }
          }
        } else {
          CheckedT.resultT[Future, AcsError, AcsWarning](())
        }
    } yield ()
  }

  private def indexedDomains(
      contractByDomain: Seq[
        (TransferDomainId, Seq[(LfContractId, (TransferCounterO, TimeOfChange))])
      ]
  ): CheckedT[Future, AcsError, AcsWarning, Seq[
    (IndexedDomain, Seq[(LfContractId, (TransferCounterO, TimeOfChange))])
  ]] =
    CheckedT.result(contractByDomain.parTraverse { case (domainId, contracts) =>
      IndexedDomain
        .indexed(indexedStringStore)(domainId.unwrap)
        .map(_ -> contracts)
    })

  override def transferInContracts(
      transferIns: Seq[(LfContractId, SourceDomainId, TransferCounterO, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val bySourceDomainIndexed =
      transferIns.groupMap { case (_, sourceDomain, _, _) => sourceDomain } {
        case (id, _, transferCounter, toc) => (id, (transferCounter, toc))
      }.toSeq
    for {
      indexedSourceDomain <- indexedDomains(bySourceDomainIndexed)
      _ <- indexedSourceDomain.parTraverse_ { case (sourceDomain, contracts) =>
        bulkInsert(
          contracts.toMap,
          ChangeType.Activation,
          remoteDomain = Some(sourceDomain),
        )
      }
      _ <- checkTransfersConsistency(
        transferIns.map { case (transfer, _, counter, toc) => (transfer, counter, toc) },
        OperationType.TransferIn,
      )
    } yield ()
  }

  override def transferOutContracts(
      transferOuts: Seq[(LfContractId, TargetDomainId, TransferCounterO, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val byTargetDomains =
      transferOuts.groupMap { case (_, targetDomain, _, _) => targetDomain } {
        case (id, _, transferCounter, toc) => (id, (transferCounter, toc))
      }.toSeq
    for {
      byTargetIndexed <- indexedDomains(byTargetDomains)
      _ <- byTargetIndexed.parTraverse_ { case (targetDomain, contracts) =>
        bulkInsert(
          contracts.toMap,
          ChangeType.Deactivation,
          remoteDomain = Some(targetDomain),
        )
      }
      _ <- checkTransfersConsistency(
        transferOuts.map { case (cid, _, counter, toc) => (cid, counter, toc) },
        OperationType.TransferOut,
      )
    } yield ()
  }

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Map[LfContractId, ContractState]] = {
    storage.profile match {
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Oracle =>
        // With H2, it is faster to do lookup contracts individually than to use a range query
        contractIds
          .to(LazyList)
          .parTraverseFilter { contractId =>
            storage
              .querySingle(fetchContractStateQuery(contractId), functionFullName)
              .semiflatMap(storedContract => {
                storedContract.toContractState.map(res => (contractId -> res))
              })
              .value
          }
          .map(_.toMap)
      case _: DbStorage.Profile.Postgres =>
        NonEmpty.from(contractIds.toSeq) match {
          case None => Future.successful(Map.empty)
          case Some(contractIdsNel) =>
            import DbStorage.Implicits.BuilderChain.*

            val queries =
              DbStorage
                .toInClauses_("contract_id", contractIdsNel, maxContractIdSqlInListSize)
                .map { inClause =>
                  val query =
                    sql"""
                with ordered_changes(contract_id, change, ts, request_counter, remote_domain_id, transfer_counter, row_num) as (
                  select contract_id, change, ts, request_counter, remote_domain_id, transfer_counter,
                     ROW_NUMBER() OVER (partition by domain_id, contract_id order by ts desc, request_counter desc, change asc)
                   from par_active_contracts
                   where domain_id = $domainId and """ ++ inClause ++
                      sql"""
                )
                select contract_id, change, ts, request_counter, remote_domain_id, transfer_counter
                from ordered_changes
                where row_num = 1;
                """

                  query.as[(LfContractId, StoredActiveContract)]
                }

            storage
              .sequentialQueryAndCombine(queries, functionFullName)
              .flatMap(_.toList.parTraverse { case (id, contract) =>
                contract.toContractState.map(cs => (id, cs))
              })
              .map(foundContracts => foundContracts.toMap)
        }

    }
  }

  override def packageUsage(
      pkg: PackageId,
      contractStore: ContractStore,
  )(implicit traceContext: TraceContext): Future[Option[(LfContractId)]] = {
    // The contractStore is unused
    // As we can directly query daml_contracts from the database

    import DbStorage.Implicits.*
    import DbStorage.Implicits.BuilderChain.*

    // TODO(i9480): Integrate with performance tests to check that we can remove packages when there are many contracts.

    val limitStatement: SQLActionBuilder = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sql"""fetch first 1 rows only"""
      case _ => sql"limit 1"
    }

    val query =
      (sql"""
                with ordered_changes(contract_id, package_id, change, ts, request_counter, remote_domain_id, row_num) as (
                  select par_active_contracts.contract_id, par_contracts.package_id, change, ts, par_active_contracts.request_counter, remote_domain_id,
                     ROW_NUMBER() OVER (
                     partition by par_active_contracts.domain_id, par_active_contracts.contract_id
                     order by
                        ts desc,
                        par_active_contracts.request_counter desc,
                        change asc
                     )
                   from par_active_contracts join par_contracts
                       on par_active_contracts.contract_id = par_contracts.contract_id
                              and par_active_contracts.domain_id = par_contracts.domain_id
                   where par_active_contracts.domain_id = $domainId
                    and par_contracts.package_id = $pkg
                )
                select contract_id, package_id
                from ordered_changes
                where row_num = 1
                and change = 'activation'
                """ ++ limitStatement).as[(LfContractId)]

    val queryResult = storage.query(query, functionFullName)
    queryResult.map(_.headOption)

  }

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, (CantonTimestamp, TransferCounterO)]] = {
    logger.debug(s"Obtaining ACS snapshot at $timestamp")
    storage
      .query(
        snapshotQuery(SnapshotQueryParameter.Ts(timestamp), None),
        functionFullName,
      )
      .map { snapshot =>
        SortedMap.from(snapshot.map { case (cid, ts, transferCounter) =>
          cid -> (ts, transferCounter)
        })
      }
  }

  override def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, (RequestCounter, TransferCounterO)]] = {
    logger.debug(s"Obtaining ACS snapshot at $rc")
    storage
      .query(
        snapshotQuery(SnapshotQueryParameter.Rc(rc), None),
        functionFullName,
      )
      .map { snapshot =>
        SortedMap.from(snapshot.map { case (cid, rc, transferCounter) =>
          cid -> (rc, transferCounter)
        })
      }
  }

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, CantonTimestamp]] = {
    {
      if (contractIds.isEmpty) Future.successful(Map.empty)
      else
        storage
          .query(
            snapshotQuery(SnapshotQueryParameter.Ts(timestamp), Some(contractIds)),
            functionFullName,
          )
          .map(_.view.map { case (cid, ts, _) => cid -> ts }.toMap)
    }
  }

  override def bulkContractsTransferCounterSnapshot(
      contractIds: Set[LfContractId],
      requestCounter: RequestCounter,
  )(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, TransferCounterO]] = {
    logger.debug(
      s"Looking up transfer counters for contracts $contractIds up to but not including $requestCounter"
    )
    if (requestCounter == RequestCounter.MinValue)
      ErrorUtil.internalError(
        new IllegalArgumentException(
          s"The request counter $requestCounter should not be equal to ${RequestCounter.MinValue}"
        )
      )
    if (contractIds.isEmpty) Future.successful(Map.empty)
    else {
      for {
        acsArchivalContracts <-
          storage
            .query(
              snapshotQuery(SnapshotQueryParameter.Rc(requestCounter - 1), Some(contractIds)),
              functionFullName,
            )
            .map { snapshot =>
              Map.from(snapshot.map { case (cid, _, transferCounter) =>
                cid -> transferCounter
              })
            }
      } yield {
        contractIds
          .map(k => (k, acsArchivalContracts.get(k).flatten))
          .toMap
      }
    }
  }

  private[this] def snapshotQuery[T](
      p: SnapshotQueryParameter[T],
      contractIds: Option[Set[LfContractId]],
  ): DbAction.ReadOnly[Seq[(LfContractId, T, TransferCounterO)]] = {
    import DbStorage.Implicits.BuilderChain.*

    val idsO = contractIds.map { ids =>
      sql"(" ++ ids.toList.map(id => sql"$id").intercalate(sql", ") ++ sql")"
    }

    implicit val getResultT: GetResult[T] = p.getResult

    // somehow, the compiler complains if it is not defined but indicates it as being unused
    @nowarn("cat=unused") implicit val setParameterT: SetParameter[T] = p.setParameter

    val ordering = sql" order by #${p.attribute} asc"

    storage.profile match {
      case _: DbStorage.Profile.H2 =>
        (sql"""
          select distinct(contract_id), #${p.attribute}, transfer_counter
          from par_active_contracts AC
          where not exists(select * from par_active_contracts AC2 where domain_id = $domainId and AC.contract_id = AC2.contract_id
            and AC2.#${p.attribute} <= ${p.bound}
            and ((AC.ts, AC.request_counter) < (AC2.ts, AC2.request_counter)
              or (AC.ts = AC2.ts and AC.request_counter = AC2.request_counter and AC2.change = ${ChangeType.Deactivation})))
           and AC.#${p.attribute} <= ${p.bound} and domain_id = $domainId""" ++
          idsO.fold(sql"")(ids => sql" and AC.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T, TransferCounterO)]
      case _: DbStorage.Profile.Postgres =>
        (sql"""
          select distinct(contract_id), AC3.#${p.attribute}, AC3.transfer_counter from par_active_contracts AC1
          join lateral
            (select #${p.attribute}, change, transfer_counter from par_active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and #${p.attribute} <= ${p.bound} order by ts desc, request_counter desc, change asc #${storage
            .limit(1)}) as AC3 on true
          where AC1.domain_id = $domainId and AC3.change = CAST(${ChangeType.Activation} as change_type)""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T, TransferCounterO)]
      case _: DbStorage.Profile.Oracle =>
        (sql"""select distinct(contract_id), AC3.#${p.attribute}, AC3.transfer_counter from par_active_contracts AC1, lateral
          (select #${p.attribute}, change, transfer_counter from par_active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and #${p.attribute} <= ${p.bound}
             order by ts desc, request_counter desc, change desc
             fetch first 1 row only) AC3
          where AC1.domain_id = $domainId and AC3.change = 'activation'""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T, TransferCounterO)]
    }
  }

  override def doPrune(beforeAndIncluding: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): Future[Int] = {
    // For each contract select the last deactivation before or at the timestamp.
    // If such a deactivation exists then delete all acs records up to and including the deactivation

    (for {
      nrPruned <-
        storage.profile match {
          case _: DbStorage.Profile.Postgres =>
            // On postgres running the single-sql-statement with both select/delete has resulted in Postgres
            // flakily hanging indefinitely on the ACS pruning select/delete. The only workaround that also still makes
            // use of the partial index "active_contracts_pruning_idx" appears to be splitting the select and delete
            // into separate statements. See #11292.
            for {
              acsEntriesToPrune <- performUnlessClosingF("Fetch ACS entries batch")(
                storage.query(
                  sql"""
                  with deactivation_counter(contract_id, request_counter) as (
                    select contract_id, max(request_counter)
                    from par_active_contracts
                    where domain_id = ${domainId}
                      and change = cast('deactivation' as change_type)
                      and ts <= ${beforeAndIncluding}
                    group by contract_id
                  )
                    select ac.contract_id, ac.ts, ac.request_counter, ac.change
                    from deactivation_counter dc
                      join par_active_contracts ac on ac.domain_id = ${domainId} and ac.contract_id = dc.contract_id
                    where ac.request_counter <= dc.request_counter"""
                    .as[(LfContractId, CantonTimestamp, RequestCounter, ChangeType)],
                  s"${functionFullName}: Fetch ACS entries to be pruned",
                )
              )
              totalEntriesPruned <-
                performUnlessClosingF("Delete ACS entries batch")(
                  if (acsEntriesToPrune.isEmpty) Future.successful(0)
                  else {
                    val deleteStatement =
                      s"delete from par_active_contracts where domain_id = ? and contract_id = ? and ts = ?"
                        + " and request_counter = ? and change = CAST(? as change_type);"
                    storage.queryAndUpdate(
                      DbStorage
                        .bulkOperation(deleteStatement, acsEntriesToPrune, storage.profile) { pp =>
                          { case (contractId, ts, rc, change) =>
                            pp >> domainId
                            pp >> contractId
                            pp >> ts
                            pp >> rc
                            pp >> change
                          }
                        }
                        .map(_.sum),
                      s"${functionFullName}: Bulk-delete ACS entries",
                    )
                  }
                )
            } yield totalEntriesPruned
          case _: DbStorage.Profile.H2 =>
            performUnlessClosingF("ACS.doPrune")(
              storage.queryAndUpdate(
                sqlu"""
            with deactivation_counter(contract_id, request_counter) as (
              select contract_id, max(request_counter)
              from par_active_contracts
              where domain_id = ${domainId}
              and change = ${ChangeType.Deactivation}
              and ts <= ${beforeAndIncluding}
              group by contract_id
            )
		    delete from par_active_contracts
            where (domain_id, contract_id, ts, request_counter, change) in (
		      select ac.domain_id, ac.contract_id, ac.ts, ac.request_counter, ac.change
              from deactivation_counter dc
              join par_active_contracts ac on ac.domain_id = ${domainId} and ac.contract_id = dc.contract_id
              where ac.request_counter <= dc.request_counter
            );
            """,
                functionFullName,
              )
            )
          case _: DbStorage.Profile.Oracle =>
            performUnlessClosingF("ACS.doPrune")(
              storage.queryAndUpdate(
                sqlu"""delete from par_active_contracts where rowid in (
            with deactivation_counter(contract_id, request_counter) as (
                select contract_id, max(request_counter)
                from par_active_contracts
                where domain_id = ${domainId}
                and change = 'deactivation'
                and ts <= ${beforeAndIncluding}
                group by contract_id
            )
            select ac.rowid
            from deactivation_counter dc
            join par_active_contracts ac on ac.domain_id = ${domainId} and ac.contract_id = dc.contract_id
            where ac.request_counter <= dc.request_counter
            )""",
                functionFullName,
              )
            )
        }
    } yield nrPruned).onShutdown(0)
  }

  def deleteSince(criterion: RequestCounter)(implicit traceContext: TraceContext): Future[Unit] = {
    val query =
      sqlu"delete from par_active_contracts where domain_id = $domainId and request_counter >= $criterion"
    storage
      .update(query, functionFullName)
      .map(count => logger.debug(s"DeleteSince on $criterion removed at least $count ACS entries"))
  }

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] = {
    ErrorUtil.requireArgument(
      fromExclusive <= toInclusive,
      s"Provided timestamps are in the wrong order: $fromExclusive and $toInclusive",
    )
    val changeQuery = {
      val changeOrder = storage.profile match {
        case _: DbStorage.Profile.Oracle => "asc"
        case _ => "desc"
      }
      sql"""select ts, request_counter, contract_id, change, transfer_counter, operation
             from par_active_contracts where domain_id = $domainId and
             ((ts = ${fromExclusive.timestamp} and request_counter > ${fromExclusive.rc}) or ts > ${fromExclusive.timestamp})
             and
             ((ts = ${toInclusive.timestamp} and request_counter <= ${toInclusive.rc}) or ts <= ${toInclusive.timestamp})
             order by ts asc, request_counter asc, change #$changeOrder"""
    }.as[
      (
          CantonTimestamp,
          RequestCounter,
          LfContractId,
          ChangeType,
          TransferCounterO,
          OperationType,
      )
    ]

    /* Computes the maximum transfer counter for each contract in the `res` vector, up to a certain request counter `rc`.
         The computation for max_transferCounter(`rc`, `cid`) reuses the result of max_transferCounter(`rc-1`, `cid`).

         Assumption: the input `res` is already sorted by request counter.
     */
    /*
         TODO(i12904): Here we compute the maximum of the previous transfer counters;
          instead, we could retrieve the transfer counter of the latest activation
     */

    def transferCounterForArchivals(
        res: Iterable[
          (
              CantonTimestamp,
              RequestCounter,
              LfContractId,
              ChangeType,
              TransferCounterO,
              OperationType,
          )
        ]
    ): Map[(RequestCounter, LfContractId), TransferCounterO] = {
      val groupedByCid = res.groupBy { case (_, _, cid, _, _, _) => cid }
      val maxTransferCountersPerCidUpToRc = groupedByCid
        .flatMap { case (cid, changes) =>
          val sortedChangesByRc = changes.collect {
            case (_, rc, _, _, transferCounter, opType) if opType != OperationType.TransferOut =>
              ((rc, cid), (transferCounter, opType))
          }.toList

          NonEmpty.from(sortedChangesByRc) match {
            case None => List.empty
            case Some(changes) =>
              changes.tail1.scanLeft(changes.head1) {
                case (
                      ((_, _), (accTransferCounter, _)),
                      ((crtRc, cid), (crtTransferCounter, opType)),
                    ) =>
                  (
                    (crtRc, cid),
                    (
                      Ordering[TransferCounterO].max(accTransferCounter, crtTransferCounter),
                      opType,
                    ),
                  )
              }
          }
        }
        .collect {
          case ((rc, cid), (transferCounter, opType)) if opType == OperationType.Archive =>
            ((rc, cid), transferCounter)
        }

      maxTransferCountersPerCidUpToRc
    }

    for {
      retrievedChangesBetween <- storage.query(
        changeQuery,
        operationName = "ACS: get changes between",
      )
      // retrieves the transfer counters for archived contracts that were activated between (`fromExclusive`, `toInclusive`]
      maxTransferCountersPerCidUpToRc = transferCounterForArchivals(retrievedChangesBetween)

      /*
         If there are contracts archived between (`fromExclusive`, `toInclusive`] that have a
         transfer counter None in maxTransferCountersPerCidUpToRc, and the protocol version
         supports transfer counters, then we need to retrieve the transfer counters of these
         archived contracts from activations taking place at time <= toInclusive.
       */
      // retrieves the transfer counters for archived contracts that were activated at time <= `fromExclusive`
      maxTransferCountersPerRemainingCidUpToRc <- {
        val archivalsWithoutTransferCounters =
          maxTransferCountersPerCidUpToRc.filter(_._2.isEmpty)
        NonEmpty
          .from(archivalsWithoutTransferCounters.map { case ((_, contractId), _) =>
            contractId
          }.toSeq)
          .fold(
            Future.successful(Map.empty[(RequestCounter, LfContractId), TransferCounterO])
          ) { cids =>
            val maximumRc =
              archivalsWithoutTransferCounters
                .map { case ((rc, _), _) => rc.unwrap }
                .maxOption
                .getOrElse(RequestCounter.Genesis.unwrap)
            val archivalCidsWithoutTransferCountersQueries = DbStorage
              .toInClauses_("contract_id", cids, maxContractIdSqlInListSize)(
                absCoidSetParameter
              )
              // Note that the sql query does not filter entries with ts <= toExclusive.timestamp,
              // but it also includes the entries between (`fromExclusive`, `toInclusive`].
              // This is an implementation choice purely to reuse code: we pass the query result into the
              // function `transferCounterForArchivals` and obtain the transfer counters for (rc, cid) pairs.
              // One could have a more restrictive query and compute the transfer counters in some other way.
              .map { inClause =>
                (sql"""select ts, request_counter, contract_id, change, transfer_counter, operation
                   from par_active_contracts where domain_id = $domainId
                   and (request_counter <= $maximumRc)
                   and (ts <= ${toInclusive.timestamp})
                   and """ ++ inClause ++ sql""" order by ts asc, request_counter asc""")
                  .as[
                    (
                        CantonTimestamp,
                        RequestCounter,
                        LfContractId,
                        ChangeType,
                        TransferCounterO,
                        OperationType,
                    )
                  ]
              }
            val resultArchivalTransferCounters = storage
              .sequentialQueryAndCombine(
                archivalCidsWithoutTransferCountersQueries,
                "ACS: get data to compute the transfer counters for archived contracts",
              )

            resultArchivalTransferCounters.map { r =>
              transferCounterForArchivals(r)
            }
          }
      }
    } yield {
      // filter None entries from maxTransferCountersPerCidUpToRc, as the transfer counters for
      // those contracts are now in remainingMaxTransferCountersPerCidUpToRc
      val definedMaxTransferCountersPerCidUpToRc = maxTransferCountersPerCidUpToRc.filter {
        case ((_, _), transferCounter) => transferCounter.isDefined
      }

      val groupedByTs =
        IterableUtil.spansBy(retrievedChangesBetween)(entry => (entry._1, entry._2))
      groupedByTs.map { case ((ts, rc), changes) =>
        val (acts, deacts) = changes.partition { case (_, _, _, changeType, _, _) =>
          changeType == ChangeType.Activation
        }
        TimeOfChange(rc, ts) -> ActiveContractIdsChange(
          acts.map { case (_, _, cid, _, transferCounter, opType) =>
            if (opType == OperationType.Create)
              (cid, StateChangeType(ContractChange.Created, transferCounter))
            else
              (cid, StateChangeType(ContractChange.Assigned, transferCounter))
          }.toMap,
          deacts.map { case (_, requestCounter, cid, _, transferCounter, opType) =>
            if (opType == OperationType.Archive) {
              (
                cid,
                StateChangeType(
                  ContractChange.Archived,
                  definedMaxTransferCountersPerCidUpToRc.getOrElse(
                    (requestCounter, cid),
                    maxTransferCountersPerRemainingCidUpToRc
                      .getOrElse((requestCounter, cid), transferCounter),
                  ),
                ),
              )
            } else (cid, StateChangeType(ContractChange.Unassigned, transferCounter))
          }.toMap,
        )
      }
    }
  }

  override private[participant] def contractCount(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] = {
    storage.query(
      sql"select count(distinct contract_id) from par_active_contracts where ts <= $timestamp"
        .as[Int]
        .head,
      functionFullName,
    )
  }

  private def checkTransfersConsistency(
      transfers: Seq[(LfContractId, TransferCounterO, TimeOfChange)],
      operation: TransferOperationType,
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] =
    if (enableAdditionalConsistencyChecks) {
      transfers.parTraverse_ { case (contractId, transferCounter, toc) =>
        for {
          _ <- checkTocAgainstLatestCreation(contractId, toc)
          _ <- transferCounter
            .traverse_(tc => checkTransferCountersShouldIncrease(contractId, toc, tc, operation))
          _ <- checkTocAgainstEarliestArchival(contractId, toc)
        } yield ()
      }
    } else CheckedT.pure(())

  private def checkChangesBeforeCreation(contractId: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val query =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sql"""select ts, request_counter from par_active_contracts
              where domain_id = $domainId and contract_id = $contractId
                and (ts < ${toc.timestamp} or (ts = ${toc.timestamp} and request_counter < ${toc.rc})) and operation != ${OperationType.Create}
              order by ts desc, request_counter desc, change asc"""
        case _ =>
          sql"""select ts, request_counter from par_active_contracts
              where domain_id = $domainId and contract_id = $contractId
                and (ts, request_counter) < (${toc.timestamp}, ${toc.rc}) and operation != CAST(${OperationType.Create} as operation_type)
              order by (ts, request_counter, change) desc"""
      }

    val result = storage.query(query.as[(CantonTimestamp, RequestCounter)], functionFullName)

    CheckedT(result.map { changes =>
      val warnings = changes.map { case (changeTs, changeRc) =>
        ChangeBeforeCreation(contractId, toc, TimeOfChange(changeRc, changeTs))
      }
      Checked.unit.appendNonaborts(Chain.fromSeq(warnings))
    })
  }

  private def checkChangesAfterArchival(contractId: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {

    val q =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sql"""select ts, request_counter from par_active_contracts
              where domain_id = $domainId and contract_id = $contractId
                and (ts > ${toc.timestamp} or (ts = ${toc.timestamp} and request_counter > ${toc.rc})) and operation != ${OperationType.Archive}
              order by ts desc, request_counter desc, change asc"""

        case _ =>
          sql"""select ts, request_counter from par_active_contracts
              where domain_id = $domainId and contract_id = $contractId
                and (ts, request_counter) > (${toc.timestamp}, ${toc.rc}) and operation != CAST(${OperationType.Archive} as operation_type)
              order by (ts, request_counter, change) desc"""
      }

    val result = storage.query(q.as[(CantonTimestamp, RequestCounter)], functionFullName)

    CheckedT(result.map { changes =>
      val warnings = changes.map { case (changeTs, changeRc) =>
        ChangeAfterArchival(contractId, toc, TimeOfChange(changeRc, changeTs))
      }
      Checked.unit.appendNonaborts(Chain.fromSeq(warnings))
    })
  }

  private def checkCreateArchiveAtUnique(
      contractId: LfContractId,
      toc: TimeOfChange,
      change: ChangeType,
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val operation = change match {
      case ChangeType.Activation => OperationType.Create
      case ChangeType.Deactivation => OperationType.Archive
    }
    val order = change match {
      case ChangeType.Activation => "desc" // find the latest creation
      case ChangeType.Deactivation => "asc" // find the earliest archival
    }
    val q =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sql"""
        select ts, request_counter from par_active_contracts
        where domain_id = $domainId and contract_id = $contractId
          and (ts <> ${toc.timestamp} or request_counter <> ${toc.rc})
          and change = $change
          and operation = $operation
        order by ts #$order, request_counter #$order
        #${storage.limit(1)}
         """

        case _ =>
          sql"""
        select ts, request_counter from par_active_contracts
        where domain_id = $domainId and contract_id = $contractId
          and (ts, request_counter) <> (${toc.timestamp}, ${toc.rc})
          and change = CAST($change as change_type)
          and operation = CAST($operation as operation_type)
        order by (ts, request_counter) #$order
        #${storage.limit(1)}
         """

      }
    val query = q.as[(CantonTimestamp, RequestCounter)]
    CheckedT(storage.query(query, functionFullName).map { changes =>
      changes.headOption.fold(Checked.unit[AcsError, AcsWarning]) { case (changeTs, changeRc) =>
        val warn =
          if (change == ChangeType.Activation)
            DoubleContractCreation(contractId, TimeOfChange(changeRc, changeTs), toc)
          else DoubleContractArchival(contractId, TimeOfChange(changeRc, changeTs), toc)
        Checked.continue(warn)
      }
    })
  }

  /** Check that the given [[com.digitalasset.canton.participant.util.TimeOfChange]]
    * is not before the latest creation. Otherwise return a [[ChangeBeforeCreation]].
    */
  private def checkTocAgainstLatestCreation(contractId: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(storage.query(fetchLatestCreation(contractId), functionFullName).map {
      case None => Checked.unit
      case Some(StoredActiveContract(_, ts, rc, _, _)) =>
        val storedToc = TimeOfChange(rc, ts)
        if (storedToc > toc) Checked.continue(ChangeBeforeCreation(contractId, storedToc, toc))
        else Checked.unit
    })

  private def checkTransferCountersShouldIncrease(
      contractId: LfContractId,
      toc: TimeOfChange,
      transferCounter: TransferCounter,
      transferType: TransferOperationType,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = CheckedT {

    // latestBefore and earliestAfter are only the latest/earliest w.r.t. what has already been persisted.
    // Given the out-of-order writes to the ActiveContractStore, there may actually be pending writes of changes
    // between "latest/earliest" and the current time of change. Therefore, we check only for monotonicity
    // instead of gap-freedom and we do not mention earliest/latest in the error messages either.
    //
    // By checking both "latest" before / "earliest" after, we cover the case that an out-of-order write of
    // an earlier change has a higher transfer counter that the current time of change: the "earliest" after
    // check will then fail on the earlier time of change.
    for {
      latestBeforeO <- storage.query(
        fetchLatestContractStateBefore(contractId, toc),
        s"$functionFullName-before",
      )
      earliestAfterO <- storage.query(
        fetchEarliestContractStateAfter(contractId, toc),
        s"$functionFullName-after",
      )
    } yield {
      for {
        _ <- ActiveContractStore.checkTransferCounterAgainstLatestBefore(
          contractId,
          toc,
          transferCounter,
          latestBeforeO.map(_.toTransferCounterAtChangeInfo),
          transferType.toTransferType,
        )
        _ <- ActiveContractStore.checkTransferCounterAgainstEarliestAfter(
          contractId,
          toc,
          transferCounter,
          earliestAfterO.map(_.toTransferCounterAtChangeInfo),
          transferType.toTransferType,
        )
      } yield ()
    }
  }

  /** Check that the given [[com.digitalasset.canton.participant.util.TimeOfChange]]
    * is not after the earliest archival. Otherwise return a [[ChangeAfterArchival]].
    */
  private def checkTocAgainstEarliestArchival(contractId: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(storage.query(fetchEarliestArchival(contractId), functionFullName).map {
      case None => Checked.unit
      case Some(StoredActiveContract(_, ts, rc, _, _)) =>
        val storedToc = TimeOfChange(rc, ts)
        if (storedToc < toc) Checked.continue(ChangeAfterArchival(contractId, storedToc, toc))
        else Checked.unit
    })

  private def bulkInsert(
      contractIdsWithTransferCounter: Map[LfContractId, (TransferCounterO, TimeOfChange)],
      change: ChangeType,
      remoteDomain: Option[IndexedDomain],
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val operation = change match {
      case ChangeType.Activation =>
        if (remoteDomain.isEmpty) OperationType.Create else OperationType.TransferIn
      case ChangeType.Deactivation =>
        if (remoteDomain.isEmpty) OperationType.Archive else OperationType.TransferOut
    }

    val insertQuery = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( par_active_contracts ( contract_id, ts, request_counter, change, domain_id, transfer_counter ) ) */
          |into par_active_contracts
          |using (select ? contract_id, ? ts, ? request_counter, ? change, ? domain_id from dual) input
          |on (par_active_contracts.contract_id = input.contract_id and par_active_contracts.ts = input.ts and
          |    par_active_contracts.request_counter = input.request_counter and par_active_contracts.change = input.change and
          |    par_active_contracts.domain_id = input.domain_id)
          |when not matched then
          |  insert (contract_id, ts, request_counter, change, domain_id, operation, remote_domain_id, transfer_counter)
          |  values (input.contract_id, input.ts, input.request_counter, input.change, input.domain_id, ?, ?, ?)""".stripMargin
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into par_active_contracts(contract_id, ts, request_counter, change, domain_id, operation, remote_domain_id, transfer_counter)
          values (?, ?, ?, CAST(? as change_type), ?, CAST(? as operation_type), ?, ?)
          on conflict do nothing"""
    }
    val insertAll =
      DbStorage.bulkOperation_(insertQuery, contractIdsWithTransferCounter, storage.profile) {
        pp => contractIdWithTransferCounter =>
          val (contractId, (transferCounter, toc)) = contractIdWithTransferCounter
          pp >> contractId
          pp >> toc.timestamp
          pp >> toc.rc
          pp >> change
          pp >> domainId
          pp >> operation
          pp >> remoteDomain
          pp >> transferCounter
      }

    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    def unzip(toCheck: NonEmpty[Seq[(LfContractId, TimeOfChange)]]): (
        NonEmpty[Seq[LfContractId]],
        NonEmpty[Seq[RequestCounter]],
        NonEmpty[Seq[CantonTimestamp]],
    ) = {
      val cids: Seq[LfContractId] =
        new View.Map[(LfContractId, TimeOfChange), LfContractId](toCheck, _._1).toSeq
      val rcs: Seq[RequestCounter] =
        new View.Map[(LfContractId, TimeOfChange), RequestCounter](toCheck, _._2.rc).toSeq
      val tss: Seq[CantonTimestamp] =
        new View.Map[(LfContractId, TimeOfChange), CantonTimestamp](toCheck, _._2.timestamp).toSeq
      (NonEmpty.from(cids).get, NonEmpty.from(rcs).get, NonEmpty.from(tss).get)
    }

    // This method relies on calls to bulkInsert inserting contract IDs exactly once
    // for a given request counter and the request counter having a unique timestamp
    def checkIdempotence(
        toCheck: NonEmpty[Seq[(LfContractId, TimeOfChange)]]
    ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
      import DbStorage.Implicits.BuilderChain.*

      val (idsToCheck, rcsToCheck, tssToCheck) = unzip(toCheck)

      val contractIdsNotInsertedInClauses =
        DbStorage.toInClauses_("contract_id", idsToCheck, maxContractIdSqlInListSize)

      val rcsInClauses =
        DbStorage.toInClauses_("request_counter", rcsToCheck, maxContractIdSqlInListSize)

      val tssInClauses =
        DbStorage.toInClauses_("ts", tssToCheck, maxContractIdSqlInListSize)

      def query(
          cidsInClause: SQLActionBuilderChain,
          rcsInClause: SQLActionBuilderChain,
          tssInClause: SQLActionBuilderChain,
      ) = storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sql"select contract_id, remote_domain_id, transfer_counter, request_counter, ts from par_active_contracts where domain_id = $domainId and " ++ cidsInClause ++
            sql" and " ++ tssInClause ++ sql" and " ++ rcsInClause ++ sql" and change = $change and (operation <> $operation or " ++
            (if (remoteDomain.isEmpty) sql"remote_domain_id is not null"
             else sql"remote_domain_id <> $remoteDomain") ++ sql")"
        case _ =>
          sql"select contract_id, remote_domain_id, transfer_counter, request_counter, ts from par_active_contracts where domain_id = $domainId and " ++ cidsInClause ++
            sql" and " ++ tssInClause ++ sql" and " ++ rcsInClause ++ sql" and change = CAST($change as change_type) and (operation <> CAST($operation as operation_type) or " ++
            (if (remoteDomain.isEmpty) sql"remote_domain_id is not null"
             else sql"remote_domain_id <> $remoteDomain") ++ sql")"
      }

      val queries =
        contractIdsNotInsertedInClauses.zip(rcsInClauses).zip(tssInClauses).map {
          case ((cidsInClause, rcsInClause), tssInClause) =>
            query(cidsInClause, rcsInClause, tssInClause)
              .as[(LfContractId, Option[Int], TransferCounterO, RequestCounter, CantonTimestamp)]
        }
      val results = storage
        .sequentialQueryAndCombine(queries, functionFullName)
        .flatMap(_.toList.parTraverseFilter {
          case (cid, Some(remoteIdx), transferCounter, rc, ts) =>
            IndexedDomain
              .fromDbIndexOT("active_contracts", indexedStringStore)(remoteIdx)
              .map { indexed =>
                (cid, TransferDetails(indexed.item, transferCounter), TimeOfChange(rc, ts))
              }
              .value
          case (cid, None, transferCounter, rc, ts) =>
            Future.successful(
              Some((cid, CreationArchivalDetail(transferCounter), TimeOfChange(rc, ts)))
            )
        })

      CheckedT(results.map { presentWithOtherValues =>
        val isActivation = change == ChangeType.Activation
        presentWithOtherValues.traverse_ { case (contractId, previousDetail, toc) =>
          val transferCounter = contractIdsWithTransferCounter.get(contractId).flatMap(_._1)
          val detail = ActivenessChangeDetail(remoteDomain.map(_.item), transferCounter)
          val warn =
            if (isActivation)
              SimultaneousActivation(
                contractId,
                toc,
                previousDetail,
                detail,
              )
            else
              SimultaneousDeactivation(
                contractId,
                toc,
                previousDetail,
                detail,
              )
          Checked.continue(warn)
        }
      })
    }

    CheckedT.result(storage.queryAndUpdate(insertAll, functionFullName)).flatMap { (_: Unit) =>
      if (enableAdditionalConsistencyChecks) {
        // Check all contracts whether they have been inserted or are already there
        // We don't analyze the update counts
        // so that we can use the fast IGNORE_ROW_ON_DUPKEY_INDEX directive in Oracle
        NonEmpty
          .from(contractIdsWithTransferCounter.view.map { case (cid, (_, toc)) =>
            (cid, toc)
          }.toSeq)
          .map(checkIdempotence)
          .getOrElse(CheckedT.pure(()))
      } else CheckedT.pure(())
    }
  }

  private def fetchLatestCreation(
      contractId: LfContractId
  ): DbAction.ReadOnly[Option[StoredActiveContract]] =
    fetchContractStateQuery(contractId, Some(OperationType.Create))

  private def fetchEarliestArchival(
      contractId: LfContractId
  ): DbAction.ReadOnly[Option[StoredActiveContract]] =
    fetchContractStateQuery(contractId, Some(OperationType.Archive), descending = false)

  private def fetchEarliestContractStateAfter(
      contractId: LfContractId,
      toc: TimeOfChange,
  ): DbAction.ReadOnly[Option[StoredActiveContract]] =
    fetchContractStateQuery(
      contractId,
      operationFilter = None,
      tocFilter = Some(toc),
      descending = false,
    )

  private def fetchLatestContractStateBefore(
      contractId: LfContractId,
      toc: TimeOfChange,
  ): DbAction.ReadOnly[Option[StoredActiveContract]] =
    fetchContractStateQuery(contractId, operationFilter = None, tocFilter = Some(toc))

  private def fetchContractStateQuery(
      contractId: LfContractId,
      operationFilter: Option[OperationType] = None,
      tocFilter: Option[TimeOfChange] = None,
      descending: Boolean = true,
  ): DbAction.ReadOnly[Option[StoredActiveContract]] = {

    import DbStorage.Implicits.BuilderChain.*

    val baseQuery =
      sql"""select change, ts, request_counter, remote_domain_id, transfer_counter from par_active_contracts
                          where domain_id = $domainId and contract_id = $contractId"""
    val opFilterQuery =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          operationFilter.fold(sql" ")(o => sql" and operation = $o")
        case _ =>
          operationFilter.fold(sql" ")(o => sql" and operation = CAST($o as operation_type)")
      }
    val tocFilterQuery = tocFilter.fold(sql" ") { toc =>
      if (descending)
        sql" and (ts < ${toc.timestamp} or (ts = ${toc.timestamp} and request_counter < ${toc.rc}))"
      else
        sql" and (ts > ${toc.timestamp} or (ts = ${toc.timestamp} and request_counter > ${toc.rc}))"
    }
    val (normal_order, reversed_order) = if (descending) ("desc", "asc") else ("asc", "desc")
    val orderQuery = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sql" order by ts #$normal_order, request_counter #$normal_order, change #$normal_order #${storage
            .limit(1)}"
      case _ =>
        sql" order by ts #$normal_order, request_counter #$normal_order, change #$reversed_order #${storage
            .limit(1)}"
    }
    val query = baseQuery ++ opFilterQuery ++ tocFilterQuery ++ orderQuery
    query.as[StoredActiveContract].headOption
  }
}

private object DbActiveContractStore {
  sealed trait SnapshotQueryParameter[T] {
    def attribute: String

    def bound: T

    def getResult: GetResult[T]

    def setParameter: SetParameter[T]
  }

  object SnapshotQueryParameter {
    final case class Ts(bound: CantonTimestamp) extends SnapshotQueryParameter[CantonTimestamp] {
      val attribute = "ts"
      val getResult: GetResult[CantonTimestamp] = implicitly[GetResult[CantonTimestamp]]
      val setParameter: SetParameter[CantonTimestamp] = implicitly[SetParameter[CantonTimestamp]]
    }

    final case class Rc(bound: RequestCounter) extends SnapshotQueryParameter[RequestCounter] {
      val attribute = "request_counter"
      val getResult: GetResult[RequestCounter] = implicitly[GetResult[RequestCounter]]
      val setParameter: SetParameter[RequestCounter] = implicitly[SetParameter[RequestCounter]]
    }
  }

  sealed trait ChangeType {
    def name: String

    // lazy val so that `kind` is initialized first in the subclasses
    final lazy val toDbPrimitive: String100 =
      // The Oracle DB schema allows up to 100 chars; Postgres, H2 map this to an enum
      String100.tryCreate(name)
  }

  object ChangeType {
    case object Activation extends ChangeType {
      override val name = "activation"
    }

    case object Deactivation extends ChangeType {
      override val name = "deactivation"
    }

    implicit val setParameterChangeType: SetParameter[ChangeType] = (v, pp) => pp >> v.toDbPrimitive
    implicit val getResultChangeType: GetResult[ChangeType] = GetResult(r =>
      r.nextString() match {
        case ChangeType.Activation.name => ChangeType.Activation
        case ChangeType.Deactivation.name => ChangeType.Deactivation
        case unknown => throw new DbDeserializationException(s"Unknown change type [$unknown]")
      }
    )
  }

  sealed trait OperationType extends Product with Serializable {
    val name: String

    // lazy val so that `kind` is initialized first in the subclasses
    final lazy val toDbPrimitive: String100 =
      // The Oracle DB schema allows up to 100 chars; Postgres, H2 map this to an enum
      String100.tryCreate(name)
  }

  sealed trait TransferOperationType extends OperationType {
    def toTransferType: ActiveContractStore.TransferType
  }

  object OperationType {
    case object Create extends OperationType {
      override val name = "create"
    }

    case object Archive extends OperationType {
      override val name = "archive"
    }

    case object TransferIn extends TransferOperationType {
      override val name = "transfer-in"

      override def toTransferType: ActiveContractStore.TransferType =
        ActiveContractStore.TransferType.TransferIn
    }

    case object TransferOut extends TransferOperationType {
      override val name = "transfer-out"

      override def toTransferType: ActiveContractStore.TransferType =
        ActiveContractStore.TransferType.TransferOut
    }

    implicit val setParameterOperationType: SetParameter[OperationType] = (v, pp) =>
      pp >> v.toDbPrimitive
    implicit val getResultChangeType: GetResult[OperationType] = GetResult(r =>
      r.nextString() match {
        case OperationType.Create.name => OperationType.Create
        case OperationType.Archive.name => OperationType.Archive
        case OperationType.TransferIn.name => OperationType.TransferIn
        case OperationType.TransferOut.name => OperationType.TransferOut
        case unknown => throw new DbDeserializationException(s"Unknown operation type [$unknown]")
      }
    )
  }

}
