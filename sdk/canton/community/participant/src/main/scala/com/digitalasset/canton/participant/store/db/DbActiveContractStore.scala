// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, NonEmptyChain}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String100}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail.*
import com.digitalasset.canton.participant.store.data.ActiveContractsData
import com.digitalasset.canton.participant.store.db.DbActiveContractStore.*
import com.digitalasset.canton.participant.store.{
  ActivationsDeactivationsConsistencyCheck,
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
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, IterableUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{RequestCounter, TransferCounter}
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
  * used on startup of the SyncDomain to delete all inflight validation requests.
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
    val indexedStringStore: IndexedStringStore,
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

  private def checkedTUnit: CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT.resultT[Future, AcsError, AcsWarning](())

  /*
  Consider the scenario where a contract is created on domain D1, then transferred to D2, then to D3 and is finally archived.
  We will have the corresponding entries in the ActiveContractStore:
  - On D1, remoteDomain will initially be None and then Some(D2) (after the transfer-out)
  - On D2, remoteDomain will initially be Some(D1) and then Some(D3) (after the transfer-out)
  - On D3, remoteDomain will initially be Some(D2) and then None (after the archival).
   */
  private case class StoredActiveContract(
      activenessChange: ActivenessChangeDetail,
      toc: TimeOfChange,
  ) {
    def toContractState(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): Future[ContractState] = {
      val statusF = activenessChange match {
        case Create(transferCounter) => Future.successful(Active(transferCounter))
        case Archive => Future.successful(Archived)
        case Add(transferCounter) => Future.successful(Active(transferCounter))
        case Purge => Future.successful(Purged)
        case in: TransferIn => Future.successful(Active(in.transferCounter))
        case out: TransferOut =>
          domainIdFromIdx(out.remoteDomainIdx).map(id =>
            TransferredAway(TargetDomainId(id), out.transferCounter)
          )
      }

      statusF.map(ContractState(_, toc.rc, toc.timestamp))
    }

    def toTransferCounterAtChangeInfo: TransferCounterAtChangeInfo =
      TransferCounterAtChangeInfo(toc, activenessChange.transferCounterO)
  }

  private implicit val getResultStoredActiveContract: GetResult[StoredActiveContract] =
    GetResult { r =>
      val activenessChange = GetResult[ActivenessChangeDetail].apply(r)
      val ts = GetResult[CantonTimestamp].apply(r)
      val rc = GetResult[RequestCounter].apply(r)

      StoredActiveContract(activenessChange, TimeOfChange(rc, ts))
    }

  override def markContractsCreatedOrAdded(
      contracts: Seq[(LfContractId, TransferCounter)],
      toc: TimeOfChange,
      isCreation: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val (operationName, builder) =
      if (isCreation) (ActivenessChangeDetail.create, ActivenessChangeDetail.Create(_))
      else (ActivenessChangeDetail.add, ActivenessChangeDetail.Add(_))

    for {
      activeContractsData <- CheckedT.fromEitherT(
        EitherT.fromEither[Future](
          ActiveContractsData
            .create(protocolVersion, toc, contracts)
            .leftMap(errorMessage => ActiveContractsDataInvariantViolation(errorMessage))
        )
      )
      _ <- bulkInsert(
        activeContractsData.asMap.fmap(builder),
        change = ChangeType.Activation,
        operationName = operationName,
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
              checkActivationsDeactivationConsistency(
                tc.contractId,
                activeContractsData.toc,
              )
            }
          }
        } else checkedTUnit
    } yield ()
  }

  override def purgeOrArchiveContracts(
      contracts: Seq[LfContractId],
      toc: TimeOfChange,
      isArchival: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val (operationName, operation) =
      if (isArchival) (ActivenessChangeDetail.archive, ActivenessChangeDetail.Archive)
      else (ActivenessChangeDetail.purge, ActivenessChangeDetail.Purge)

    for {
      _ <- bulkInsert(
        contracts.map(cid => ((cid, toc), operation)).toMap,
        change = ChangeType.Deactivation,
        operationName = operationName,
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
                _ <- checkActivationsDeactivationConsistency(contractId, toc)
              } yield ()
            }
          }
        } else checkedTUnit
    } yield ()
  }

  private def transferContracts(
      transfers: Seq[(LfContractId, TransferDomainId, TransferCounter, TimeOfChange)],
      builder: (TransferCounter, Int) => TransferChangeDetail,
      change: ChangeType,
      operationName: LengthLimitedString,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val domains = transfers.map { case (_, domain, _, _) => domain.unwrap }.distinct

    type PreparedTransfer = ((LfContractId, TimeOfChange), TransferChangeDetail)

    for {
      domainIndices <- getDomainIndices(domains)

      preparedTransfersE = MonadUtil.sequentialTraverse(
        transfers
      ) { case (cid, remoteDomain, transferCounter, toc) =>
        domainIndices
          .get(remoteDomain.unwrap)
          .toRight[AcsError](UnableToFindIndex(remoteDomain.unwrap))
          .map(idx => ((cid, toc), builder(transferCounter, idx.index)))
      }

      preparedTransfers <- CheckedT.fromChecked(Checked.fromEither(preparedTransfersE)): CheckedT[
        Future,
        AcsError,
        AcsWarning,
        Seq[PreparedTransfer],
      ]

      _ <- bulkInsert(
        preparedTransfers.toMap,
        change,
        operationName = operationName,
      )

      _ <- checkTransfersConsistency(preparedTransfers)
    } yield ()
  }

  override def transferInContracts(
      transferIns: Seq[(LfContractId, SourceDomainId, TransferCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    transferContracts(
      transferIns,
      TransferIn.apply,
      ChangeType.Activation,
      ActivenessChangeDetail.transferIn,
    )

  override def transferOutContracts(
      transferOuts: Seq[(LfContractId, TargetDomainId, TransferCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = transferContracts(
    transferOuts,
    TransferOut.apply,
    ChangeType.Deactivation,
    ActivenessChangeDetail.transferOut,
  )

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
              .semiflatMap(_.toContractState.map(res => (contractId -> res)))
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
                with ordered_changes(contract_id, operation, transfer_counter, remote_domain_idx, ts, request_counter, row_num) as (
                  select contract_id, operation, transfer_counter, remote_domain_idx, ts, request_counter,
                     ROW_NUMBER() OVER (partition by domain_id, contract_id order by ts desc, request_counter desc, change asc)
                   from par_active_contracts
                   where domain_id = $domainId and """ ++ inClause ++
                      sql"""
                )
                select contract_id, operation, transfer_counter, remote_domain_idx, ts, request_counter
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
                with ordered_changes(contract_id, package_id, change, ts, request_counter, remote_domain_idx, row_num) as (
                  select par_active_contracts.contract_id, par_contracts.package_id, change, ts, par_active_contracts.request_counter, remote_domain_idx,
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
  ): Future[SortedMap[LfContractId, (CantonTimestamp, TransferCounter)]] = {
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
  ): Future[SortedMap[LfContractId, (RequestCounter, TransferCounter)]] = {
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
  ): Future[Map[LfContractId, TransferCounter]] = {
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
          .diff(acsArchivalContracts.keySet)
          .foreach(cid =>
            ErrorUtil.internalError(
              new IllegalStateException(
                s"Archived non-transient contract $cid should have been active in the ACS and have a transfer counter defined"
              )
            )
          )
        acsArchivalContracts
      }
    }
  }

  private[this] def snapshotQuery[T](
      p: SnapshotQueryParameter[T],
      contractIds: Option[Set[LfContractId]],
  ): DbAction.ReadOnly[Seq[(LfContractId, T, TransferCounter)]] = {
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
          .as[(LfContractId, T, TransferCounter)]
      case _: DbStorage.Profile.Postgres =>
        (sql"""
          select distinct(contract_id), AC3.#${p.attribute}, AC3.transfer_counter from par_active_contracts AC1
          join lateral
            (select #${p.attribute}, change, transfer_counter from par_active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and #${p.attribute} <= ${p.bound} order by ts desc, request_counter desc, change asc #${storage
            .limit(1)}) as AC3 on true
          where AC1.domain_id = $domainId and AC3.change = CAST(${ChangeType.Activation} as change_type)""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T, TransferCounter)]
      case _: DbStorage.Profile.Oracle =>
        (sql"""select distinct(contract_id), AC3.#${p.attribute}, AC3.transfer_counter from par_active_contracts AC1, lateral
          (select #${p.attribute}, change, transfer_counter from par_active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and #${p.attribute} <= ${p.bound}
             order by ts desc, request_counter desc, change desc
             fetch first 1 row only) AC3
          where AC1.domain_id = $domainId and AC3.change = 'activation'""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T, TransferCounter)]
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

  /* Computes the maximum transfer counter for each contract in the `res` vector.
     The computation for max_transferCounter(`rc`, `cid`) reuses the result of max_transferCounter(`rc-1`, `cid`).

       Assumption: the input `res` is already sorted by request counter.
   */
  /*
       TODO(i12904): Here we compute the maximum of the previous transfer counters;
        instead, we could retrieve the transfer counter of the latest activation
   */
  private def transferCounterForArchivals(
      res: Iterable[(TimeOfChange, LfContractId, ActivenessChangeDetail)]
  ): Map[(RequestCounter, LfContractId), Option[TransferCounter]] = {
    res
      .groupBy { case (_, cid, _) => cid }
      .flatMap { case (cid, changes) =>
        val sortedChangesByRc = changes.collect {
          case (TimeOfChange(rc, _), _, change)
              if change.name != ActivenessChangeDetail.transferOut =>
            ((rc, cid), change)
        }.toList

        NonEmpty.from(sortedChangesByRc) match {
          case None => List.empty
          case Some(changes) =>
            val ((rc, cid), op) = changes.head1
            val initial = ((rc, cid), (op.transferCounterO, op))

            changes.tail1.scanLeft(initial) {
              case (
                    ((_, _), (accTransferCounter, _)),
                    ((crtRc, cid), change),
                  ) =>
                (
                  (crtRc, cid),
                  (
                    Ordering[Option[TransferCounter]].max(
                      accTransferCounter,
                      change.transferCounterO,
                    ),
                    change,
                  ),
                )
            }
        }
      }
      .collect {
        case ((rc, cid), (transferCounter, Archive)) => ((rc, cid), transferCounter)
        case ((rc, cid), (transferCounter, Purge)) => ((rc, cid), transferCounter)
      }
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
      sql"""select ts, request_counter, contract_id, operation, transfer_counter, remote_domain_idx
             from par_active_contracts where domain_id = $domainId and
             ((ts = ${fromExclusive.timestamp} and request_counter > ${fromExclusive.rc}) or ts > ${fromExclusive.timestamp})
             and
             ((ts = ${toInclusive.timestamp} and request_counter <= ${toInclusive.rc}) or ts <= ${toInclusive.timestamp})
             order by ts asc, request_counter asc, change #$changeOrder"""
    }.as[(TimeOfChange, LfContractId, ActivenessChangeDetail)]

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
        val archivalsWithoutTransferCounters = maxTransferCountersPerCidUpToRc.filter(_._2.isEmpty)

        NonEmpty
          .from(archivalsWithoutTransferCounters.map { case ((_, contractId), _) =>
            contractId
          }.toSeq)
          .fold(
            Future.successful(Map.empty[(RequestCounter, LfContractId), Option[TransferCounter]])
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
                (sql"""select ts, request_counter, contract_id, operation, transfer_counter, remote_domain_idx
                   from par_active_contracts where domain_id = $domainId
                   and (request_counter <= $maximumRc)
                   and (ts <= ${toInclusive.timestamp})
                   and """ ++ inClause ++ sql""" order by ts asc, request_counter asc""")
                  .as[(TimeOfChange, LfContractId, ActivenessChangeDetail)]
              }
            val resultArchivalTransferCounters = storage
              .sequentialQueryAndCombine(
                archivalCidsWithoutTransferCountersQueries,
                "ACS: get data to compute the transfer counters for archived contracts",
              )

            resultArchivalTransferCounters.map(transferCounterForArchivals)
          }
      }

      res <- combineTransferCounters(
        maxTransferCountersPerRemainingCidUpToRc = maxTransferCountersPerRemainingCidUpToRc,
        maxTransferCountersPerCidUpToRc = maxTransferCountersPerCidUpToRc,
        retrievedChangesBetween = retrievedChangesBetween,
      )

    } yield res
  }

  private def combineTransferCounters(
      maxTransferCountersPerRemainingCidUpToRc: Map[
        (RequestCounter, LfContractId),
        Option[TransferCounter],
      ],
      maxTransferCountersPerCidUpToRc: Map[(RequestCounter, LfContractId), Option[TransferCounter]],
      retrievedChangesBetween: Seq[(TimeOfChange, LfContractId, ActivenessChangeDetail)],
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] = {
    // filter None entries from maxTransferCountersPerCidUpToRc, as the transfer counters for
    // those contracts are now in remainingMaxTransferCountersPerCidUpToRc
    val definedMaxTransferCountersPerCidUpToRc = maxTransferCountersPerCidUpToRc.collect {
      case (key, Some(transferCounter)) => (key, transferCounter)
    }

    type AccType = (LfContractId, StateChangeType)
    val empty = Vector.empty[AccType]

    IterableUtil
      .spansBy(retrievedChangesBetween) { case (toc, _, _) => toc }
      .traverse { case (toc, changes) =>
        val resE = changes.forgetNE
          .foldLeftM[Either[String, *], (Vector[AccType], Vector[AccType])]((empty, empty)) {
            case ((acts, deacts), (_, cid, change)) =>
              change match {
                case create: Create =>
                  Right((acts :+ (cid, create.toStateChangeType), deacts))

                case in: TransferIn =>
                  Right((acts :+ (cid, in.toStateChangeType), deacts))
                case out: TransferOut =>
                  Right((acts, deacts :+ (cid, out.toStateChangeType)))

                case add: Add =>
                  Right((acts :+ (cid, add.toStateChangeType), deacts))

                case Archive | Purge =>
                  val transferCounterE = definedMaxTransferCountersPerCidUpToRc
                    .get((toc.rc, cid))
                    .orElse(
                      maxTransferCountersPerRemainingCidUpToRc.get((toc.rc, cid)).flatten
                    )
                    .toRight(s"Unable to find transfer counter for $cid at $toc")

                  transferCounterE.map { transferCounter =>
                    val newChange = (
                      cid,
                      StateChangeType(ContractChange.Archived, transferCounter),
                    )

                    (acts, deacts :+ newChange)
                  }
              }
          }

        resE.map { case (acts, deacts) => toc -> ActiveContractIdsChange(acts.toMap, deacts.toMap) }
      }
      .bimap(err => Future.failed(new IllegalStateException(err)), Future.successful)
      .merge
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
      transfers: Seq[((LfContractId, TimeOfChange), TransferChangeDetail)]
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] =
    if (enableAdditionalConsistencyChecks) {
      transfers.parTraverse_ { case ((contractId, toc), transfer) =>
        for {
          _ <- checkTransferCountersShouldIncrease(contractId, toc, transfer)
          _ <- checkActivationsDeactivationConsistency(contractId, toc)
        } yield ()
      }
    } else CheckedT.pure(())

  private def checkActivationsDeactivationConsistency(
      contractId: LfContractId,
      toc: TimeOfChange,
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] = {

    val query = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        throw new IllegalArgumentException("Implement for oracle")
      case _ =>
        // change desc allows to have activations first
        sql"""select operation, transfer_counter, remote_domain_idx, ts, request_counter from par_active_contracts
              where domain_id = $domainId and contract_id = $contractId
              order by ts asc, request_counter asc, change desc"""
    }

    val changesF: Future[Vector[StoredActiveContract]] =
      storage.query(query.as[StoredActiveContract], functionFullName)

    val checkedUnit = Checked.unit[AcsError, AcsWarning]

    CheckedT(changesF.map { changes =>
      NonEmpty.from(changes).fold(checkedUnit) { changes =>
        NonEmptyChain
          .fromSeq(
            ActivationsDeactivationsConsistencyCheck(
              contractId,
              toc,
              changes.map(c => (c.toc, c.activenessChange)),
            )
          )
          .fold(checkedUnit)(Checked.continues)
      }
    })
  }

  private def checkTransferCountersShouldIncrease(
      contractId: LfContractId,
      toc: TimeOfChange,
      transfer: TransferChangeDetail,
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
          transfer.transferCounter,
          latestBeforeO.map(_.toTransferCounterAtChangeInfo),
        )
        _ <- ActiveContractStore.checkTransferCounterAgainstEarliestAfter(
          contractId,
          toc,
          transfer.transferCounter,
          earliestAfterO.map(_.toTransferCounterAtChangeInfo),
          transfer.toTransferType,
        )
      } yield ()
    }
  }

  private def bulkInsert(
      contractChanges: Map[(LfContractId, TimeOfChange), ActivenessChangeDetail],
      change: ChangeType,
      operationName: LengthLimitedString,
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val insertQuery = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( par_active_contracts ( contract_id, ts, request_counter, change, domain_id, transfer_counter ) ) */
          |into par_active_contracts
          |using (select ? contract_id, ? ts, ? request_counter, ? change, ? domain_id from dual) input
          |on (par_active_contracts.contract_id = input.contract_id and par_active_contracts.ts = input.ts and
          |    par_active_contracts.request_counter = input.request_counter and par_active_contracts.change = input.change and
          |    par_active_contracts.domain_id = input.domain_id)
          |when not matched then
          |  insert (contract_id, ts, request_counter, change, domain_id, operation, transfer_counter, remote_domain_idx)
          |  values (input.contract_id, input.ts, input.request_counter, input.change, input.domain_id, ?, ?, ?)""".stripMargin
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into par_active_contracts(contract_id, ts, request_counter, change, domain_id, operation, transfer_counter, remote_domain_idx)
          values (?, ?, ?, CAST(? as change_type), ?, CAST(? as operation_type), ?, ?)
          on conflict do nothing"""
    }
    val insertAll =
      DbStorage.bulkOperation_(insertQuery, contractChanges, storage.profile) { pp => element =>
        val ((contractId, toc), operationType) = element
        pp >> contractId
        pp >> toc.timestamp
        pp >> toc.rc
        pp >> operationType.changeType
        pp >> domainId
        pp >> operationType
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
          sql"select contract_id, operation, transfer_counter, remote_domain_idx, ts, request_counter from par_active_contracts where domain_id = $domainId and " ++ cidsInClause ++
            sql" and " ++ tssInClause ++ sql" and " ++ rcsInClause ++ sql" and change = $change"
        case _ =>
          sql"select contract_id, operation, transfer_counter, remote_domain_idx, ts, request_counter from par_active_contracts where domain_id = $domainId and " ++ cidsInClause ++
            sql" and " ++ tssInClause ++ sql" and " ++ rcsInClause ++ sql" and change = CAST($change as change_type)"
      }

      val queries =
        contractIdsNotInsertedInClauses.zip(rcsInClauses).zip(tssInClauses).map {
          case ((cidsInClause, rcsInClause), tssInClause) =>
            query(cidsInClause, rcsInClause, tssInClause)
              .as[(LfContractId, ActivenessChangeDetail, TimeOfChange)]
        }

      val isActivation = change == ChangeType.Activation

      val warningsF = storage
        .sequentialQueryAndCombine(queries, functionFullName)
        .map(_.toList.mapFilter { case (cid, previousOperationType, toc) =>
          val newOperationType = contractChanges.getOrElse((cid, toc), previousOperationType)

          if (newOperationType == previousOperationType)
            None
          else {
            if (isActivation)
              Some(
                SimultaneousActivation(
                  cid,
                  toc,
                  previousOperationType,
                  newOperationType,
                )
              )
            else
              Some(
                SimultaneousDeactivation(
                  cid,
                  toc,
                  previousOperationType,
                  newOperationType,
                )
              )
          }
        })

      CheckedT(warningsF.map(_.traverse_(Checked.continue)))
    }

    CheckedT.result(storage.queryAndUpdate(insertAll, functionFullName)).flatMap { (_: Unit) =>
      if (enableAdditionalConsistencyChecks) {
        // Check all contracts whether they have been inserted or are already there
        // We don't analyze the update count
        // so that we can use the fast IGNORE_ROW_ON_DUPKEY_INDEX directive in Oracle
        NonEmpty
          .from(contractChanges.keySet.toSeq)
          .map(checkIdempotence)
          .getOrElse(CheckedT.pure(()))
      } else CheckedT.pure(())
    }
  }

  private def fetchLatestCreation(
      contractId: LfContractId
  ): DbAction.ReadOnly[Option[StoredActiveContract]] =
    fetchContractStateQuery(contractId, Some(ActivenessChangeDetail.create))

  private def fetchEarliestArchival(
      contractId: LfContractId
  ): DbAction.ReadOnly[Option[StoredActiveContract]] =
    fetchContractStateQuery(contractId, Some(ActivenessChangeDetail.archive), descending = false)

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
      operationFilter: Option[LengthLimitedString] = None,
      tocFilter: Option[TimeOfChange] = None,
      descending: Boolean = true,
  ): DbAction.ReadOnly[Option[StoredActiveContract]] = {

    import DbStorage.Implicits.BuilderChain.*

    val baseQuery =
      sql"""select operation, transfer_counter, remote_domain_idx, ts, request_counter from par_active_contracts
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
}
