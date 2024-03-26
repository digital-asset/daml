// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, NonEmptyChain}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String100}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail.{
  Add,
  Archive,
  Create,
  Purge,
  TransferChangeDetail,
  TransferIn,
  TransferOut,
}
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
import com.digitalasset.canton.resource.DbStorage.*
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.{DbDeserializationException, DbPrunableByTimeDomain}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore, PrunableByTimeParameters}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, IterableUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
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
  * - CREATE index active_contracts_dirty_request_reset_idx ON active_contracts (domain_id, request_counter)
  * used on startup of the SyncDomain to delete all dirty requests.
  * - CREATE index active_contracts_contract_id_idx ON active_contracts (contract_id)
  * used in conflict detection for point wise lookup of the contract status.
  * - CREATE index active_contracts_ts_domain_id_idx ON active_contracts (ts, domain_id)
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

  protected[this] override val pruning_status_table = "active_contract_pruning"

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
        case Create => Future.successful(Active)
        case Archive => Future.successful(Archived)
        case Add => Future.successful(Active)
        case Purge => Future.successful(Purged)
        case _: TransferIn => Future.successful(Active)
        case out: TransferOut =>
          domainIdFromIdx(out.remoteDomainIdx).map(id => TransferredAway(TargetDomainId(id)))
      }

      statusF.map(ContractState(_, toc.rc, toc.timestamp))
    }
  }

  private implicit val getResultStoredActiveContract: GetResult[StoredActiveContract] =
    GetResult { r =>
      val activenessChange = GetResult[ActivenessChangeDetail].apply(r)
      val ts = GetResult[CantonTimestamp].apply(r)
      val rc = GetResult[RequestCounter].apply(r)

      StoredActiveContract(activenessChange, TimeOfChange(rc, ts))
    }

  override protected val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("active-contract-store")

  override def markContractsCreatedOrAdded(
      contracts: Seq[LfContractId],
      toc: TimeOfChange,
      isCreation: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val (operationName, change) =
      if (isCreation) (ActivenessChangeDetail.create, ActivenessChangeDetail.Create)
      else (ActivenessChangeDetail.add, ActivenessChangeDetail.Add)

    processingTime.checkedTEvent {
      for {
        activeContractsData <- CheckedT.fromEitherT(
          EitherT.fromEither[Future](
            ActiveContractsData
              .create(protocolVersion, toc, contracts)
              .leftMap(errorMessage => ActiveContractsDataInvariantViolation(errorMessage))
          )
        )
        _ <- bulkInsert(
          activeContractsData.asMap(change),
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
                for {
                  _ <- checkActivationsDeactivationConsistency(
                    tc.contractId,
                    activeContractsData.toc,
                  )
                } yield ()
              }
            }
          } else {
            CheckedT.resultT[Future, AcsError, AcsWarning](())
          }
      } yield ()
    }
  }

  override def purgeOrArchiveContracts(
      contracts: Seq[LfContractId],
      toc: TimeOfChange,
      isArchival: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val (operationName, change) =
      if (isArchival) (ActivenessChangeDetail.archive, ActivenessChangeDetail.Archive)
      else (ActivenessChangeDetail.purge, ActivenessChangeDetail.Purge)

    processingTime.checkedTEvent {
      for {
        _ <- bulkInsert(
          contracts.map(cid => ((cid, toc), change)).toMap,
          ChangeType.Deactivation,
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
          } else {
            CheckedT.resultT[Future, AcsError, AcsWarning](())
          }
      } yield ()
    }
  }

  private def indexedDomains(
      contractByDomain: Seq[
        (TransferDomainId, Seq[(LfContractId, TimeOfChange)])
      ]
  ): CheckedT[Future, AcsError, AcsWarning, Seq[
    (IndexedDomain, Seq[(LfContractId, TimeOfChange)])
  ]] =
    CheckedT.result(contractByDomain.parTraverse { case (domainId, contracts) =>
      IndexedDomain
        .indexed(indexedStringStore)(domainId.unwrap)
        .map(_ -> contracts)
    })

  private def transferContracts(
      transfers: Seq[(LfContractId, TransferDomainId, TimeOfChange)],
      builder: Int => TransferChangeDetail,
      change: ChangeType,
      operationName: LengthLimitedString,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val domains = transfers.map { case (_, domain, _) => domain.unwrap }.distinct

    type PreparedTransfer = ((LfContractId, TimeOfChange), TransferChangeDetail)

    for {
      domainIndices <- getDomainIndices(domains)

      preparedTransfersE = MonadUtil.sequentialTraverse(
        transfers
      ) { case (cid, remoteDomain, toc) =>
        domainIndices
          .get(remoteDomain.unwrap)
          .toRight[AcsError](UnableToFindIndex(remoteDomain.unwrap))
          .map(idx => ((cid, toc), builder(idx.index)))
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

      _ <- checkTransfersConsistency(preparedTransfers.map { case ((cid, toc), _) => (cid, toc) })
    } yield ()
  }

  override def transferInContracts(
      transferIns: Seq[(LfContractId, SourceDomainId, TimeOfChange)]
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
      transferOuts: Seq[(LfContractId, TargetDomainId, TimeOfChange)]
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
                with ordered_changes(contract_id, operation, remote_domain_id, ts, request_counter, row_num) as (
                  select contract_id, operation, remote_domain_id, ts, request_counter,
                     ROW_NUMBER() OVER (partition by domain_id, contract_id order by ts desc, request_counter desc, change asc)
                   from active_contracts
                   where domain_id = $domainId and """ ++ inClause ++
                      sql"""
                )
                select contract_id, operation, remote_domain_id, ts, request_counter
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
                  select active_contracts.contract_id, contracts.package_id, change, ts, active_contracts.request_counter, remote_domain_id,
                     ROW_NUMBER() OVER (
                     partition by active_contracts.domain_id, active_contracts.contract_id
                     order by
                        ts desc,
                        active_contracts.request_counter desc,
                        change asc
                     )
                   from active_contracts join contracts
                       on active_contracts.contract_id = contracts.contract_id
                              and active_contracts.domain_id = contracts.domain_id
                   where active_contracts.domain_id = $domainId
                    and contracts.package_id = $pkg
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
  ): Future[SortedMap[LfContractId, CantonTimestamp]] =
    processingTime.event {
      logger.debug(s"Obtaining ACS snapshot at $timestamp")
      storage
        .query(
          snapshotQuery(SnapshotQueryParameter.Ts(timestamp), None),
          functionFullName,
        )
        .map { snapshot => SortedMap.from(snapshot) }
    }

  override def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, RequestCounter]] = processingTime.event {
    logger.debug(s"Obtaining ACS snapshot at $rc")
    storage
      .query(
        snapshotQuery(SnapshotQueryParameter.Rc(rc), None),
        functionFullName,
      )
      .map { snapshot =>
        SortedMap.from(snapshot.map { case (cid, rc) =>
          cid -> rc
        })
      }
  }

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, CantonTimestamp]] = {
    processingTime.event {
      if (contractIds.isEmpty) Future.successful(Map.empty)
      else
        storage
          .query(
            snapshotQuery(SnapshotQueryParameter.Ts(timestamp), Some(contractIds)),
            functionFullName,
          )
          .map(_.toMap)
    }
  }

  private[this] def snapshotQuery[T](
      p: SnapshotQueryParameter[T],
      contractIds: Option[Set[LfContractId]],
  ): DbAction.ReadOnly[Seq[(LfContractId, T)]] = {
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
          select distinct(contract_id), #${p.attribute}
          from active_contracts AC
          where not exists(select * from active_contracts AC2 where domain_id = $domainId and AC.contract_id = AC2.contract_id
            and AC2.#${p.attribute} <= ${p.bound}
            and ((AC.ts, AC.request_counter) < (AC2.ts, AC2.request_counter)
              or (AC.ts = AC2.ts and AC.request_counter = AC2.request_counter and AC2.change = ${ChangeType.Deactivation})))
           and AC.#${p.attribute} <= ${p.bound} and domain_id = $domainId""" ++
          idsO.fold(sql"")(ids => sql" and AC.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T)]
      case _: DbStorage.Profile.Postgres =>
        (sql"""
          select distinct(contract_id), AC3.#${p.attribute} from active_contracts AC1
          join lateral
            (select #${p.attribute}, change from active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and #${p.attribute} <= ${p.bound} order by ts desc, request_counter desc, change asc #${storage
            .limit(1)}) as AC3 on true
          where AC1.domain_id = $domainId and AC3.change = CAST(${ChangeType.Activation} as change_type)""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T)]
      case _: DbStorage.Profile.Oracle =>
        (sql"""select distinct(contract_id), AC3.#${p.attribute} from active_contracts AC1, lateral
          (select #${p.attribute}, change from active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and #${p.attribute} <= ${p.bound}
             order by ts desc, request_counter desc, change desc
             fetch first 1 row only) AC3
          where AC1.domain_id = $domainId and AC3.change = 'activation'""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T)]
    }
  }

  override def doPrune(beforeAndIncluding: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): Future[Int] = processingTime.event {
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
                    from active_contracts
                    where domain_id = ${domainId}
                      and change = cast('deactivation' as change_type)
                      and ts <= ${beforeAndIncluding}
                    group by contract_id
                  )
                    select ac.contract_id, ac.ts, ac.request_counter, ac.change
                    from deactivation_counter dc
                      join active_contracts ac on ac.domain_id = ${domainId} and ac.contract_id = dc.contract_id
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
                      s"delete from active_contracts where domain_id = ? and contract_id = ? and ts = ?"
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
              from active_contracts
              where domain_id = ${domainId}
              and change = ${ChangeType.Deactivation}
              and ts <= ${beforeAndIncluding}
              group by contract_id
            )
		    delete from active_contracts
            where (domain_id, contract_id, ts, request_counter, change) in (
		      select ac.domain_id, ac.contract_id, ac.ts, ac.request_counter, ac.change
              from deactivation_counter dc
              join active_contracts ac on ac.domain_id = ${domainId} and ac.contract_id = dc.contract_id
              where ac.request_counter <= dc.request_counter
            );
            """,
                functionFullName,
              )
            )
          case _: DbStorage.Profile.Oracle =>
            performUnlessClosingF("ACS.doPrune")(
              storage.queryAndUpdate(
                sqlu"""delete from active_contracts where rowid in (
            with deactivation_counter(contract_id, request_counter) as (
                select contract_id, max(request_counter)
                from active_contracts
                where domain_id = ${domainId}
                and change = 'deactivation'
                and ts <= ${beforeAndIncluding}
                group by contract_id
            )
            select ac.rowid
            from deactivation_counter dc
            join active_contracts ac on ac.domain_id = ${domainId} and ac.contract_id = dc.contract_id
            where ac.request_counter <= dc.request_counter
            )""",
                functionFullName,
              )
            )
        }
    } yield nrPruned).onShutdown(0)
  }

  def deleteSince(criterion: RequestCounter)(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
      val query =
        sqlu"delete from active_contracts where domain_id = $domainId and request_counter >= $criterion"
      storage
        .update(query, functionFullName)
        .map(count =>
          logger.debug(s"DeleteSince on $criterion removed at least $count ACS entries")
        )
    }

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    processingTime.event {
      ErrorUtil.requireArgument(
        fromExclusive <= toInclusive,
        s"Provided timestamps are in the wrong order: $fromExclusive and $toInclusive",
      )
      val changeQuery = {
        val changeOrder = storage.profile match {
          case _: DbStorage.Profile.Oracle => "asc"
          case _ => "desc"
        }
        sql"""select ts, request_counter, contract_id, change, operation
                         from active_contracts where domain_id = $domainId and
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
            OperationType,
        )
      ]

      for {
        retrievedChangesBetween <- storage.query(
          changeQuery,
          operationName = "ACS: get changes between",
        )
      } yield {
        val groupedByTs =
          IterableUtil.spansBy(retrievedChangesBetween)(entry => (entry._1, entry._2))
        groupedByTs.map { case ((ts, rc), changes) =>
          val (acts, deacts) = changes.partition { case (_, _, _, changeType, _) =>
            changeType == ChangeType.Activation
          }
          TimeOfChange(rc, ts) -> ActiveContractIdsChange(
            acts.map { case (_, _, cid, _, opType) =>
              if (opType == OperationType.Create)
                (cid, StateChangeType(ContractChange.Created))
              else
                (cid, StateChangeType(ContractChange.TransferIn))
            }.toMap,
            deacts.map { case (_, _, cid, _, opType) =>
              if (opType == OperationType.Archive) {
                (
                  cid,
                  StateChangeType(ContractChange.Archived),
                )
              } else (cid, StateChangeType(ContractChange.TransferOut))
            }.toMap,
          )
        }
      }
    }

  override private[participant] def contractCount(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] = {
    storage.query(
      sql"select count(distinct contract_id) from active_contracts where ts <= $timestamp"
        .as[Int]
        .head,
      functionFullName,
    )
  }

  private def checkActivationsDeactivationConsistency(
      contractId: LfContractId,
      toc: TimeOfChange,
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] = {

    val query = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        // change asc allows to have activations first
        sql"""select operation, remote_domain_id, ts, request_counter from active_contracts
                where domain_id = $domainId and contract_id = $contractId
                order by ts asc, request_counter asc, change asc
                """
      case _ =>
        // change desc allows to have activations first
        sql"""select operation, remote_domain_id, ts, request_counter from active_contracts
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

  private def checkTransfersConsistency(
      transfers: Seq[(LfContractId, TimeOfChange)]
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] =
    if (enableAdditionalConsistencyChecks) {
      transfers.parTraverse_ { case (contractId, toc) =>
        for {
          _ <- checkActivationsDeactivationConsistency(contractId, toc)
        } yield ()
      }
    } else CheckedT.pure(())

  private def bulkInsert(
      contractChanges: Map[(LfContractId, TimeOfChange), ActivenessChangeDetail],
      change: ChangeType,
      operationName: LengthLimitedString,
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val insertQuery = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( active_contracts ( contract_id, ts, request_counter, change, domain_id ) ) */
          |into active_contracts
          |using (select ? contract_id, ? ts, ? request_counter, ? change, ? domain_id from dual) input
          |on (active_contracts.contract_id = input.contract_id and active_contracts.ts = input.ts and
          |    active_contracts.request_counter = input.request_counter and active_contracts.change = input.change and
          |    active_contracts.domain_id = input.domain_id)
          |when not matched then
          |  insert (contract_id, ts, request_counter, change, domain_id, operation, remote_domain_id)
          |  values (input.contract_id, input.ts, input.request_counter, input.change, input.domain_id, ?, ?)""".stripMargin
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into active_contracts(contract_id, ts, request_counter, change, domain_id, operation, remote_domain_id)
          values (?, ?, ?, CAST(? as change_type), ?, CAST(? as operation_type), ?)
          on conflict do nothing"""
    }
    val insertAll =
      DbStorage.bulkOperation_(insertQuery, contractChanges, storage.profile) { pp => element =>
        val ((contractId, toc), changeDetail) = element
        pp >> contractId
        pp >> toc.timestamp
        pp >> toc.rc
        pp >> changeDetail.changeType
        pp >> domainId
        pp >> changeDetail
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
          sql"select contract_id, operation, remote_domain_id, ts, request_counter from active_contracts where domain_id = $domainId and " ++ cidsInClause ++
            sql" and " ++ tssInClause ++ sql" and " ++ rcsInClause ++ sql" and change = $change"
        case _ =>
          sql"select contract_id, operation, remote_domain_id, ts, request_counter from active_contracts where domain_id = $domainId and " ++ cidsInClause ++
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
    fetchContractStateQuery(contractId, Some(OperationType.Create))

  private def fetchEarliestArchival(
      contractId: LfContractId
  ): DbAction.ReadOnly[Option[StoredActiveContract]] =
    fetchContractStateQuery(contractId, Some(OperationType.Archive), descending = false)

  private def fetchContractStateQuery(
      contractId: LfContractId,
      operationFilter: Option[OperationType] = None,
      tocFilter: Option[TimeOfChange] = None,
      descending: Boolean = true,
  ): DbAction.ReadOnly[Option[StoredActiveContract]] = {

    import DbStorage.Implicits.BuilderChain.*

    val baseQuery =
      sql"""select operation, remote_domain_id, ts, request_counter from active_contracts
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

    case object Add extends OperationType {
      override val name = "add"
    }

    case object Purge extends OperationType {
      override val name = "purge"
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
        case OperationType.Add.name => OperationType.Add
        case OperationType.Purge.name => OperationType.Purge
        case unknown => throw new DbDeserializationException(s"Unknown operation type [$unknown]")
      }
    )
  }

}
