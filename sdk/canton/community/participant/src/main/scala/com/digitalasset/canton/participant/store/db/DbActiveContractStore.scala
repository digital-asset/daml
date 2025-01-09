// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, NonEmptyChain}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail.*
import com.digitalasset.canton.participant.store.data.ActiveContractsData
import com.digitalasset.canton.participant.store.db.DbActiveContractStore.*
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.resource.DbStorage.*
import com.digitalasset.canton.resource.DbStorage.Implicits.BuilderChain.{
  fromSQLActionBuilderChain,
  toSQLActionBuilderChain,
}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbPrunableByTimeDomain
import com.digitalasset.canton.store.{
  IndexedStringStore,
  IndexedSynchronizer,
  PrunableByTimeParameters,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{ReassignmentCounter, RequestCounter}
import com.digitalasset.daml.lf.data.Ref.PackageId
import slick.jdbc.*

import scala.Ordered.orderingToOrdered
import scala.collection.View
import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext

/** Active contracts journal
  *
  * This database table has the following indexes to support scaling query performance:
  * - create index idx_par_active_contracts_dirty_request_reset on par_active_contracts (synchronizer_idx, request_counter)
  * used on startup of the SyncDomain to delete all inflight validation requests.
  * - create index idx_par_active_contracts_contract_id on par_active_contracts (contract_id)
  * used in conflict detection for point-wise lookup of the contract status.
  * - create index idx_par_active_contracts_ts_synchronizer_idx on par_active_contracts (ts, synchronizer_idx)
  * used on startup by the SyncDomain to replay ACS changes to the ACS commitment processor.
  */
class DbActiveContractStore(
    override protected val storage: DbStorage,
    protected[this] override val indexedSynchronizer: IndexedSynchronizer,
    enableAdditionalConsistencyChecks: Boolean,
    batchingParametersConfig: PrunableByTimeParameters,
    val indexedStringStore: IndexedStringStore,
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

  private def checkedTUnit: CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] =
    CheckedT.resultT[FutureUnlessShutdown, AcsError, AcsWarning](())

  /*
  Consider the scenario where a contract is created on domain D1, then reassigned to D2, then to D3 and is finally archived.
  We will have the corresponding entries in the ActiveContractStore:
  - On D1, remoteDomain will initially be None and then Some(D2) (after the unassignment)
  - On D2, remoteDomain will initially be Some(D1) and then Some(D3) (after the unassignment)
  - On D3, remoteDomain will initially be Some(D2) and then None (after the archival).
   */
  private case class StoredActiveContract(
      activenessChange: ActivenessChangeDetail,
      toc: TimeOfChange,
  ) {
    def toContractStateFUS(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): FutureUnlessShutdown[ContractState] = {
      val statusF = activenessChange match {
        case Create(reassignmentCounter) => FutureUnlessShutdown.pure(Active(reassignmentCounter))
        case Archive => FutureUnlessShutdown.pure(Archived)
        case Add(reassignmentCounter) => FutureUnlessShutdown.pure(Active(reassignmentCounter))
        case Purge => FutureUnlessShutdown.pure(Purged)
        case in: Assignment => FutureUnlessShutdown.pure(Active(in.reassignmentCounter))
        case out: Unassignment =>
          synchronizerIdFromIdxFUS(out.remoteSynchronizerIdx).map(id =>
            ReassignedAway(Target(id), out.reassignmentCounter)
          )
      }

      statusF.map(ContractState(_, toc.rc, toc.timestamp))
    }

    def toReassignmentCounterAtChangeInfo: ReassignmentCounterAtChangeInfo =
      ReassignmentCounterAtChangeInfo(toc, activenessChange.reassignmentCounterO)
  }

  private implicit val getResultStoredActiveContract: GetResult[StoredActiveContract] =
    GetResult { r =>
      val activenessChange = GetResult[ActivenessChangeDetail].apply(r)
      val ts = GetResult[CantonTimestamp].apply(r)
      val rc = GetResult[RequestCounter].apply(r)

      StoredActiveContract(activenessChange, TimeOfChange(rc, ts))
    }

  override def markContractsCreatedOrAdded(
      contracts: Seq[(LfContractId, ReassignmentCounter, TimeOfChange)],
      isCreation: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    val (operationName, builder) =
      if (isCreation) (ActivenessChangeDetail.create, ActivenessChangeDetail.Create(_))
      else (ActivenessChangeDetail.add, ActivenessChangeDetail.Add(_))

    for {
      activeContractsData <- CheckedT.fromEitherT(
        EitherT.fromEither[FutureUnlessShutdown](
          ActiveContractsData
            .create(contracts)
            .leftMap(errorMessage => ActiveContractsDataInvariantViolation(errorMessage))
        )
      )
      _ <- bulkInsert(
        activeContractsData.asMap.fmap(builder),
        change = ChangeType.Activation,
      )
      _ <-
        if (enableAdditionalConsistencyChecks) {
          performUnlessClosingCheckedUST(
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
                tc.toc,
              )
            }
          }
        } else checkedTUnit
    } yield ()
  }

  override def purgeOrArchiveContracts(
      contracts: Seq[(LfContractId, TimeOfChange)],
      isArchival: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    val (operationName, operation) =
      if (isArchival) (ActivenessChangeDetail.archive, ActivenessChangeDetail.Archive)
      else (ActivenessChangeDetail.purge, ActivenessChangeDetail.Purge)

    for {
      _ <- bulkInsert(
        contracts.map(contract => (contract, operation)).toMap,
        change = ChangeType.Deactivation,
      )
      _ <-
        if (enableAdditionalConsistencyChecks) {
          performUnlessClosingCheckedUST(
            "additional-consistency-check",
            Checked.result[AcsError, AcsWarning, Unit](
              logger.debug(
                "Could not perform additional consistency check because node is shutting down"
              )
            ),
          )(contracts.parTraverse_(checkActivationsDeactivationConsistency tupled))
        } else checkedTUnit
    } yield ()
  }

  private def reassignContracts(
      reassignments: Seq[
        (LfContractId, ReassignmentTag[SynchronizerId], ReassignmentCounter, TimeOfChange)
      ],
      builder: (ReassignmentCounter, Int) => ReassignmentChangeDetail,
      change: ChangeType,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    val synchronizerIds = reassignments.map { case (_, domain, _, _) => domain.unwrap }.distinct

    type PreparedReassignment = ((LfContractId, TimeOfChange), ReassignmentChangeDetail)

    for {
      synchronizerIndices <- getSynchronizerIndices(synchronizerIds)

      preparedReassignmentsE = MonadUtil.sequentialTraverse(
        reassignments
      ) { case (cid, remoteDomain, reassignmentCounter, toc) =>
        synchronizerIndices
          .get(remoteDomain.unwrap)
          .toRight[AcsError](UnableToFindIndex(remoteDomain.unwrap))
          .map(idx => ((cid, toc), builder(reassignmentCounter, idx.index)))
      }

      preparedReassignments <- CheckedT.fromChecked(
        Checked.fromEither(preparedReassignmentsE)
      ): CheckedT[
        FutureUnlessShutdown,
        AcsError,
        AcsWarning,
        Seq[PreparedReassignment],
      ]

      _ <- bulkInsert(
        preparedReassignments.toMap,
        change,
      )

      _ <- checkReassignmentsConsistency(preparedReassignments)
    } yield ()
  }

  override def assignContracts(
      assignments: Seq[(LfContractId, Source[SynchronizerId], ReassignmentCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] =
    reassignContracts(
      assignments,
      Assignment.apply,
      ChangeType.Activation,
    )

  override def unassignContracts(
      unassignments: Seq[(LfContractId, Target[SynchronizerId], ReassignmentCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = reassignContracts(
    unassignments,
    Unassignment.apply,
    ChangeType.Deactivation,
  )

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfContractId, ContractState]] =
    storage.profile match {
      case _: DbStorage.Profile.H2 =>
        // With H2, it is faster to do lookup contracts individually than to use a range query
        contractIds
          .to(LazyList)
          .parTraverseFilter { contractId =>
            storage
              .querySingleUnlessShutdown(fetchContractStateQuery(contractId), functionFullName)
              .semiflatMap(_.toContractStateFUS.map(res => (contractId -> res)))
              .value
          }
          .map(_.toMap)
      case _: DbStorage.Profile.Postgres =>
        NonEmpty.from(contractIds.toSeq) match {
          case None => FutureUnlessShutdown.pure(Map.empty)
          case Some(contractIdsNel) =>
            import DbStorage.Implicits.BuilderChain.*

            val query =
              (sql"""
                with ordered_changes(contract_id, operation, reassignment_counter, remote_synchronizer_idx, ts, request_counter, row_num) as (
                  select contract_id, operation, reassignment_counter, remote_synchronizer_idx, ts, request_counter,
                     ROW_NUMBER() OVER (partition by synchronizer_idx, contract_id order by ts desc, request_counter desc, change asc)
                   from par_active_contracts
                   where synchronizer_idx = $indexedSynchronizer and """ ++ DbStorage
                .toInClause("contract_id", contractIdsNel) ++
                sql"""
                )
                select contract_id, operation, reassignment_counter, remote_synchronizer_idx, ts, request_counter
                from ordered_changes
                where row_num = 1;
                """).as[(LfContractId, StoredActiveContract)]

            storage
              .queryUnlessShutdown(query, functionFullName)
              .flatMap(_.toList.parTraverse { case (id, contract) =>
                contract.toContractStateFUS.map(cs => (id, cs))
              })
              .map(foundContracts => foundContracts.toMap)
        }

    }

  override def packageUsage(
      pkg: PackageId,
      contractStore: ContractStore,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[(LfContractId)]] = {
    // The contractStore is unused
    // As we can directly query daml_contracts from the database

    import DbStorage.Implicits.*

    // TODO(i9480): Integrate with performance tests to check that we can remove packages when there are many contracts.

    val query =
      sql"""
          with ordered_changes(contract_id, package_id, change, ts, request_counter, remote_synchronizer_idx, row_num) as (
            select par_active_contracts.contract_id, par_contracts.package_id, change, ts, par_active_contracts.request_counter, remote_synchronizer_idx,
               ROW_NUMBER() OVER (
               partition by par_active_contracts.synchronizer_idx, par_active_contracts.contract_id
               order by
                  ts desc,
                  par_active_contracts.request_counter desc,
                  change asc
               )
             from par_active_contracts join par_contracts
              on par_active_contracts.contract_id = par_contracts.contract_id
             where par_active_contracts.synchronizer_idx = $indexedSynchronizer
              and par_contracts.package_id = $pkg
          )
          select contract_id, package_id
          from ordered_changes
          where row_num = 1
          and change = 'activation'
          limit 1
          """.as[(LfContractId)]

    val queryResult = storage.queryUnlessShutdown(query, functionFullName)
    queryResult.map(_.headOption)

  }

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedMap[LfContractId, (CantonTimestamp, ReassignmentCounter)]] = {
    logger.debug(s"Obtaining ACS snapshot at $timestamp")
    storage
      .query(
        snapshotQuery(SnapshotQueryParameter.Ts(timestamp), None),
        functionFullName,
      )
      .map { snapshot =>
        SortedMap.from(snapshot.map { case (cid, ts, reassignmentCounter) =>
          cid -> (ts, reassignmentCounter)
        })
      }
  }

  override def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedMap[LfContractId, (RequestCounter, ReassignmentCounter)]] = {
    logger.debug(s"Obtaining ACS snapshot at $rc")
    storage
      .query(
        snapshotQuery(SnapshotQueryParameter.Rc(rc), None),
        functionFullName,
      )
      .map { snapshot =>
        SortedMap.from(snapshot.map { case (cid, rc, reassignmentCounter) =>
          cid -> (rc, reassignmentCounter)
        })
      }
  }

  override def activenessOf(contracts: Seq[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    SortedMap[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]
  ] = {
    logger.debug(s"Obtaining activeness changes of contracts $contracts")

    NonEmpty.from(contracts) match {
      case None =>
        FutureUnlessShutdown.pure(
          SortedMap.empty[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]
        )
      case Some(neContracts) =>
        storage
          .query(
            activenessQuery(neContracts),
            functionFullName,
          )
          .map { res =>
            SortedMap.from(
              res
                .groupBy { case (cid, ts, operation) => cid }
                .map { case (cid, seq) =>
                  cid -> seq.map { case (cid2, ts, operation) => (ts, operation) }.toSeq
                }
            )
          }
    }
  }

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, CantonTimestamp]] =
    if (contractIds.isEmpty) FutureUnlessShutdown.pure(Map.empty)
    else
      storage
        .query(
          snapshotQuery(SnapshotQueryParameter.Ts(timestamp), Some(contractIds)),
          functionFullName,
        )
        .map(_.view.map { case (cid, ts, _) => cid -> ts }.toMap)

  override def bulkContractsReassignmentCounterSnapshot(
      contractIds: Set[LfContractId],
      requestCounter: RequestCounter,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ReassignmentCounter]] = {
    logger.debug(
      s"Looking up reassignment counters for contracts $contractIds up to but not including $requestCounter"
    )
    if (requestCounter == RequestCounter.MinValue)
      ErrorUtil.internalError(
        new IllegalArgumentException(
          s"The request counter $requestCounter should not be equal to ${RequestCounter.MinValue}"
        )
      )
    if (contractIds.isEmpty) FutureUnlessShutdown.pure(Map.empty)
    else {
      for {
        acsArchivalContracts <-
          storage
            .query(
              snapshotQuery(SnapshotQueryParameter.Rc(requestCounter - 1), Some(contractIds)),
              functionFullName,
            )
            .map { snapshot =>
              Map.from(snapshot.map { case (cid, _, reassignmentCounter) =>
                cid -> reassignmentCounter
              })
            }
      } yield {
        contractIds
          .diff(acsArchivalContracts.keySet)
          .foreach(cid =>
            ErrorUtil.internalError(
              new IllegalStateException(
                s"Archived non-transient contract $cid should have been active in the ACS and have a reassignment counter defined"
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
  ): DbAction.ReadOnly[Seq[(LfContractId, T, ReassignmentCounter)]] = {
    import DbStorage.Implicits.BuilderChain.*

    val idsO = contractIds.map { ids =>
      sql"(" ++ ids.toList.map(id => sql"$id").intercalate(sql", ") ++ sql")"
    }

    implicit val getResultT: GetResult[T] = p.getResult

    implicit val setParameterT: SetParameter[T] = p.setParameter

    val ordering = sql" order by #${p.attribute} asc"

    storage.profile match {
      case _: DbStorage.Profile.H2 =>
        (sql"""
          select distinct(contract_id), #${p.attribute}, reassignment_counter
          from par_active_contracts AC
          where not exists(select * from par_active_contracts AC2 where synchronizer_idx = $indexedSynchronizer and AC.contract_id = AC2.contract_id
            and AC2.#${p.attribute} <= ${p.bound}
            and ((AC.ts, AC.request_counter) < (AC2.ts, AC2.request_counter)
              or (AC.ts = AC2.ts and AC.request_counter = AC2.request_counter and AC2.change = ${ChangeType.Deactivation})))
           and AC.#${p.attribute} <= ${p.bound} and synchronizer_idx = $indexedSynchronizer""" ++
          idsO.fold(sql"")(ids => sql" and AC.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T, ReassignmentCounter)]
      case _: DbStorage.Profile.Postgres =>
        (sql"""
          select distinct(contract_id), AC3.#${p.attribute}, AC3.reassignment_counter from par_active_contracts AC1
          join lateral
            (select #${p.attribute}, change, reassignment_counter from par_active_contracts AC2 where synchronizer_idx = $indexedSynchronizer
             and AC2.contract_id = AC1.contract_id and #${p.attribute} <= ${p.bound} order by ts desc, request_counter desc, change asc #${storage
            .limit(1)}) as AC3 on true
          where AC1.synchronizer_idx = $indexedSynchronizer and AC3.change = CAST(${ChangeType.Activation} as change_type)""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T, ReassignmentCounter)]
    }
  }

  private[this] def activenessQuery(
      contractIds: NonEmpty[Seq[LfContractId]]
  ): DbAction.ReadOnly[Seq[(LfContractId, CantonTimestamp, ActivenessChangeDetail)]] = {
    import DbStorage.Implicits.BuilderChain.*

    storage.profile match {
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        (sql"""
          select contract_id, ts, operation, reassignment_counter, remote_synchronizer_idx
          from par_active_contracts
          where synchronizer_idx = $indexedSynchronizer and """ ++ DbStorage
          .toInClause("contract_id", contractIds))
          .as[(LfContractId, CantonTimestamp, ActivenessChangeDetail)]
      case _ => throw new UnsupportedOperationException("Oracle not supported")
    }
  }

  override def doPrune(beforeAndIncluding: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    // For each contract select the last deactivation before or at the timestamp.
    // If such a deactivation exists then delete all acs records up to and including the deactivation

    for {
      nrPruned <-
        storage.profile match {
          case _: DbStorage.Profile.Postgres =>
            // On postgres running the single-sql-statement with both select/delete has resulted in Postgres
            // flakily hanging indefinitely on the ACS pruning select/delete. The only workaround that also still makes
            // use of the partial index "active_contracts_pruning_idx" appears to be splitting the select and delete
            // into separate statements. See #11292.
            for {
              acsEntriesToPrune <- performUnlessClosingUSF("Fetch ACS entries batch")(
                storage.query(
                  sql"""
                  with deactivation_counter(contract_id, request_counter) as (
                    select contract_id, max(request_counter)
                    from par_active_contracts
                    where synchronizer_idx = $indexedSynchronizer
                      and change = cast('deactivation' as change_type)
                      and ts <= $beforeAndIncluding
                    group by contract_id
                  )
                    select ac.contract_id, ac.ts, ac.request_counter, ac.change
                    from deactivation_counter dc
                      join par_active_contracts ac on ac.synchronizer_idx = $indexedSynchronizer and ac.contract_id = dc.contract_id
                    where ac.request_counter <= dc.request_counter"""
                    .as[(LfContractId, CantonTimestamp, RequestCounter, ChangeType)],
                  s"$functionFullName: Fetch ACS entries to be pruned",
                )
              )
              totalEntriesPruned <-
                performUnlessClosingUSF("Delete ACS entries batch")(
                  if (acsEntriesToPrune.isEmpty) FutureUnlessShutdown.pure(0)
                  else {
                    val deleteStatement =
                      s"delete from par_active_contracts where synchronizer_idx = ? and contract_id = ? and ts = ?" + " and request_counter = ? and change = CAST(? as change_type);"
                    storage.queryAndUpdate(
                      DbStorage
                        .bulkOperation(deleteStatement, acsEntriesToPrune, storage.profile) { pp =>
                          { case (contractId, ts, rc, change) =>
                            pp >> indexedSynchronizer
                            pp >> contractId
                            pp >> ts
                            pp >> rc
                            pp >> change
                          }
                        }
                        .map(_.sum),
                      s"$functionFullName: Bulk-delete ACS entries",
                    )
                  }
                )
            } yield totalEntriesPruned
          case _: DbStorage.Profile.H2 =>
            performUnlessClosingUSF("ACS.doPrune")(
              storage.queryAndUpdate(
                sqlu"""
            with deactivation_counter(contract_id, request_counter) as (
              select contract_id, max(request_counter)
              from par_active_contracts
              where synchronizer_idx = $indexedSynchronizer
              and change = ${ChangeType.Deactivation}
              and ts <= $beforeAndIncluding
              group by contract_id
            )
		    delete from par_active_contracts
            where (synchronizer_idx, contract_id, ts, request_counter, change) in (
		      select ac.synchronizer_idx, ac.contract_id, ac.ts, ac.request_counter, ac.change
              from deactivation_counter dc
              join par_active_contracts ac on ac.synchronizer_idx = $indexedSynchronizer and ac.contract_id = dc.contract_id
              where ac.request_counter <= dc.request_counter
            );
            """,
                functionFullName,
              )
            )
        }
    } yield nrPruned

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.updateUnlessShutdown_(
      sqlu"delete from par_active_contracts where synchronizer_idx = $indexedSynchronizer",
      functionFullName,
    )

  /* Computes the maximum reassignment counter for each contract in the `res` vector.
     The computation for max_reassignmentCounter(`rc`, `cid`) reuses the result of max_reassignmentCounter(`rc-1`, `cid`).

       Assumption: the input `res` is already sorted by request counter.
   */
  /*
       TODO(i12904): Here we compute the maximum of the previous reassignment counters;
        instead, we could retrieve the reassignment counter of the latest activation
   */
  private def reassignmentCounterForArchivals(
      res: Iterable[(TimeOfChange, LfContractId, ActivenessChangeDetail)]
  ): Map[(RequestCounter, LfContractId), Option[ReassignmentCounter]] =
    res
      .groupBy { case (_, cid, _) => cid }
      .flatMap { case (cid, changes) =>
        val sortedChangesByRc = changes.collect {
          case (TimeOfChange(rc, _), _, change)
              if change.name != ActivenessChangeDetail.unassignment =>
            ((rc, cid), change)
        }.toList

        NonEmpty.from(sortedChangesByRc) match {
          case None => List.empty
          case Some(changes) =>
            val ((rc, cid), op) = changes.head1
            val initial = ((rc, cid), (op.reassignmentCounterO, op))

            changes.tail1.scanLeft(initial) {
              case (
                    ((_, _), (accReassignmentCounter, _)),
                    ((crtRc, cid), change),
                  ) =>
                (
                  (crtRc, cid),
                  (
                    Ordering[Option[ReassignmentCounter]].max(
                      accReassignmentCounter,
                      change.reassignmentCounterO,
                    ),
                    change,
                  ),
                )
            }
        }
      }
      .collect {
        case ((rc, cid), (reassignmentCounter, Archive)) => ((rc, cid), reassignmentCounter)
        case ((rc, cid), (reassignmentCounter, Purge)) => ((rc, cid), reassignmentCounter)
      }

  def deleteSince(
      criterion: RequestCounter
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val query =
      sqlu"delete from par_active_contracts where synchronizer_idx = $indexedSynchronizer and request_counter >= $criterion"
    storage
      .update(query, functionFullName)
      .map(count => logger.debug(s"DeleteSince on $criterion removed at least $count ACS entries"))
  }

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[LazyList[(TimeOfChange, ActiveContractIdsChange)]] = {
    ErrorUtil.requireArgument(
      fromExclusive <= toInclusive,
      s"Provided timestamps are in the wrong order: $fromExclusive and $toInclusive",
    )
    val changeQuery = {
      sql"""select ts, request_counter, contract_id, operation, reassignment_counter, remote_synchronizer_idx
             from par_active_contracts where synchronizer_idx = $indexedSynchronizer and
             ((ts = ${fromExclusive.timestamp} and request_counter > ${fromExclusive.rc}) or ts > ${fromExclusive.timestamp})
             and
             ((ts = ${toInclusive.timestamp} and request_counter <= ${toInclusive.rc}) or ts <= ${toInclusive.timestamp})
             order by ts asc, request_counter asc, change desc"""
    }.as[(TimeOfChange, LfContractId, ActivenessChangeDetail)]

    for {
      retrievedChangesBetween <- storage.query(
        changeQuery,
        operationName = "ACS: get changes between",
      )
      // retrieves the reassignment counters for archived contracts that were activated between (`fromExclusive`, `toInclusive`]
      maxReassignmentCountersPerCidUpToRc = reassignmentCounterForArchivals(retrievedChangesBetween)

      /*
         If there are contracts archived between (`fromExclusive`, `toInclusive`] that have a
         reassignment counter None in maxReassignmentCountersPerCidUpToRc, and the protocol version
         supports reassignment counters, then we need to retrieve the reassignment counters of these
         archived contracts from activations taking place at time <= toInclusive.
       */
      // retrieves the reassignment counters for archived contracts that were activated at time <= `fromExclusive`
      maxReassignmentCountersPerRemainingCidUpToRc <- {
        val archivalsWithoutReassignmentCounters =
          maxReassignmentCountersPerCidUpToRc.filter(_._2.isEmpty)

        NonEmpty
          .from(archivalsWithoutReassignmentCounters.map { case ((_, contractId), _) =>
            contractId
          }.toSeq)
          .fold(
            FutureUnlessShutdown
              .pure(Map.empty[(RequestCounter, LfContractId), Option[ReassignmentCounter]])
          ) { cids =>
            val maximumRc =
              archivalsWithoutReassignmentCounters
                .map { case ((rc, _), _) => rc.unwrap }
                .maxOption
                .getOrElse(RequestCounter.Genesis.unwrap)
            val inClause = DbStorage
              .toInClause("contract_id", cids)(absCoidSetParameter)
            val archivalCidsWithoutReassignmentCountersQueries =
              // Note that the sql query does not filter entries with ts <= toExclusive.timestamp,
              // but it also includes the entries between (`fromExclusive`, `toInclusive`].
              // This is an implementation choice purely to reuse code: we pass the query result into the
              // function `reassignmentCounterForArchivals` and obtain the reassignment counters for (rc, cid) pairs.
              // One could have a more restrictive query and compute the reassignment counters in some other way.
              (sql"""select ts, request_counter, contract_id, operation, reassignment_counter, remote_synchronizer_idx
                   from par_active_contracts where synchronizer_idx = $indexedSynchronizer
                   and (request_counter <= $maximumRc)
                   and (ts <= ${toInclusive.timestamp})
                   and """ ++ inClause ++ sql""" order by ts asc, request_counter asc""")
                .as[(TimeOfChange, LfContractId, ActivenessChangeDetail)]
            val resultArchivalReassignmentCounters = storage
              .query(
                archivalCidsWithoutReassignmentCountersQueries,
                "ACS: get data to compute the reassignment counters for archived contracts",
              )

            resultArchivalReassignmentCounters.map(reassignmentCounterForArchivals)
          }
      }

      res <- combineReassignmentCounters(
        maxReassignmentCountersPerRemainingCidUpToRc = maxReassignmentCountersPerRemainingCidUpToRc,
        maxReassignmentCountersPerCidUpToRc = maxReassignmentCountersPerCidUpToRc,
        retrievedChangesBetween = retrievedChangesBetween,
      )

    } yield res
  }

  private def combineReassignmentCounters(
      maxReassignmentCountersPerRemainingCidUpToRc: Map[
        (RequestCounter, LfContractId),
        Option[ReassignmentCounter],
      ],
      maxReassignmentCountersPerCidUpToRc: Map[(RequestCounter, LfContractId), Option[
        ReassignmentCounter
      ]],
      retrievedChangesBetween: Seq[(TimeOfChange, LfContractId, ActivenessChangeDetail)],
  ): FutureUnlessShutdown[LazyList[(TimeOfChange, ActiveContractIdsChange)]] = {
    // filter None entries from maxReassignmentCountersPerCidUpToRc, as the reassignment counters for
    // those contracts are now in remainingMaxReassignmentCountersPerCidUpToRc
    val definedMaxReassignmentCountersPerCidUpToRc = maxReassignmentCountersPerCidUpToRc.collect {
      case (key, Some(reassignmentCounter)) => (key, reassignmentCounter)
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

                case in: Assignment =>
                  Right((acts :+ (cid, in.toStateChangeType), deacts))
                case out: Unassignment =>
                  Right((acts, deacts :+ (cid, out.toStateChangeType)))

                case add: Add =>
                  Right((acts :+ (cid, add.toStateChangeType), deacts))

                case Archive | Purge =>
                  val reassignmentCounterE = definedMaxReassignmentCountersPerCidUpToRc
                    .get((toc.rc, cid))
                    .orElse(
                      maxReassignmentCountersPerRemainingCidUpToRc.get((toc.rc, cid)).flatten
                    )
                    .toRight(s"Unable to find reassignment counter for $cid at $toc")

                  reassignmentCounterE.map { reassignmentCounter =>
                    val newChange = (
                      cid,
                      StateChangeType(ContractChange.Archived, reassignmentCounter),
                    )

                    (acts, deacts :+ newChange)
                  }
              }
          }

        resE.map { case (acts, deacts) => toc -> ActiveContractIdsChange(acts.toMap, deacts.toMap) }
      }
      .bimap(
        err => FutureUnlessShutdown.failed(new IllegalStateException(err)),
        FutureUnlessShutdown.pure,
      )
      .merge
  }

  override private[participant] def contractCount(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    storage.query(
      sql"select count(distinct contract_id) from par_active_contracts where ts <= $timestamp"
        .as[Int]
        .head,
      functionFullName,
    )

  private def checkReassignmentsConsistency(
      reassignments: Seq[((LfContractId, TimeOfChange), ReassignmentChangeDetail)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] =
    if (enableAdditionalConsistencyChecks) {
      reassignments.parTraverse_ { case ((contractId, toc), reassignment) =>
        for {
          _ <- checkReassignmentCountersShouldIncrease(contractId, toc, reassignment)
          _ <- checkActivationsDeactivationConsistency(contractId, toc)
        } yield ()
      }
    } else CheckedT.pure(())

  private def checkActivationsDeactivationConsistency(
      contractId: LfContractId,
      toc: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {

    val query =
      // change desc allows to have activations first
      sql"""select operation, reassignment_counter, remote_synchronizer_idx, ts, request_counter from par_active_contracts
              where synchronizer_idx = $indexedSynchronizer and contract_id = $contractId
              order by ts asc, request_counter asc, change desc"""

    val changesF: FutureUnlessShutdown[Vector[StoredActiveContract]] =
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

  private def checkReassignmentCountersShouldIncrease(
      contractId: LfContractId,
      toc: TimeOfChange,
      reassignment: ReassignmentChangeDetail,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = CheckedT {

    // latestBefore and earliestAfter are only the latest/earliest w.r.t. what has already been persisted.
    // Given the out-of-order writes to the ActiveContractStore, there may actually be pending writes of changes
    // between "latest/earliest" and the current time of change. Therefore, we check only for monotonicity
    // instead of gap-freedom and we do not mention earliest/latest in the error messages either.
    //
    // By checking both "latest" before / "earliest" after, we cover the case that an out-of-order write of
    // an earlier change has a higher reassignment counter that the current time of change: the "earliest" after
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
        _ <- ActiveContractStore.checkReassignmentCounterAgainstLatestBefore(
          contractId,
          toc,
          reassignment.reassignmentCounter,
          latestBeforeO.map(_.toReassignmentCounterAtChangeInfo),
        )
        _ <- ActiveContractStore.checkReassignmentCounterAgainstEarliestAfter(
          contractId,
          toc,
          reassignment.reassignmentCounter,
          earliestAfterO.map(_.toReassignmentCounterAtChangeInfo),
          reassignment.toReassignmentType,
        )
      } yield ()
    }
  }

  private def bulkInsert(
      contractChanges: Map[(LfContractId, TimeOfChange), ActivenessChangeDetail],
      change: ChangeType,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    val insertQuery = storage.profile match {
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into par_active_contracts(contract_id, ts, request_counter, change, synchronizer_idx, operation, reassignment_counter, remote_synchronizer_idx)
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
        pp >> indexedSynchronizer
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
    ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
      import DbStorage.Implicits.BuilderChain.*

      val (idsToCheck, rcsToCheck, tssToCheck) = unzip(toCheck)

      val cidsInClause =
        DbStorage.toInClause("contract_id", idsToCheck)

      val rcsInClause =
        DbStorage.toInClause("request_counter", rcsToCheck)

      val tssInClause =
        DbStorage.toInClause("ts", tssToCheck)

      val query =
        (sql"select contract_id, operation, reassignment_counter, remote_synchronizer_idx, ts, request_counter from par_active_contracts where synchronizer_idx = $indexedSynchronizer and " ++ cidsInClause ++
          sql" and " ++ tssInClause ++ sql" and " ++ rcsInClause ++ sql" and change = CAST($change as change_type)")
          .as[(LfContractId, ActivenessChangeDetail, TimeOfChange)]

      val isActivation = change == ChangeType.Activation

      val warningsF = storage
        .query(query, functionFullName)
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
        NonEmpty
          .from(contractChanges.keySet.toSeq)
          .map(checkIdempotence)
          .getOrElse(CheckedT.pure(()))
      } else CheckedT.pure(())
    }
  }

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
      sql"""select operation, reassignment_counter, remote_synchronizer_idx, ts, request_counter from par_active_contracts
                          where synchronizer_idx = $indexedSynchronizer and contract_id = $contractId"""
    val opFilterQuery =
      operationFilter.fold(sql" ")(o => sql" and operation = CAST($o as operation_type)")
    val tocFilterQuery = tocFilter.fold(sql" ") { toc =>
      if (descending)
        sql" and (ts < ${toc.timestamp} or (ts = ${toc.timestamp} and request_counter < ${toc.rc}))"
      else
        sql" and (ts > ${toc.timestamp} or (ts = ${toc.timestamp} and request_counter > ${toc.rc}))"
    }
    val (normal_order, reversed_order) = if (descending) ("desc", "asc") else ("asc", "desc")
    val orderQuery =
      sql" order by ts #$normal_order, request_counter #$normal_order, change #$reversed_order #${storage
          .limit(1)}"
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
}
