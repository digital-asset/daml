// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Monad
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.data.UnassignmentData.ReassignmentGlobalOffset
import com.digitalasset.canton.data.{CantonTimestamp, Offset, UnassignmentData}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.IncompleteReassignmentData.InternalIncompleteReassignmentData
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentData,
  IncompleteReassignmentData,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.store.ReassignmentStore.*
import com.digitalasset.canton.participant.store.db.DbReassignmentStore.{
  DbContracts,
  ReassignmentEntryRaw,
}
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId, ReassignmentId}
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.{DbParameterUtils, DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.{IndexedStringStore, IndexedSynchronizer}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.SingletonTraverseOps
import slick.jdbc.GetResult.GetInt
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.collection.immutable.ArraySeq
import scala.concurrent.ExecutionContext

class DbReassignmentStore(
    override protected val storage: DbStorage,
    indexedTargetSynchronizer: Target[IndexedSynchronizer],
    indexedStringStore: IndexedStringStore,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    batchingConfig: BatchingConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ReassignmentStore
    with DbStore {
  import storage.api.*
  import storage.converters.*

  private val targetSynchronizerId: Target[SynchronizerId] =
    indexedTargetSynchronizer.map(_.synchronizerId)

  private def indexedSynchronizerF[T[_]: SingletonTraverse](
      synchronizerId: T[SynchronizerId]
  ): FutureUnlessShutdown[T[IndexedSynchronizer]] =
    synchronizerId.traverseSingleton((_, synchronizerId) =>
      IndexedSynchronizer.indexed(indexedStringStore)(synchronizerId)
    )

  private def indexedSynchronizerET[E, T[_]: SingletonTraverse](
      synchronizerId: T[SynchronizerId]
  ): EitherT[FutureUnlessShutdown, E, T[IndexedSynchronizer]] =
    EitherT.right[E](indexedSynchronizerF(synchronizerId))

  private def synchronizerIdF(
      idx: Int,
      attributeName: String,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SynchronizerId] =
    IndexedSynchronizer
      .fromDbIndexOT(s"par_reassignments attribute $attributeName", indexedStringStore)(idx)
      .map(_.synchronizerId)
      .getOrElse(
        throw new RuntimeException(
          s"Unable to find synchronizer id for synchronizer with index $idx"
        )
      )

  private implicit val setParameterStorageUnassignmentData: SetParameter[UnassignmentData] =
    UnassignmentData.getVersionedSetParameter

  implicit val setParameterReassignmentTagSynchronizerId: SetParameter[SynchronizerId] =
    (d: SynchronizerId, pp: PositionedParameters) => pp >> d.toLengthLimitedString

  implicit val setParameterReassignmentTagSynchronizerIdO
      : SetParameter[Option[ReassignmentTag[SynchronizerId]]] =
    (d: Option[ReassignmentTag[SynchronizerId]], pp: PositionedParameters) =>
      pp >> d.map(_.unwrap.toLengthLimitedString)

  private implicit val getResultReassignmentGlobalOffset
      : GetResult[Option[ReassignmentGlobalOffset]] = GetResult { r =>
    ReassignmentGlobalOffset
      .create(
        r.nextLongOption().map(Offset.tryFromLong),
        r.nextLongOption().map(Offset.tryFromLong),
      )
      .valueOr(err => throw new DbDeserializationException(err))
  }

  private implicit val setParameterDbContracts: SetParameter[DbContracts] = { (value, pp) =>
    DbParameterUtils.setArrayBytesParameterDb(
      storageProfile = storage.profile,
      items = value.contracts.toArray.sortBy(_.contractId.toString),
      serialize = DbContracts.serializeOne,
      pp = pp,
    )
  }

  private implicit val getDbContracts: GetResult[DbContracts] =
    DbParameterUtils
      .getDataBytesArrayResultsDb[ContractInstance](deserialize = DbContracts.tryDeserializeOne)
      .andThen { arr =>
        val contracts: NonEmpty[Seq[ContractInstance]] = NonEmpty
          .from(ArraySeq.unsafeWrapArray(arr))
          .getOrElse(throw new DbDeserializationException(s"Found empty contract array"))
        DbContracts(contracts)
      }

  private implicit val getResultReassignmentEntryRaw: GetResult[ReassignmentEntryRaw] = GetResult {
    r =>
      ReassignmentEntryRaw(
        sourceSynchronizerIndex = GetResult[Int].apply(r),
        reassignmentId = GetResult[ReassignmentId].apply(r),
        unassignmentTs = GetResult[CantonTimestamp].apply(r),
        contracts = GetResult[DbContracts].apply(r).contracts,
        unassignmentData = GetResult[Option[UnassignmentData]].apply(r),
        reassignmentGlobalOffset = ReassignmentGlobalOffset
          .create(
            r.nextLongOption().map(Offset.tryFromLong),
            r.nextLongOption().map(Offset.tryFromLong),
          )
          .valueOr(err => throw new DbDeserializationException(err)),
        GetResult[Option[CantonTimestamp]].apply(r),
      )
  }

  // Ensure updates of the unassignment/in global offsets are sequential
  private val sequentialQueue = new SimpleExecutionQueue(
    "reassignmment-store-offsets-update",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  override def addUnassignmentData(
      unassignmentData: UnassignmentData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    ErrorUtil.requireArgument(
      unassignmentData.targetPSId.map(_.logical) == targetSynchronizerId,
      s"Synchronizer $targetSynchronizerId: Reassignment store cannot store reassignment for synchronizer ${unassignmentData.targetPSId
          .map(_.logical)}",
    )

    logger.debug(s"Add unassignment request in the store: ${unassignmentData.reassignmentId}")

    def insert(indexedSourceSynchronizer: Source[IndexedSynchronizer]) =
      // TODO(i23636): remove the 'contract' columns
      // once we remove the computation of incomplete reassignments from the reassignmentStore
      storage.profile match {
        case _: Profile.Postgres =>
          sqlu"""insert into par_reassignments as r(target_synchronizer_idx, source_synchronizer_idx, reassignment_id, unassignment_timestamp, unassignment_data, contracts)
        values (
          $indexedTargetSynchronizer,
          $indexedSourceSynchronizer,
          ${unassignmentData.reassignmentId},
          ${unassignmentData.unassignmentTs},
          $unassignmentData,
          ${DbContracts(unassignmentData.contractsBatch.contracts.map(_.contract))}
        )
        on conflict (target_synchronizer_idx, reassignment_id) do update set unassignment_data = $unassignmentData
        where r.target_synchronizer_idx=$indexedTargetSynchronizer and r.reassignment_id=${unassignmentData.reassignmentId} and r.unassignment_data IS NULL;
      """
        case _: Profile.H2 =>
          sqlu"""MERGE INTO par_reassignments using dual
                 on (target_synchronizer_idx=$indexedTargetSynchronizer and reassignment_id=${unassignmentData.reassignmentId})
                 when matched and unassignment_data IS NULL then
                   update set unassignment_data = $unassignmentData
                 when not matched then
                   insert (target_synchronizer_idx, source_synchronizer_idx, reassignment_id, unassignment_timestamp, unassignment_data, contracts)
                   values ($indexedTargetSynchronizer, $indexedSourceSynchronizer, ${unassignmentData.reassignmentId}, ${unassignmentData.unassignmentTs}, $unassignmentData, ${DbContracts(
              unassignmentData.contractsBatch.contracts.map(_.contract)
            )});
  """
      }

    for {
      indexedSourceSynchronizer <- indexedSynchronizerET(
        unassignmentData.sourcePSId.map(_.logical)
      )
      _ <- EitherT.right(
        storage.update(
          insert(indexedSourceSynchronizer),
          functionFullName,
        )
      )
    } yield ()
  }

  /** Inserts fake `unassignmentRequestCounter` into the database. These will be overwritten once
    * the unassignment is completed. If the reassignment data has already been inserted, this method
    * will do nothing.
    */
  def addAssignmentDataIfAbsent(assignmentData: AssignmentData)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    // will be overwritten by the completion of the unassignment

    logger.debug(s"Add assignment data in the store: ${assignmentData.reassignmentId}")
    def insert(indexedSourceSynchronizer: Source[IndexedSynchronizer]) =
      sqlu"""
      insert into par_reassignments(target_synchronizer_idx, source_synchronizer_idx, reassignment_id, unassignment_timestamp,
        unassignment_data, unassignment_global_offset, assignment_global_offset, contracts)
        values (
          $indexedTargetSynchronizer,
          $indexedSourceSynchronizer,
          ${assignmentData.reassignmentId},
          ${assignmentData.unassignmentDecisionTime},
          NULL, -- unassignmentRequest
          NULL, -- unassignment_global_offset
          NULL, -- assignment_global_offset
          ${DbContracts(assignmentData.contracts.contracts.map(_.contract))}
        )
          ON conflict do nothing;
          """

    for {
      indexedSourceSynchronizer <- indexedSynchronizerET(
        assignmentData.sourceSynchronizer
      )
      _ <- EitherT.right(
        storage.update(
          insert(indexedSourceSynchronizer),
          functionFullName,
        )
      )
    } yield ()
  }

  override def lookup(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStore.ReassignmentLookupError, UnassignmentData] = {
    logger.debug(s"Looking up reassignment $reassignmentId in store")
    findReassignmentEntry(reassignmentId).flatMap {
      case ReassignmentEntry(_, _, _, None, _, _, None) =>
        EitherT.leftT(AssignmentStartingBeforeUnassignment(reassignmentId))
      case ReassignmentEntry(_, _, _, _, _, _, Some(tsCompletion)) =>
        EitherT.leftT(ReassignmentCompleted(reassignmentId, tsCompletion))
      case ReassignmentEntry(
            _reassignmentId,
            _sourceSynchronizer,
            _contract,
            Some(unassignmentData),
            _reassignmentGlobalOffset,
            _unassignmentTs,
            _,
          ) =>
        EitherT.rightT(unassignmentData)
    }
  }

  private def entryExists(
      reassignmentId: ReassignmentId
  ): DbAction.ReadOnly[Option[ReassignmentEntryRaw]] =
    sql"""
     select source_synchronizer_idx, reassignment_id, unassignment_timestamp, contracts, unassignment_data,
     unassignment_global_offset, assignment_global_offset, assignment_timestamp
     from par_reassignments
     where target_synchronizer_idx=$indexedTargetSynchronizer and reassignment_id=$reassignmentId
    """.as[ReassignmentEntryRaw].headOption

  def addReassignmentsOffsets(offsets: Map[ReassignmentId, ReassignmentGlobalOffset])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
    if (offsets.isEmpty) EitherT.pure[FutureUnlessShutdown, ReassignmentStoreError](())
    else {
      logger.debug(s"Adding reassignment offsets: $offsets")
      MonadUtil.sequentialTraverse_(offsets.toList.grouped(batchingConfig.maxItemsInBatch.unwrap))(
        offsets => addReassignmentsOffsetsInternal(NonEmptyUtil.fromUnsafe(offsets))
      )
    }

  /*
    Requires:
      - reassignment id to be all distinct
      - size of the list be within bounds that allow for single DB queries (use `batchSize`)
   */
  private def addReassignmentsOffsetsInternal(
      offsets: NonEmpty[List[(ReassignmentId, ReassignmentGlobalOffset)]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    import DbStorage.Implicits.BuilderChain.*

    def select(ids: Seq[ReassignmentId]) = {
      val reassignmentIdsFilter = ids
        .map { case reassignmentId => sql"reassignment_id=$reassignmentId" }
        .intercalate(sql" or ")
        .toActionBuilder

      val query =
        sql"""select reassignment_id, unassignment_global_offset, assignment_global_offset
           from par_reassignments
           where
            target_synchronizer_idx=$indexedTargetSynchronizer and (""" ++ reassignmentIdsFilter ++ sql")"

      storage.query(
        query.as[(ReassignmentId, Option[Offset], Option[Offset])],
        functionFullName,
      )
    }

    val updateQuery =
      """update par_reassignments
       set unassignment_global_offset = ?, assignment_global_offset = ?
       where target_synchronizer_idx = ? and reassignment_id = ?
    """

    lazy val task = for {
      selected <- EitherT.right(select(offsets.map(_._1)))
      retrievedItems = selected.map { case (reassignmentId, unassignmentOffset, assignmentOffset) =>
        reassignmentId -> (unassignmentOffset, assignmentOffset)
      }.toMap

      mergedGlobalOffsets <- EitherT.fromEither[FutureUnlessShutdown](offsets.forgetNE.traverse {
        case (reassignmentId, newOffsets) =>
          retrievedItems
            .get(reassignmentId)
            .toRight(UnknownReassignmentId(reassignmentId))
            .map { case (offsetOutO, offsetInO) =>
              ReassignmentGlobalOffset
                .create(offsetOutO, offsetInO)
                .valueOr(err => throw new DbDeserializationException(err))
            }
            .flatMap { case globalOffsetO =>
              globalOffsetO
                .fold[Either[String, ReassignmentGlobalOffset]](Right(newOffsets))(
                  _.merge(newOffsets)
                )
                .leftMap(ReassignmentGlobalOffsetsMerge(reassignmentId, _))
                .map((reassignmentId, _))
            }
      })

      batchUpdate = DbStorage.bulkOperation_(updateQuery, mergedGlobalOffsets, storage.profile)(
        pp => { case (reassignmentId, mergedGlobalOffset) =>
          pp >> mergedGlobalOffset.unassignment
          pp >> mergedGlobalOffset.assignment
          pp >> indexedTargetSynchronizer
          pp >> reassignmentId
        }
      )

      _ <- EitherT.right[ReassignmentStoreError](
        storage.queryAndUpdate(batchUpdate, functionFullName)
      )
    } yield ()

    sequentialQueue.executeEUS(task, "addReassignmentsOffsets")
  }

  override def completeReassignment(reassignmentId: ReassignmentId, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit] = {
    logger.debug(s"Marking reassignment $reassignmentId as completed at $ts")

    val updateSameOrUnset =
      sqlu"""
        update par_reassignments
          set assignment_timestamp=$ts
        where
          target_synchronizer_idx=$indexedTargetSynchronizer and reassignment_id=$reassignmentId
          and (assignment_timestamp is NULL or assignment_timestamp = $ts)
      """

    val doneE = EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit](
      storage.update(updateSameOrUnset, functionFullName).map { changed =>
        if (changed > 0) {
          if (changed != 1)
            logger.error(
              s"Reassignment completion query changed $changed lines. It should only change 1."
            )
          Either.unit
        } else {
          if (changed != 0)
            logger.error(
              s"Reassignment completion query changed $changed lines -- this should not be negative."
            )
          Left(
            ReassignmentAlreadyCompleted(
              reassignmentId,
              ts,
            ): ReassignmentStoreError
          )
        }
      }
    )

    CheckedT.fromEitherTNonabort((), doneE)
  }

  override def deleteReassignment(
      reassignmentId: ReassignmentId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Deleting reassignment $reassignmentId from the store")
    for {
      _ <- storage.update_(
        sqlu"""delete from par_reassignments
                where target_synchronizer_idx=$indexedTargetSynchronizer and reassignment_id=$reassignmentId""",
        functionFullName,
      )
    } yield ()
  }

  override def deleteCompletionsSince(
      criterionInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val query = sqlu"""
       update par_reassignments
         set assignment_timestamp=null
         where target_synchronizer_idx=$indexedTargetSynchronizer and assignment_timestamp >= $criterionInclusive
      """
    storage.update_(query, functionFullName)
  }

  private def findPendingBase(
      onlyNotFinished: Boolean
  ) = {
    import DbStorage.Implicits.BuilderChain.*

    val synchronizerFilter = sql"target_synchronizer_idx=$indexedTargetSynchronizer"

    val notFinishedFilter =
      if (onlyNotFinished)
        sql" and assignment_timestamp is null"
      else sql" "

    val base: SQLActionBuilder = sql"""
     select source_synchronizer_idx, reassignment_id, unassignment_timestamp, contracts, unassignment_data,
     unassignment_global_offset, assignment_global_offset, assignment_timestamp
     from par_reassignments
     where
   """

    base ++ synchronizerFilter ++ notFinishedFilter
  }

  override def findAfter(
      requestAfter: Option[(CantonTimestamp, Source[SynchronizerId])],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[UnassignmentData]] = for {
    queryData <- requestAfter.fold(
      FutureUnlessShutdown.pure(Option.empty[(CantonTimestamp, Source[IndexedSynchronizer])])
    ) { case (ts, sd) =>
      indexedSynchronizerF(sd).map(indexedSynchronizer => Some((ts, indexedSynchronizer)))
    }
    rawEntries <- storage.query(
      {
        import DbStorage.Implicits.BuilderChain.*

        val timestampFilter =
          queryData.fold(sql"") { case (requestTimestamp, indexedSynchronizer) =>
            sql" and (unassignment_timestamp, source_synchronizer_idx) > ($requestTimestamp, $indexedSynchronizer) "
          }
        val unassignmentRequestFilter = sql" and unassignment_data is not null "
        val order = sql" order by unassignment_timestamp, source_synchronizer_idx "
        val limitSql = storage.limitSql(limit)

        (findPendingBase(onlyNotFinished =
          true
        ) ++ unassignmentRequestFilter ++ timestampFilter ++ order ++ limitSql)
          .as[ReassignmentEntryRaw]
      },
      functionFullName,
    )

    entries <- MonadUtil.sequentialTraverse(rawEntries)(raw =>
      synchronizerIdF(raw.sourceSynchronizerIndex, "source_synchronizer_idx").map(
        raw.toReassignmentEntry
      )
    )

    res = entries.flatMap(_.unassignmentData)
  } yield res

  private def findIncomplete(
      sourceSynchronizer: Option[Source[SynchronizerId]],
      validAt: Offset,
      start: Long,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[InternalIncompleteReassignmentData]] =
    for {
      indexedSourceSynchronizerO <- sourceSynchronizer.fold(
        FutureUnlessShutdown.pure(Option.empty[Source[IndexedSynchronizer]])
      )(sd => indexedSynchronizerF(sd).map(Some(_)))
      res <- storage
        .query(
          {
            import DbStorage.Implicits.BuilderChain.*

            val unassignmentCompleted =
              sql"(unassignment_global_offset is not null and unassignment_global_offset <= $validAt) and (assignment_global_offset is null or assignment_global_offset > $validAt)"
            val assignmentCompleted =
              sql"(assignment_global_offset is not null and assignment_global_offset <= $validAt) and (unassignment_global_offset is null or unassignment_global_offset > $validAt)"
            val incomplete =
              sql" and (" ++ unassignmentCompleted ++ sql" or " ++ assignmentCompleted ++ sql")"

            val sourceSynchronizerFilter =
              indexedSourceSynchronizerO.fold(sql"")(indexedSourceSynchronizer =>
                sql" and source_synchronizer_idx=$indexedSourceSynchronizer"
              )

            val limitSql =
              storage.limitSql(numberOfItems = DbReassignmentStore.dbQueryLimit, skipItems = start)

            val base: SQLActionBuilder =
              sql"""select reassignment_id, unassignment_data, contracts, unassignment_global_offset, assignment_global_offset
              from par_reassignments
              where target_synchronizer_idx=$indexedTargetSynchronizer"""

            (base ++ incomplete ++ sourceSynchronizerFilter ++ limitSql)
              .as[
                (
                    ReassignmentId,
                    Option[UnassignmentData],
                    DbContracts,
                    Option[ReassignmentGlobalOffset],
                )
              ]
          },
          functionFullName,
        )

      incompletes = res.map {
        case (
              reassignmentId,
              unassignmentData,
              contract,
              reassignmentGlobalOffset,
            ) =>
          InternalIncompleteReassignmentData(
            reassignmentId,
            unassignmentData,
            reassignmentGlobalOffset,
            contract.contracts,
          )
      }
    } yield incompletes

  /*
    We cannot do the stakeholders filtering in the DB, so we may need to query the
    DB several times in order to be able to return `limit` elements.
    TODO(#11735)
   */
  private def queryWithFiltering(
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
      dbQueryLimit: Int,
      queryFrom: (
          Long,
          TraceContext,
      ) => FutureUnlessShutdown[Seq[InternalIncompleteReassignmentData]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Vector[InternalIncompleteReassignmentData]] = {

    def stakeholderFilter(data: InternalIncompleteReassignmentData): Boolean = {
      val dataStakeholders = data.contracts.flatMap(_.metadata.stakeholders).toSet
      stakeholders.forall(_.exists(dataStakeholders.contains(_)))
    }

    Monad[FutureUnlessShutdown].tailRecM(
      (Vector.empty[InternalIncompleteReassignmentData], 0, 0L)
    ) { case (acc, accSize, start) =>
      val missing = limit.unwrap - accSize

      if (missing <= 0)
        FutureUnlessShutdown.pure(Right(acc))
      else {
        queryFrom(start, traceContext).map { result =>
          val filteredResult = result.filter(stakeholderFilter).take(missing)
          val filteredResultSize = filteredResult.size

          if (result.isEmpty)
            Right(acc)
          else
            Left((acc ++ filteredResult, accSize + filteredResultSize, start + dbQueryLimit))
        }
      }
    }
  }

  override def findIncomplete(
      sourceSynchronizer: Option[Source[SynchronizerId]],
      validAt: Offset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[IncompleteReassignmentData]] = {
    val queryFrom = (start: Long, traceContext: TraceContext) =>
      findIncomplete(
        sourceSynchronizer = sourceSynchronizer,
        validAt = validAt,
        start = start,
      )(traceContext)

    queryWithFiltering(
      stakeholders = stakeholders,
      limit = limit,
      queryFrom = queryFrom,
      dbQueryLimit = DbReassignmentStore.dbQueryLimit,
    ).map(
      _.map(
        _.toIncompleteReassignmentData(validAt)
          .valueOr(err =>
            throw new IllegalArgumentException(s"Unable to create IncompleteReassignmentData: $err")
          )
      )
    ).map(_.sortBy(_.reassignmentEventGlobalOffset.globalOffset))
  }

  override def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[SynchronizerId])]] =
    for {
      queryResult <- storage
        .query(
          sql"""
                  select coalesce(assignment_global_offset, unassignment_global_offset), reassignment_id
                  from par_reassignments
                  where target_synchronizer_idx=$indexedTargetSynchronizer and ((unassignment_global_offset is null) != (assignment_global_offset is null))
                  """
            .as[(Offset, ReassignmentId)],
          functionFullName,
        )
    } yield queryResult
      .minByOption(_._1)
      .map { case (offset, reassignmentId) =>
        (offset, reassignmentId, targetSynchronizerId)
      }

  override def findContractReassignmentId(
      contractIds: Seq[LfContractId],
      sourceSynchronizer: Option[Source[SynchronizerId]],
      unassignmentTs: Option[CantonTimestamp],
      completionTs: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, Seq[ReassignmentId]]] = {
    import DbStorage.Implicits.BuilderChain.*

    for {
      indexedSourceSynchronizerO <- sourceSynchronizer.fold(
        FutureUnlessShutdown.pure(Option.empty[Source[IndexedSynchronizer]])
      )(sd => indexedSynchronizerF(sd).map(Some(_)))

      filterSynchronizers = indexedSourceSynchronizerO match {
        case Some(source) => Some(sql"source_synchronizer_idx=$source")
        case None => None
      }

      filterUnassignmentTs = unassignmentTs match {
        case Some(ts) => Some(sql"unassignment_timestamp=$ts")
        case None => None
      }

      filterCompletionTs = completionTs match {
        case Some(ts) => Some(sql"assignment_timestamp=$ts")
        case None => None
      }

      filterUnassignmentRequest = Some(sql"unassignment_data is not null")

      filter =
        Seq(
          filterSynchronizers,
          filterUnassignmentTs,
          filterCompletionTs,
          filterUnassignmentRequest,
        )
          .filter(_.nonEmpty)
          .collect { case Some(i) => i }
          .intercalate(sql" and ")
          .toActionBuilder

      query =
        sql"""select reassignment_id, unassignment_data from par_reassignments
          where target_synchronizer_idx=$indexedTargetSynchronizer and (""" ++ filter ++ sql")"

      res <- storage
        .query(
          query.as[(ReassignmentId, UnassignmentData)],
          functionFullName,
        )
        .map(
          _.collect {
            case (reassignmentId, unassignmentData)
                if contractIds.exists(unassignmentData.contractsBatch.contractIds.contains(_)) =>
              unassignmentData.contractsBatch.contractIds.map(_ -> reassignmentId)
          }.flatten
            .groupMap { case (cid, _) => cid } { case (_, reassignmentId) => reassignmentId }
        )
    } yield res
  }

  override def findReassignmentEntry(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownReassignmentId, ReassignmentEntry] =
    for {
      entryRaw <- EitherT(
        storage.query(entryExists(reassignmentId), functionFullName).map {
          case None => Left(UnknownReassignmentId(reassignmentId))
          case Some(entry) => Right(entry)
        }
      )
      res <- EitherT.right(
        synchronizerIdF(entryRaw.sourceSynchronizerIndex, "source_synchronizer_idx").map(
          entryRaw.toReassignmentEntry
        )
      )
    } yield res

  override def listInFlightReassignmentIds()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ReassignmentId]] =
    storage.query(
      sql"""SELECT reassignment_id
           FROM par_reassignments
           WHERE target_synchronizer_idx = $indexedTargetSynchronizer AND assignment_timestamp IS NULL"""
        .as[ReassignmentId],
      functionFullName,
    )
}

object DbReassignmentStore {

  /** The data of reassignment as it's in the reassignment store, i.e. withe synchronizerIndex and
    * not the syncrhonizerId
    */
  private final case class ReassignmentEntryRaw(
      sourceSynchronizerIndex: Int,
      reassignmentId: ReassignmentId,
      unassignmentTs: CantonTimestamp,
      contracts: NonEmpty[Seq[ContractInstance]],
      unassignmentData: Option[UnassignmentData],
      reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
      assignmentTs: Option[CantonTimestamp],
  ) {

    def toReassignmentEntry(synchronizerId: SynchronizerId): ReassignmentEntry =
      ReassignmentEntry(
        reassignmentId,
        Source(synchronizerId),
        contracts,
        unassignmentData,
        reassignmentGlobalOffset,
        unassignmentTs,
        assignmentTs,
      )
  }

  // We tend to use 1000 to limit queries
  private val dbQueryLimit = 1000

  import com.google.protobuf.ByteString

  // Used for encoding and decoding the par_reassignments.contracts column, which is an array type.
  private[db] final case class DbContracts(contracts: NonEmpty[Seq[ContractInstance]])
  private[db] object DbContracts {
    def serializeOne(
        contract: ContractInstance
    ): Array[Byte] =
      contract.encoded.toByteArray

    def tryDeserializeOne(bytes: Array[Byte]): ContractInstance =
      ContractInstance
        .decodeWithCreatedAt(ByteString.copyFrom(bytes))
        .valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize contract: $err")
        )
  }

}
