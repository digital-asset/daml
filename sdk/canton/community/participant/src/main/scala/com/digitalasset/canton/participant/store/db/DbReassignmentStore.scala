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
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.IncompleteReassignmentData.InternalIncompleteReassignmentData
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentData.ReassignmentGlobalOffset
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentData,
  IncompleteReassignmentData,
  UnassignmentData,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.store.ReassignmentStore.*
import com.digitalasset.canton.participant.store.db.DbReassignmentStore.{
  DbContracts,
  DbReassignmentId,
  ReassignmentEntryRaw,
}
import com.digitalasset.canton.protocol.{
  LfContractId,
  ReassignmentId,
  SerializableContract,
  UnassignId,
}
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.{DbParameterUtils, DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.{IndexedStringStore, IndexedSynchronizer}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.SingletonTraverseOps
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.google.common.collect.HashBiMap
import com.google.protobuf.ByteString
import slick.jdbc.GetResult.GetInt
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.collection.immutable.ArraySeq
import scala.concurrent.ExecutionContext

class DbReassignmentStore(
    override protected val storage: DbStorage,
    indexedTargetSynchronizer: Target[IndexedSynchronizer],
    indexedStringStore: IndexedStringStore,
    targetSynchronizerProtocolVersion: Target[ProtocolVersion],
    cryptoApi: CryptoPureApi,
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

  private def parseFullUnassignmentTree(
      bytes: Array[Byte]
  ) =
    FullUnassignmentTree
      .fromByteString(cryptoApi, Source(ProtocolVersionValidation.NoValidation))(
        ByteString.copyFrom(bytes)
      )
      .valueOr(error =>
        throw new DbDeserializationException(
          s"Error deserializing unassignment request $error"
        )
      )

  implicit val getResultFullUnassignmentTree: GetResult[FullUnassignmentTree] =
    GetResult(r => parseFullUnassignmentTree(r.nextBytes()))

  implicit val getResultFullUnassignmentTreeO: GetResult[Option[FullUnassignmentTree]] =
    GetResult(r => r.nextBytesOption().map(parseFullUnassignmentTree))

  private implicit val setResultFullUnassignmentTree: SetParameter[FullUnassignmentTree] =
    (r: FullUnassignmentTree, pp: PositionedParameters) => pp >> r.toByteString.toByteArray

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
      serialize = DbContracts.serializeOne(targetSynchronizerProtocolVersion.unwrap),
      pp = pp,
    )
  }

  private implicit val getDbContracts: GetResult[DbContracts] =
    DbParameterUtils
      .getDataBytesArrayResultsDb[SerializableContract](deserialize = DbContracts.tryDeserializeOne)
      .andThen { arr =>
        val contracts: NonEmpty[Seq[SerializableContract]] = NonEmpty
          .from(ArraySeq.unsafeWrapArray(arr))
          .getOrElse(throw new DbDeserializationException(s"Found empty contract array"))
        DbContracts(contracts)
      }

  private implicit val getResultReassignmentEntryRaw: GetResult[ReassignmentEntryRaw] = GetResult {
    r =>
      ReassignmentEntryRaw(
        sourceSynchronizerIndex = GetResult[Int].apply(r),
        unassignId = GetResult[UnassignId].apply(r),
        unassignmentTs = GetResult[CantonTimestamp].apply(r),
        contracts = GetResult[DbContracts].apply(r).contracts,
        unassignmentRequest = getResultFullUnassignmentTreeO.apply(r),
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
      unassignmentData.targetSynchronizer.map(_.logical) == targetSynchronizerId,
      s"Synchronizer $targetSynchronizerId: Reassignment store cannot store reassignment for synchronizer ${unassignmentData.targetSynchronizer
          .map(_.logical)}",
    )

    logger.debug(s"Add unassignment request in the store: ${unassignmentData.reassignmentId}")

    def insert(dbReassignmentId: DbReassignmentId) =
      // TODO(i23636): remove the 'contract' columns
      // once we remove the computation of incomplete reassignments from the reassignmentStore
      storage.profile match {
        case _: Profile.Postgres =>
          sqlu"""insert into par_reassignments as r(target_synchronizer_idx, source_synchronizer_idx, unassign_id, unassignment_timestamp, unassignment_request, contracts)
        values (
          $indexedTargetSynchronizer,
          ${dbReassignmentId.indexedSourceSynchronizer},
          ${dbReassignmentId.unassignId},
          ${unassignmentData.unassignmentTs},
          ${unassignmentData.unassignmentRequest},
          ${DbContracts(unassignmentData.contracts.contracts.map(_.contract))}
        )
        on conflict (target_synchronizer_idx, source_synchronizer_idx, unassign_id) do update set unassignment_request = ${unassignmentData.unassignmentRequest}
        where r.target_synchronizer_idx=$indexedTargetSynchronizer and r.source_synchronizer_idx=${dbReassignmentId.indexedSourceSynchronizer} and r.unassign_id=${dbReassignmentId.unassignId} and r.unassignment_request IS NULL;
      """
        case _: Profile.H2 =>
          sqlu"""MERGE INTO par_reassignments using dual
                 on (target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=${dbReassignmentId.indexedSourceSynchronizer} and unassign_id=${dbReassignmentId.unassignId})
                 when matched and unassignment_request IS NULL then
                   update set unassignment_request = ${unassignmentData.unassignmentRequest}
                 when not matched then
                   insert (target_synchronizer_idx, source_synchronizer_idx, unassign_id, unassignment_timestamp, unassignment_request, contracts)
                   values ($indexedTargetSynchronizer, ${dbReassignmentId.indexedSourceSynchronizer}, ${dbReassignmentId.unassignId}, ${unassignmentData.unassignmentTs}, ${unassignmentData.unassignmentRequest}, ${DbContracts(
              unassignmentData.contracts.contracts.map(_.contract)
            )});
  """
      }

    val reassignmentId = unassignmentData.reassignmentId

    for {
      indexedSourceSynchronizer <- indexedSynchronizerET(reassignmentId.sourceSynchronizer)
      dbReassignmentId = DbReassignmentId(indexedSourceSynchronizer, reassignmentId.unassignId)
      _ <- EitherT.right(
        storage.update(
          insert(dbReassignmentId),
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
      insert into par_reassignments(target_synchronizer_idx, source_synchronizer_idx, unassign_id, unassignment_timestamp,
        unassignment_request, unassignment_global_offset, assignment_global_offset, contracts)
        values (
          $indexedTargetSynchronizer,
          $indexedSourceSynchronizer,
          ${assignmentData.reassignmentId.unassignId},
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
        assignmentData.reassignmentId.sourceSynchronizer
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
      case ReassignmentEntry(_, _, None, _, _, None) =>
        EitherT.leftT(AssignmentStartingBeforeUnassignment(reassignmentId))
      case ReassignmentEntry(_, _, _, _, _, Some(tsCompletion)) =>
        EitherT.leftT(ReassignmentCompleted(reassignmentId, tsCompletion))
      case ReassignmentEntry(
            reassignmentId,
            _contract,
            Some(unassignmentRequest),
            _reassignmentGlobalOffset,
            unassignmentTs,
            _,
          ) =>
        EitherT.rightT(
          UnassignmentData(
            reassignmentId,
            unassignmentRequest,
            unassignmentTs,
          )
        )
    }
  }

  private def entryExists(id: DbReassignmentId): DbAction.ReadOnly[Option[ReassignmentEntryRaw]] =
    sql"""
     select source_synchronizer_idx, unassign_id, unassignment_timestamp, contracts, unassignment_request,
     unassignment_global_offset, assignment_global_offset, assignment_timestamp
     from par_reassignments
     where target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=${id.indexedSourceSynchronizer}
     and unassign_id=${id.unassignId}
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

    def select(ids: Seq[(Int, UnassignId)]) = {
      val reassignmentIdsFilter = ids
        .map { case (sourceSynchronizerIndex, unassignId) =>
          sql"(source_synchronizer_idx=$sourceSynchronizerIndex and unassign_id=$unassignId)"
        }
        .intercalate(sql" or ")
        .toActionBuilder

      val query =
        sql"""select source_synchronizer_idx, unassign_id, unassignment_global_offset, assignment_global_offset
           from par_reassignments
           where
              target_synchronizer_idx=$indexedTargetSynchronizer and (""" ++ reassignmentIdsFilter ++ sql")"

      storage.query(
        query.as[(Int, UnassignId, Option[Offset], Option[Offset])],
        functionFullName,
      )
    }

    val updateQuery =
      """update par_reassignments
       set unassignment_global_offset = ?, assignment_global_offset = ?
       where target_synchronizer_idx = ? and source_synchronizer_idx = ? and unassign_id = ?
    """

    val deduplicatedSourceSynchronizers = offsets.map(_._1.sourceSynchronizer).toSet
    val sourceSynchronizerToIndexBiMap: HashBiMap[Source[SynchronizerId], Int] =
      HashBiMap.create[Source[SynchronizerId], Int]()

    lazy val task = for {
      _ <- MonadUtil.sequentialTraverse(deduplicatedSourceSynchronizers.forgetNE.toSeq)(sd =>
        indexedSynchronizerET(sd).map { indexedSynchronizer =>
          sourceSynchronizerToIndexBiMap.put(sd, indexedSynchronizer.unwrap.index)
        }
      )
      selectData = offsets.map { case (reassignmentId, _) =>
        (
          sourceSynchronizerToIndexBiMap.get(reassignmentId.sourceSynchronizer),
          reassignmentId.unassignId,
        )
      }
      selected <- EitherT.right(select(selectData))
      retrievedItems = selected.map {
        case (sourceSynchronizerIndex, unassignId, unassignmentOffset, assignmentOffset) =>
          val sourceSynchronizerId =
            sourceSynchronizerToIndexBiMap.inverse().get(sourceSynchronizerIndex)

          ReassignmentId(
            sourceSynchronizerId,
            unassignId,
          ) -> (unassignmentOffset, assignmentOffset, sourceSynchronizerIndex)
      }.toMap

      mergedGlobalOffsets <- EitherT.fromEither[FutureUnlessShutdown](offsets.forgetNE.traverse {
        case (reassignmentId, newOffsets) =>
          retrievedItems
            .get(reassignmentId)
            .toRight(UnknownReassignmentId(reassignmentId))
            .map { case (offsetOutO, offsetInO, sourceSynchronizerIndex) =>
              sourceSynchronizerIndex -> ReassignmentGlobalOffset
                .create(offsetOutO, offsetInO)
                .valueOr(err => throw new DbDeserializationException(err))
            }
            .flatMap { case (sourceSynchronizerIndex, globalOffsetO) =>
              globalOffsetO
                .fold[Either[String, ReassignmentGlobalOffset]](Right(newOffsets))(
                  _.merge(newOffsets)
                )
                .leftMap(ReassignmentGlobalOffsetsMerge(reassignmentId, _))
                .map((sourceSynchronizerIndex, reassignmentId.unassignId, _))
            }
      })

      batchUpdate = DbStorage.bulkOperation_(updateQuery, mergedGlobalOffsets, storage.profile) {
        pp => mergedGlobalOffsetWithId =>
          val (sourceSynchronizerIndex, unassignId, mergedGlobalOffset) =
            mergedGlobalOffsetWithId

          pp >> mergedGlobalOffset.unassignment
          pp >> mergedGlobalOffset.assignment
          pp >> indexedTargetSynchronizer
          pp >> sourceSynchronizerIndex
          pp >> unassignId
      }

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

    def updateSameOrUnset(indexedSourceSynchronizer: Source[IndexedSynchronizer]) =
      sqlu"""
        update par_reassignments
          set assignment_timestamp=$ts
        where
          target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=$indexedSourceSynchronizer and unassign_id=${reassignmentId.unassignId}
          and (assignment_timestamp is NULL or assignment_timestamp = $ts)
      """

    val doneE: EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = for {
      indexedSourceSynchronizer <- indexedSynchronizerET(reassignmentId.sourceSynchronizer)
      _ <- EitherT(
        storage.update(updateSameOrUnset(indexedSourceSynchronizer), functionFullName).map {
          changed =>
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
    } yield ()

    CheckedT.fromEitherTNonabort((), doneE)
  }

  override def deleteReassignment(
      reassignmentId: ReassignmentId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Deleting reassignment $reassignmentId from the store")
    for {
      indexedSourceSynchronizer: Source[IndexedSynchronizer] <- indexedSynchronizerF(
        reassignmentId.sourceSynchronizer
      )
      _ <- storage.update_(
        sqlu"""delete from par_reassignments
                where target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=$indexedSourceSynchronizer and unassign_id=${reassignmentId.unassignId}""",
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
     select source_synchronizer_idx, unassign_id, unassignment_timestamp, contracts, unassignment_request,
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
        val unassignmentRequestFilter = sql" and unassignment_request is not null "
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

    res = entries.flatMap(_.unassignmentDataO)
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
              sql"""select source_synchronizer_idx, unassign_id, unassignment_request, contracts, unassignment_global_offset, assignment_global_offset
              from par_reassignments
              where target_synchronizer_idx=$indexedTargetSynchronizer"""

            (base ++ incomplete ++ sourceSynchronizerFilter ++ limitSql)
              .as[
                (
                    Int,
                    UnassignId,
                    Option[FullUnassignmentTree],
                    DbContracts,
                    Option[ReassignmentGlobalOffset],
                )
              ]
          },
          functionFullName,
        )

      incompletes <- MonadUtil.sequentialTraverse(res) {
        case (
              synchronizerIndex,
              unassignId,
              unassignmentRequest,
              contract,
              reassignmentGlobalOffset,
            ) =>
          synchronizerIdF(synchronizerIndex, "source_synchronizer_idx").map(sourceSynchronizer =>
            InternalIncompleteReassignmentData(
              ReassignmentId(Source(sourceSynchronizer), unassignId),
              unassignmentRequest,
              reassignmentGlobalOffset,
              contract.contracts,
            )
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
                  select min(coalesce(assignment_global_offset, unassignment_global_offset)), source_synchronizer_idx, unassign_id
                  from par_reassignments
                  where target_synchronizer_idx=$indexedTargetSynchronizer and ((unassignment_global_offset is null) != (assignment_global_offset is null))
                  group by source_synchronizer_idx, unassign_id
                  """
            .as[(Offset, Int, UnassignId)],
          functionFullName,
        )
      resultWithSourceSynchronizerId <- MonadUtil.sequentialTraverse(queryResult.toList) {
        case (offset, synchronizerSourceIndex, unassignId) =>
          synchronizerIdF(synchronizerSourceIndex, "source_synchronizer_idx")
            .map(synchronizerId => (offset, ReassignmentId(Source(synchronizerId), unassignId)))

      }
    } yield (resultWithSourceSynchronizerId: Seq[(Offset, ReassignmentId)])
      .minByOption(_._1)
      .map { case (offset, reassignmentId) => (offset, reassignmentId, targetSynchronizerId) }

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

      filterUnassignmentRequest = Some(sql"unassignment_request is not null")

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
        sql"select source_synchronizer_idx, unassign_id, unassignment_request from par_reassignments where 1=1 and (" ++ filter ++ sql")"

      res <- storage
        .query(
          query.as[(Int, UnassignId, FullUnassignmentTree)],
          functionFullName,
        )
        .map(
          _.collect {
            case (sourceForeachEntryIdx, unassignId, unassignmentRequest)
                if contractIds.exists(unassignmentRequest.contracts.contractIds.contains(_)) =>
              synchronizerIdF(sourceForeachEntryIdx, "source_synchronizer_idx").map(
                synchronizerId =>
                  unassignmentRequest.contracts.contractIds.map(
                    _ -> ReassignmentId(
                      Source(synchronizerId),
                      unassignId,
                    )
                  )
              )
          }.sequence
            .map(_.flatten)
            .map(_.groupBy(_._1).map { case (id, value) => id -> value.map(_._2) })
        )
    } yield res
  }.flatten

  override def findReassignmentEntry(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownReassignmentId, ReassignmentEntry] =
    for {
      indexedSourceSynchronizer <- indexedSynchronizerET(reassignmentId.sourceSynchronizer)
      dbReassignmentId = DbReassignmentId(indexedSourceSynchronizer, reassignmentId.unassignId)
      entryRaw <- EitherT(
        storage.query(entryExists(dbReassignmentId), functionFullName).map {
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
    for {
      ids <- storage.query(
        sql"""SELECT source_synchronizer_idx, unassign_id
             FROM par_reassignments
             WHERE target_synchronizer_idx = $indexedTargetSynchronizer AND assignment_timestamp IS NULL"""
          .as[(Int, UnassignId)],
        functionFullName,
      )
      reassignmentIds <- MonadUtil.sequentialTraverse(ids) { case (synchronizerIndex, unassignId) =>
        synchronizerIdF(synchronizerIndex, "source_synchronizer_idx").map(synchronizerId =>
          ReassignmentId(Source(synchronizerId), unassignId)
        )
      }
    } yield reassignmentIds

}

object DbReassignmentStore {

  private final case class DbReassignmentId(
      indexedSourceSynchronizer: Source[IndexedSynchronizer],
      unassignId: UnassignId,
  )

  /** The data of reassignment as it's in the reassignment store, i.e. withe synchronizerIndex and
    * not the syncrhonizerId
    */
  private final case class ReassignmentEntryRaw(
      sourceSynchronizerIndex: Int,
      unassignId: UnassignId,
      unassignmentTs: CantonTimestamp,
      contracts: NonEmpty[Seq[SerializableContract]],
      unassignmentRequest: Option[FullUnassignmentTree],
      reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
      assignmentTs: Option[CantonTimestamp],
  ) {

    def toReassignmentEntry(synchronizerId: SynchronizerId): ReassignmentEntry =
      ReassignmentEntry(
        ReassignmentId(Source(synchronizerId), unassignId),
        contracts,
        unassignmentRequest,
        reassignmentGlobalOffset,
        unassignmentTs,
        assignmentTs,
      )
  }

  // We tend to use 1000 to limit queries
  private val dbQueryLimit = 1000

  // Used for encoding and decoding the par_reassignments.contracts column, which is an array type.
  private[db] final case class DbContracts(contracts: NonEmpty[Seq[SerializableContract]])
  private[db] object DbContracts {
    def serializeOne(protocolVersion: ProtocolVersion)(
        contract: SerializableContract
    ): Array[Byte] =
      contract.toByteArray(protocolVersion)

    def tryDeserializeOne(bytes: Array[Byte]): SerializableContract =
      SerializableContract
        .fromTrustedByteArray(bytes)
        .valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize contract: $err")
        )
  }

}
