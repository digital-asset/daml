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
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree, Offset}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentData.ReassignmentGlobalOffset
import com.digitalasset.canton.participant.protocol.reassignment.{
  IncompleteReassignmentData,
  ReassignmentData,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.store.ReassignmentStore.*
import com.digitalasset.canton.participant.store.db.DbReassignmentStore.{
  DbReassignmentId,
  RawDeliveredUnassignmentResult,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId, SerializableContract}
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.{NoOpeningErrors, SequencedEvent, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.{IndexedStringStore, IndexedSynchronizer}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.SingletonTraverseOps
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, RequestCounter}
import com.google.common.collect.HashBiMap
import com.google.protobuf.ByteString
import slick.jdbc.TransactionIsolation.Serializable
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class DbReassignmentStore(
    override protected val storage: DbStorage,
    indexedTargetSynchronizer: Target[IndexedSynchronizer],
    indexedStringStore: IndexedStringStore,
    targetSynchronizerProtocolVersion: Target[ProtocolVersion],
    cryptoApi: CryptoPureApi,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    // TODO(#9270) clean up how we parameterize our nodes
    batchSize: Int = 500,
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

  private def indexedSynchronizerF(
      idx: Int,
      attributeName: String,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[IndexedSynchronizer] =
    IndexedSynchronizer
      .fromDbIndexOT(s"par_reassignments attribute $attributeName", indexedStringStore)(idx)
      .getOrElse(
        throw new RuntimeException(
          s"Unable to find synchronizer id for domain with index $idx"
        )
      )

  private def getResultFullUnassignmentTree(
      sourceSynchronizerProtocolVersion: Source[ProtocolVersion]
  ): GetResult[FullUnassignmentTree] =
    GetResult(r =>
      FullUnassignmentTree
        .fromByteString(cryptoApi, sourceSynchronizerProtocolVersion)(
          ByteString.copyFrom(r.<<[Array[Byte]])
        )
        .fold[FullUnassignmentTree](
          error =>
            throw new DbDeserializationException(
              s"Error deserializing unassignment request $error"
            ),
          Predef.identity,
        )
    )

  private implicit val setResultFullUnassignmentTree: SetParameter[FullUnassignmentTree] =
    (r: FullUnassignmentTree, pp: PositionedParameters) => pp >> r.toByteString.toByteArray

  private implicit val setParameterSerializableContract: SetParameter[SerializableContract] =
    SerializableContract.getVersionedSetParameter(targetSynchronizerProtocolVersion.unwrap)

  implicit val setParameterReassignmentTagSynchronizerId: SetParameter[SynchronizerId] =
    (d: SynchronizerId, pp: PositionedParameters) => pp >> d.toLengthLimitedString

  implicit val setParameterReassignmentTagSynchronizerIdO
      : SetParameter[Option[ReassignmentTag[SynchronizerId]]] =
    (d: Option[ReassignmentTag[SynchronizerId]], pp: PositionedParameters) =>
      pp >> d.map(_.unwrap.toLengthLimitedString)

  private implicit val getResultOptionRawDeliveredUnassignmentResult
      : GetResult[Option[RawDeliveredUnassignmentResult]] = GetResult { r =>
    r.nextBytesOption().map { bytes =>
      RawDeliveredUnassignmentResult(bytes, GetResult[ProtocolVersion].apply(r))
    }
  }

  private def getResultDeliveredUnassignmentResult(
      sourceProtocolVersion: Source[ProtocolVersion]
  ): GetResult[Option[DeliveredUnassignmentResult]] =
    GetResult(r =>
      r.nextBytesOption().map { bytes =>
        DbReassignmentStore
          .tryCreateDeliveredUnassignmentResult(cryptoApi)(bytes, sourceProtocolVersion)
      }
    )

  private implicit val setParameterDeliveredUnassignmentResult
      : SetParameter[DeliveredUnassignmentResult] =
    (r: DeliveredUnassignmentResult, pp: PositionedParameters) => pp >> r.result.toByteArray

  private implicit val setParameterOptionDeliveredUnassignmentResult
      : SetParameter[Option[DeliveredUnassignmentResult]] =
    (r: Option[DeliveredUnassignmentResult], pp: PositionedParameters) =>
      pp >> r.map(_.result.toByteArray)

  private implicit val getResultReassignmentData: GetResult[ReassignmentData] = GetResult { r =>
    val sourceProtocolVersion = Source(GetResult[ProtocolVersion].apply(r))

    ReassignmentData(
      sourceProtocolVersion = sourceProtocolVersion,
      unassignmentTs = GetResult[CantonTimestamp].apply(r),
      unassignmentRequestCounter = GetResult[RequestCounter].apply(r),
      unassignmentRequest = getResultFullUnassignmentTree(sourceProtocolVersion).apply(r),
      unassignmentDecisionTime = GetResult[CantonTimestamp].apply(r),
      contract = GetResult[SerializableContract].apply(r),
      unassignmentResult = getResultDeliveredUnassignmentResult(sourceProtocolVersion).apply(r),
      reassignmentGlobalOffset = ReassignmentGlobalOffset
        .create(
          r.nextLongOption().map(Offset.tryFromLong),
          r.nextLongOption().map(Offset.tryFromLong),
        )
        .valueOr(err => throw new DbDeserializationException(err)),
    )
  }

  private implicit val getResultReassignmentEntry: GetResult[ReassignmentEntry] = GetResult(r =>
    ReassignmentEntry(
      getResultReassignmentData(r),
      GetResult[Option[TimeOfChange]].apply(r),
    )
  )

  /*
   Used to ensure updates of the unassignment/in global offsets are sequential
   Note: this safety could be removed as the callers of `addReassignmentsOffsets` are the multi-domain event log and the
   `InFlightSubmissionTracker` which both call this sequentially.
   */
  private val sequentialQueue = new SimpleExecutionQueue(
    "reassignmment-store-offsets-update",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  override def addReassignment(
      reassignmentData: ReassignmentData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {

    ErrorUtil.requireArgument(
      reassignmentData.targetSynchronizer == targetSynchronizerId,
      s"Domain $targetSynchronizerId: Reassignment store cannot store reassignment for domain ${reassignmentData.targetSynchronizer}",
    )
    def insert(dbReassignmentId: DbReassignmentId): DBIO[Int] =
      sqlu"""
        insert into par_reassignments(target_synchronizer_idx, source_synchronizer_idx, unassignment_timestamp, unassignment_request_counter,
        unassignment_request, unassignment_decision_time, contract, unassignment_result,
        submitter_lf, source_protocol_version, unassignment_global_offset, assignment_global_offset)
        values (
          $indexedTargetSynchronizer,
          ${dbReassignmentId.indexedSourceSynchronizer},
          ${dbReassignmentId.unassignmentTs},
          ${reassignmentData.unassignmentRequestCounter},
          ${reassignmentData.unassignmentRequest},
          ${reassignmentData.unassignmentDecisionTime},
          ${reassignmentData.contract},
          ${reassignmentData.unassignmentResult},
          ${reassignmentData.unassignmentRequest.submitter},
          ${reassignmentData.sourceProtocolVersion},
          ${reassignmentData.unassignmentGlobalOffset},
          ${reassignmentData.assignmentGlobalOffset}
        )
      """

    def insertExisting(
        existingEntry: ReassignmentEntry,
        id: DbReassignmentId,
    ): Checked[ReassignmentStoreError, ReassignmentAlreadyCompleted, Option[DBIO[Int]]] = {
      def update(entry: ReassignmentEntry): DBIO[Int] = {
        val data = entry.reassignmentData
        sqlu"""
          update par_reassignments
          set unassignment_request_counter=${data.unassignmentRequestCounter},
            unassignment_request=${data.unassignmentRequest}, unassignment_decision_time=${data.unassignmentDecisionTime},
            contract=${data.contract},
            unassignment_result=${data.unassignmentResult}, submitter_lf=${data.unassignmentRequest.submitter},
            source_protocol_version=${data.sourceProtocolVersion},
            unassignment_global_offset=${data.unassignmentGlobalOffset}, assignment_global_offset=${data.assignmentGlobalOffset}
           where
              target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=${id.indexedSourceSynchronizer} and unassignment_timestamp=${data.unassignmentTs}
          """
      }

      val newEntry = ReassignmentEntry(reassignmentData, None)
      existingEntry.mergeWith(newEntry).map(entry => Some(update(entry)))
    }

    val reassignmentId = reassignmentData.reassignmentId

    for {
      indexedSourceDomain <- indexedSynchronizerET(reassignmentId.sourceSynchronizer)
      dbReassignmentId = DbReassignmentId(indexedSourceDomain, reassignmentId.unassignmentTs)
      _ <- insertDependentDeprecated(
        dbReassignmentId,
        entryExists,
        insertExisting,
        insert,
        dbError => throw dbError,
      )
        .map(_ => ())
        .toEitherT
    } yield ()
  }

  override def lookup(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStore.ReassignmentLookupError, ReassignmentData] =
    for {
      indexedSourceDomain <- indexedSynchronizerET(reassignmentId.sourceSynchronizer)
      dbReassignmentId = DbReassignmentId(indexedSourceDomain, reassignmentId.unassignmentTs)
      res <- EitherT(
        storage.queryUnlessShutdown(entryExists(dbReassignmentId), functionFullName).map {
          case None => Left(UnknownReassignmentId(reassignmentId))
          case Some(ReassignmentEntry(_, Some(timeOfCompletion))) =>
            Left(ReassignmentCompleted(reassignmentId, timeOfCompletion))
          case Some(reassignmentEntry) => Right(reassignmentEntry.reassignmentData)
        }
      )
    } yield res

  private def entryExists(id: DbReassignmentId): DbAction.ReadOnly[Option[ReassignmentEntry]] =
    sql"""
     select source_protocol_version, unassignment_timestamp, unassignment_request_counter, unassignment_request, unassignment_decision_time,
     contract, unassignment_result, unassignment_global_offset, assignment_global_offset,
     time_of_completion_request_counter, time_of_completion_timestamp
     from par_reassignments where target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=${id.indexedSourceSynchronizer} and unassignment_timestamp=${id.unassignmentTs}
    """.as[ReassignmentEntry].headOption

  override def addUnassignmentResult(
      unassignmentResult: DeliveredUnassignmentResult
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    def exists(id: DbReassignmentId) = {
      val existsRaw: DbAction.ReadOnly[Option[Option[RawDeliveredUnassignmentResult]]] = sql"""
       select unassignment_result, source_protocol_version
       from par_reassignments
       where
          target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=${id.indexedSourceSynchronizer} and unassignment_timestamp=${id.unassignmentTs}
        """.as[Option[RawDeliveredUnassignmentResult]].headOption

      existsRaw.map(_.map(_.map(_.tryCreateDeliveredUnassignmentResul(cryptoApi))))
    }

    def update(previousResult: Option[DeliveredUnassignmentResult], id: DbReassignmentId) =
      previousResult
        .fold[Checked[ReassignmentStoreError, Nothing, Option[DBIO[Int]]]](
          Checked.result(Some(sqlu"""
              update par_reassignments
              set unassignment_result=$unassignmentResult
              where target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=${id.indexedSourceSynchronizer} and unassignment_timestamp=${id.unassignmentTs}
              """))
        )(previous =>
          if (previous == unassignmentResult) Checked.result(None)
          else
            Checked.abort(
              UnassignmentResultAlreadyExists(
                unassignmentResult.reassignmentId,
                previous,
                unassignmentResult,
              )
            )
        )

    for {
      indexedSourceDomain <- indexedSynchronizerET(
        unassignmentResult.reassignmentId.sourceSynchronizer
      )
      dbReassignmentId = DbReassignmentId(
        indexedSourceDomain,
        unassignmentResult.reassignmentId.unassignmentTs,
      )
      _ <-
        updateDependentDeprecated(
          dbReassignmentId,
          exists,
          update,
          _ => Checked.abort(UnknownReassignmentId(unassignmentResult.reassignmentId)),
          dbError => throw dbError,
        )
          .map(_ => ())
          .toEitherT
    } yield ()
  }

  def addReassignmentsOffsets(offsets: Map[ReassignmentId, ReassignmentGlobalOffset])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
    if (offsets.isEmpty) EitherT.pure[FutureUnlessShutdown, ReassignmentStoreError](())
    else
      MonadUtil.sequentialTraverse_(offsets.toList.grouped(batchSize))(offsets =>
        addReassignmentsOffsetsInternal(NonEmptyUtil.fromUnsafe(offsets))
      )

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

    def select(ids: Seq[(Int, CantonTimestamp)]) = {
      val reassignmentIdsFilter = ids
        .map { case (sourceSynchronizerIndex, unassignmentTs) =>
          sql"(source_synchronizer_idx=$sourceSynchronizerIndex and unassignment_timestamp=$unassignmentTs)"
        }
        .intercalate(sql" or ")
        .toActionBuilder

      val query =
        sql"""select source_synchronizer_idx, unassignment_timestamp, unassignment_global_offset, assignment_global_offset
           from par_reassignments
           where
              target_synchronizer_idx=$indexedTargetSynchronizer and (""" ++ reassignmentIdsFilter ++ sql")"

      storage.query(
        query.as[(Int, CantonTimestamp, Option[Offset], Option[Offset])],
        functionFullName,
      )
    }

    val updateQuery =
      """update par_reassignments
       set unassignment_global_offset = ?, assignment_global_offset = ?
       where target_synchronizer_idx = ? and source_synchronizer_idx = ? and unassignment_timestamp = ?
    """

    val deduplicatedSourceDomains = offsets.map(_._1.sourceSynchronizer).toSet
    val sourceSynchronizerToIndexBiMap: HashBiMap[Source[SynchronizerId], Int] =
      HashBiMap.create[Source[SynchronizerId], Int]()

    lazy val task = for {
      _ <- MonadUtil.sequentialTraverse(deduplicatedSourceDomains.forgetNE.toSeq)(sd =>
        indexedSynchronizerET(sd).map { indexedDomain =>
          sourceSynchronizerToIndexBiMap.put(sd, indexedDomain.unwrap.index)
        }
      )
      selectData = offsets.map { case (reassignmentId, _) =>
        (
          sourceSynchronizerToIndexBiMap.get(reassignmentId.sourceSynchronizer),
          reassignmentId.unassignmentTs,
        )
      }
      selected <- EitherT.right(select(selectData))
      retrievedItems = selected.map {
        case (sourceSynchronizerIndex, unassignmentTs, unassignmentOffset, assignmentOffset) =>
          val sourceSynchronizerId =
            sourceSynchronizerToIndexBiMap.inverse().get(sourceSynchronizerIndex)

          ReassignmentId(
            sourceSynchronizerId,
            unassignmentTs,
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
                .map((sourceSynchronizerIndex, reassignmentId.unassignmentTs, _))
            }
      })

      batchUpdate = DbStorage.bulkOperation_(updateQuery, mergedGlobalOffsets, storage.profile) {
        pp => mergedGlobalOffsetWithId =>
          val (sourceSynchronizerIndex, unassignmentTs, mergedGlobalOffset) =
            mergedGlobalOffsetWithId

          pp >> mergedGlobalOffset.unassignment
          pp >> mergedGlobalOffset.assignment
          pp >> indexedTargetSynchronizer
          pp >> sourceSynchronizerIndex
          pp >> unassignmentTs
      }

      _ <- EitherT.right[ReassignmentStoreError](
        storage.queryAndUpdateUnlessShutdown(batchUpdate, functionFullName)
      )
    } yield ()

    sequentialQueue.executeEUS(task, "addReassignmentsOffsets")
  }

  override def completeReassignment(reassignmentId: ReassignmentId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit] = {
    logger.debug(s"Marking reassignment $reassignmentId as completed at $toc")

    def updateSameOrUnset(indexedSourceSynchronizer: Source[IndexedSynchronizer]) =
      sqlu"""
        update par_reassignments
          set time_of_completion_request_counter=${toc.rc}, time_of_completion_timestamp=${toc.timestamp}
        where
          target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=$indexedSourceSynchronizer and unassignment_timestamp=${reassignmentId.unassignmentTs}
          and (time_of_completion_request_counter is NULL
            or (time_of_completion_request_counter = ${toc.rc} and time_of_completion_timestamp = ${toc.timestamp}))
      """

    val doneE: EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = for {
      indexedSourceDomain <- indexedSynchronizerET(reassignmentId.sourceSynchronizer)
      _ <- EitherT(
        storage.updateUnlessShutdown(updateSameOrUnset(indexedSourceDomain), functionFullName).map {
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
                  toc,
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
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = for {
    indexedSourceDomain <- indexedSynchronizerF(reassignmentId.sourceSynchronizer)
    _ <- storage.updateUnlessShutdown_(
      sqlu"""delete from par_reassignments
                where target_synchronizer_idx=$indexedTargetSynchronizer and source_synchronizer_idx=$indexedSourceDomain and unassignment_timestamp=${reassignmentId.unassignmentTs}""",
      functionFullName,
    )
  } yield ()

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val query = sqlu"""
       update par_reassignments
         set time_of_completion_request_counter=null, time_of_completion_timestamp=null
         where target_synchronizer_idx=$indexedTargetSynchronizer and time_of_completion_request_counter >= $criterionInclusive
      """
    storage.update_(query, functionFullName)
  }

  private def findPendingBase(
      onlyNotFinished: Boolean
  ) = {
    import DbStorage.Implicits.BuilderChain.*

    val domainFilter = sql"target_synchronizer_idx=$indexedTargetSynchronizer"

    val notFinishedFilter = if (onlyNotFinished)
      sql" and time_of_completion_request_counter is null and time_of_completion_timestamp is null"
    else sql" "

    val base: SQLActionBuilder = sql"""
     select source_protocol_version, unassignment_timestamp, unassignment_request_counter, unassignment_request, unassignment_decision_time,
     contract, unassignment_result, unassignment_global_offset, assignment_global_offset
     from par_reassignments
     where
   """

    base ++ domainFilter ++ notFinishedFilter
  }

  override def find(
      filterSource: Option[Source[SynchronizerId]],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[ReassignmentData]] = {
    import DbStorage.Implicits.BuilderChain.*

    val timestampFilter =
      filterTimestamp.fold(sql"")(ts => sql" and unassignment_timestamp=$ts")
    val submitterFilter =
      filterSubmitter.fold(sql"")(submitter => sql" and submitter_lf=$submitter")
    val limitSql = storage.limitSql(limit)
    for {
      indexedSourceSynchronizerO <- filterSource.fold(
        FutureUnlessShutdown.pure(Option.empty[Source[IndexedSynchronizer]])
      )(sd => indexedSynchronizerF(sd).map(Some(_)))
      sourceFilter =
        indexedSourceSynchronizerO.fold(sql"")(indexedSourceDomain =>
          sql" and source_synchronizer_idx=$indexedSourceDomain"
        )
      res <- storage.queryUnlessShutdown(
        (findPendingBase(onlyNotFinished =
          true
        ) ++ sourceFilter ++ timestampFilter ++ submitterFilter ++ limitSql)
          .as[ReassignmentData],
        functionFullName,
      )
    } yield res
  }

  override def findAfter(
      requestAfter: Option[(CantonTimestamp, Source[SynchronizerId])],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[ReassignmentData]] = for {
    queryData <- requestAfter.fold(
      FutureUnlessShutdown.pure(Option.empty[(CantonTimestamp, Source[IndexedSynchronizer])])
    ) { case (ts, sd) =>
      indexedSynchronizerF(sd).map(indexedDomain => Some((ts, indexedDomain)))
    }
    res <- storage.queryUnlessShutdown(
      {
        import DbStorage.Implicits.BuilderChain.*

        val timestampFilter =
          queryData.fold(sql"") { case (requestTimestamp, indexedDomain) =>
            sql" and (unassignment_timestamp, source_synchronizer_idx) > ($requestTimestamp, $indexedDomain) "
          }
        val order = sql" order by unassignment_timestamp, source_synchronizer_idx "
        val limitSql = storage.limitSql(limit)

        (findPendingBase(onlyNotFinished = true) ++ timestampFilter ++ order ++ limitSql)
          .as[ReassignmentData]
      },
      functionFullName,
    )
  } yield res

  private def findIncomplete(
      sourceSynchronizer: Option[Source[SynchronizerId]],
      validAt: Offset,
      start: Long,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[ReassignmentData]] = for {
    indexedSourceSynchronizerO <- sourceSynchronizer.fold(
      FutureUnlessShutdown.pure(Option.empty[Source[IndexedSynchronizer]])
    )(sd => indexedSynchronizerF(sd).map(Some(_)))
    res <- storage
      .queryUnlessShutdown(
        {
          import DbStorage.Implicits.BuilderChain.*

          val outCompleted =
            sql"(unassignment_global_offset is not null and unassignment_global_offset <= $validAt) and (assignment_global_offset is null or assignment_global_offset > $validAt)"
          val inCompleted =
            sql"(assignment_global_offset is not null and assignment_global_offset <= $validAt) and (unassignment_global_offset is null or unassignment_global_offset > $validAt)"
          val incomplete = sql" and (" ++ outCompleted ++ sql" or " ++ inCompleted ++ sql")"

          val sourceSynchronizerFilter =
            indexedSourceSynchronizerO.fold(sql"")(indexedSourceDomain =>
              sql" and source_synchronizer_idx=$indexedSourceDomain"
            )

          val limitSql =
            storage.limitSql(numberOfItems = DbReassignmentStore.dbQueryLimit, skipItems = start)

          val base = findPendingBase(onlyNotFinished = false)

          (base ++ incomplete ++ sourceSynchronizerFilter ++ limitSql).as[ReassignmentData]
        },
        functionFullName,
      )
  } yield res

  /*
    We cannot do the stakeholders filtering in the DB, so we may need to query the
    DB several times in order to be able to return `limit` elements.
    TODO(#11735)
   */
  private def queryWithFiltering(
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
      dbQueryLimit: Int,
      queryFrom: (Long, TraceContext) => FutureUnlessShutdown[Seq[ReassignmentData]],
  )(implicit traceContext: TraceContext) = {

    def stakeholderFilter(data: ReassignmentData): Boolean = stakeholders
      .forall(_.exists(data.contract.metadata.stakeholders))

    Monad[FutureUnlessShutdown].tailRecM((Vector.empty[ReassignmentData], 0, 0L)) {
      case (acc, accSize, start) =>
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
      _.map(IncompleteReassignmentData.tryCreate(_, validAt))
        .sortBy(_.reassignmentEventGlobalOffset.globalOffset)
    )
  }

  override def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[SynchronizerId])]] =
    for {
      queryResult <- storage
        .queryUnlessShutdown(
          {
            val maxCompletedOffset: SQLActionBuilder =
              sql"""select min(coalesce(assignment_global_offset,${Offset.MaxValue})),
                  min(coalesce(unassignment_global_offset,${Offset.MaxValue})),
                  source_synchronizer_idx, unassignment_timestamp
                  from par_reassignments
                  where target_synchronizer_idx=$indexedTargetSynchronizer and (unassignment_global_offset is null or assignment_global_offset is null)
                  group by source_synchronizer_idx, unassignment_timestamp
                  """

            maxCompletedOffset
              .as[(Option[Offset], Option[Offset], Int, CantonTimestamp)]
          },
          functionFullName,
        )
      resultWithSourceSynchronizerId <- MonadUtil.sequentialTraverse(queryResult.toList) {
        case (assignmentOffset, unassignmentOffset, domainSourceIndex, unassignmentTs) =>
          indexedSynchronizerF(domainSourceIndex, "source_synchronizer_idx")
            .map(indexedDomain =>
              (
                assignmentOffset,
                unassignmentOffset,
                Source(indexedDomain.synchronizerId),
                unassignmentTs,
              )
            )

      }
      res = resultWithSourceSynchronizerId
        .map { case (in, out, sourceSynchronizerId, unassignmentTs) =>
          (
            (in.toList ++ out.toList).minOption,
            ReassignmentId(sourceSynchronizerId, unassignmentTs),
          )
        }
        .foldLeft(
          (
            Offset.MaxValue,
            ReassignmentId(
              Source(targetSynchronizerId.unwrap),
              CantonTimestamp.MaxValue,
            ),
          )
        )((acc: (Offset, ReassignmentId), n) =>
          n match {
            case (Some(o), tid) => if (acc._1 > o) (o, tid) else acc
            case (None, _) => acc
          }
        ) match {
        case (offset, reassignmentId) =>
          if (offset == Offset.MaxValue) None
          else Some((offset, reassignmentId, targetSynchronizerId))
      }
    } yield res

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

      filterDomains = indexedSourceSynchronizerO match {
        case Some(source) => Some(sql"source_synchronizer_idx=$source")
        case None => None
      }

      filterUnassignmentTs = unassignmentTs match {
        case Some(ts) => Some(sql"unassignment_timestamp=$ts")
        case None => None
      }

      filterCompletionTs = completionTs match {
        case Some(ts) => Some(sql"time_of_completion_timestamp=$ts")
        case None => None
      }

      filter =
        Seq(filterDomains, filterUnassignmentTs, filterCompletionTs)
          .filter(_.nonEmpty)
          .collect { case Some(i) => i }
          .intercalate(sql" and ")
          .toActionBuilder

      query =
        sql"select source_synchronizer_idx, unassignment_timestamp, contract from par_reassignments where 1=1 and (" ++ filter ++ sql")"

      res <- storage
        .queryUnlessShutdown(
          query.as[(Int, CantonTimestamp, SerializableContract)],
          functionFullName,
        )
        .map(_.collect {
          case (sourceForeachEntryIdx, unassignTs, contract)
              if contractIds.contains(contract.contractId) =>
            indexedSynchronizerF(sourceForeachEntryIdx, "source_synchronizer_idx").map(
              sourceSynchronizer =>
                contract.contractId -> ReassignmentId(
                  Source(sourceSynchronizer.synchronizerId),
                  unassignTs,
                )
            )
        }.sequence.map(_.groupBy(_._1).map { case (id, value) => id -> value.map(_._2) }))
    } yield res
  }.flatten

  private def insertDependentDeprecated[E, W, A, R](
      dbReassignmentId: DbReassignmentId,
      exists: DbReassignmentId => DBIO[Option[A]],
      insertExisting: (A, DbReassignmentId) => Checked[E, W, Option[DBIO[R]]],
      insertFresh: DbReassignmentId => DBIO[R],
      errorHandler: Throwable => E,
      operationName: String = "insertDependentDeprecated",
  )(implicit traceContext: TraceContext): CheckedT[FutureUnlessShutdown, E, W, Option[R]] =
    updateDependentDeprecated(
      dbReassignmentId,
      exists,
      insertExisting,
      dbReassignmentId => Checked.result(Some(insertFresh(dbReassignmentId))),
      errorHandler,
      operationName,
    )

  private def updateDependentDeprecated[E, W, A, R](
      dbReassignmentId: DbReassignmentId,
      exists: DbReassignmentId => DBIO[Option[A]],
      insertExisting: (A, DbReassignmentId) => Checked[E, W, Option[DBIO[R]]],
      insertNonExisting: DbReassignmentId => Checked[E, W, Option[DBIO[R]]],
      errorHandler: Throwable => E,
      operationName: String = "updateDependentDeprecated",
  )(implicit traceContext: TraceContext): CheckedT[FutureUnlessShutdown, E, W, Option[R]] = {
    import DbStorage.Implicits.*
    import storage.api.{DBIO as _, *}

    val readAndInsert =
      exists(dbReassignmentId)
        .flatMap(existing =>
          existing
            .fold(insertNonExisting(dbReassignmentId))(insertExisting(_, dbReassignmentId))
            .traverse {
              case None => DBIO.successful(None): DBIO[Option[R]]
              case Some(action) => action.map(Some(_)): DBIO[Option[R]]
            }
        )
    val compoundAction = readAndInsert.transactionally.withTransactionIsolation(Serializable)

    val result = storage.queryAndUpdateUnlessShutdown(compoundAction, operationName = operationName)

    CheckedT(result.recover[Checked[E, W, Option[R]]] { case NonFatal(x) =>
      UnlessShutdown.Outcome(Checked.abort(errorHandler(x)))
    })
  }
}

object DbReassignmentStore {

  private final case class DbReassignmentId(
      indexedSourceSynchronizer: Source[IndexedSynchronizer],
      unassignmentTs: CantonTimestamp,
  )

  // We tend to use 1000 to limit queries
  private val dbQueryLimit = 1000

  /*
    This class is a helper to deserialize DeliveredUnassignmentResult because its deserialization
    depends on the ProtocolVersion of the source domain.
   */
  final case class RawDeliveredUnassignmentResult(
      result: Array[Byte],
      sourceProtocolVersion: ProtocolVersion,
  ) {
    def tryCreateDeliveredUnassignmentResul(
        cryptoApi: CryptoPureApi
    ): DeliveredUnassignmentResult =
      tryCreateDeliveredUnassignmentResult(cryptoApi)(
        bytes = result,
        sourceProtocolVersion = Source(sourceProtocolVersion),
      )
  }

  private def tryCreateDeliveredUnassignmentResult(cryptoApi: CryptoPureApi)(
      bytes: Array[Byte],
      sourceProtocolVersion: Source[ProtocolVersion],
  ) = {
    val res: ParsingResult[DeliveredUnassignmentResult] = for {
      signedContent <- SignedContent
        .fromTrustedByteArray(bytes)
        .flatMap(
          _.deserializeContent(
            SequencedEvent.fromByteStringOpen(cryptoApi, sourceProtocolVersion.unwrap)
          )
        )
      result <- DeliveredUnassignmentResult
        .create(NoOpeningErrors(signedContent))
        .leftMap(err => OtherError(err.toString))
    } yield result

    res.fold(
      error =>
        throw new DbDeserializationException(
          s"Error deserializing delivered unassignment result $error"
        ),
      identity,
    )
  }
}
