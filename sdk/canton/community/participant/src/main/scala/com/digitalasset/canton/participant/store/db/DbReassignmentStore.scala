// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.GlobalOffset
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
import com.digitalasset.canton.protocol.{ReassignmentId, SerializableContract}
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.{NoOpeningErrors, SequencedEvent, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.SingletonTraverseOps
import com.digitalasset.canton.util.{
  Checked,
  CheckedT,
  ErrorUtil,
  MonadUtil,
  ReassignmentTag,
  SimpleExecutionQueue,
  SingletonTraverse,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, RequestCounter}
import com.google.common.collect.HashBiMap
import com.google.protobuf.ByteString
import slick.jdbc.TransactionIsolation.Serializable
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class DbReassignmentStore(
    override protected val storage: DbStorage,
    indexedTargetDomain: Target[IndexedDomain],
    indexedStringStore: IndexedStringStore,
    targetDomainProtocolVersion: Target[ProtocolVersion],
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

  private val targetDomainId: Target[DomainId] =
    indexedTargetDomain.map(_.domainId)

  private def indexedDomainF[T[_]: SingletonTraverse](
      domainId: T[DomainId]
  ): Future[T[IndexedDomain]] =
    domainId.traverseSingleton((_, domainId) => IndexedDomain.indexed(indexedStringStore)(domainId))

  private def indexedDomainET[E, T[_]: SingletonTraverse](
      domainId: T[DomainId]
  ): EitherT[Future, E, T[IndexedDomain]] =
    EitherT.right[E](indexedDomainF(domainId))

  private def indexedDomainETUS[T[_]: SingletonTraverse](
      domainId: T[DomainId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, T[IndexedDomain]] =
    EitherT.right[ReassignmentStoreError](
      performUnlessClosingF("index-for-source-domain")(
        indexedDomainF(domainId)
      )
    )

  private def indexedDomainF(
      idx: Int,
      attributeName: String,
  )(implicit
      traceContext: TraceContext
  ): Future[IndexedDomain] =
    IndexedDomain
      .fromDbIndexOT(s"par_reassignments attribute $attributeName", indexedStringStore)(idx)
      .value
      .flatMap {
        case Some(sourceDomainId) => Future.successful(sourceDomainId)
        case None =>
          Future.failed(
            new RuntimeException(
              s"Unable to find domain ID for domain with index $idx"
            )
          )
      }

  private def getResultFullUnassignmentTree(
      sourceDomainProtocolVersion: Source[ProtocolVersion]
  ): GetResult[FullUnassignmentTree] =
    GetResult(r =>
      FullUnassignmentTree
        .fromByteString(cryptoApi, sourceDomainProtocolVersion)(
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
    SerializableContract.getVersionedSetParameter(targetDomainProtocolVersion.unwrap)

  implicit val setParameterReassignmentTagDomainId: SetParameter[DomainId] =
    (d: DomainId, pp: PositionedParameters) => pp >> d.toLengthLimitedString

  implicit val setParameterReassignmentTagDomainIdO
      : SetParameter[Option[ReassignmentTag[DomainId]]] =
    (d: Option[ReassignmentTag[DomainId]], pp: PositionedParameters) =>
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
          r.nextLongOption().map(GlobalOffset.tryFromLong),
          r.nextLongOption().map(GlobalOffset.tryFromLong),
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
      reassignmentData.targetDomain == targetDomainId,
      s"Domain $targetDomainId: Reassignment store cannot store reassignment for domain ${reassignmentData.targetDomain}",
    )
    def insert(dbReassignmentId: DbReassignmentId): DBIO[Int] =
      sqlu"""
        insert into par_reassignments(target_domain_idx, source_domain_idx, unassignment_timestamp, unassignment_request_counter,
        unassignment_request, unassignment_decision_time, contract, unassignment_result,
        submitter_lf, source_protocol_version, unassignment_global_offset, assignment_global_offset)
        values (
          $indexedTargetDomain,
          ${dbReassignmentId.indexedSourceDomain},
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
              target_domain_idx=$indexedTargetDomain and source_domain_idx=${id.indexedSourceDomain} and unassignment_timestamp=${data.unassignmentTs}
          """
      }

      val newEntry = ReassignmentEntry(reassignmentData, None)
      existingEntry.mergeWith(newEntry).map(entry => Some(update(entry)))
    }

    val reassignmentId = reassignmentData.reassignmentId

    for {
      indexedSourceDomain <- indexedDomainETUS(reassignmentId.sourceDomain)
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
  ): EitherT[Future, ReassignmentStore.ReassignmentLookupError, ReassignmentData] = for {
    indexedSourceDomain <- indexedDomainET(reassignmentId.sourceDomain)
    dbReassignmentId = DbReassignmentId(indexedSourceDomain, reassignmentId.unassignmentTs)
    res <- EitherT(
      storage.query(entryExists(dbReassignmentId), functionFullName).map {
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
     from par_reassignments where target_domain_idx=$indexedTargetDomain and source_domain_idx=${id.indexedSourceDomain} and unassignment_timestamp=${id.unassignmentTs}
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
          target_domain_idx=$indexedTargetDomain and source_domain_idx=${id.indexedSourceDomain} and unassignment_timestamp=${id.unassignmentTs}
        """.as[Option[RawDeliveredUnassignmentResult]].headOption

      existsRaw.map(_.map(_.map(_.tryCreateDeliveredUnassignmentResul(cryptoApi))))
    }

    def update(previousResult: Option[DeliveredUnassignmentResult], id: DbReassignmentId) =
      previousResult
        .fold[Checked[ReassignmentStoreError, Nothing, Option[DBIO[Int]]]](
          Checked.result(Some(sqlu"""
              update par_reassignments
              set unassignment_result=$unassignmentResult
              where target_domain_idx=$indexedTargetDomain and source_domain_idx=${id.indexedSourceDomain} and unassignment_timestamp=${id.unassignmentTs}
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
      indexedSourceDomain <- indexedDomainETUS(
        unassignmentResult.reassignmentId.sourceDomain
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
        .map { case (sourceDomainIndex, unassignmentTs) =>
          sql"(source_domain_idx=$sourceDomainIndex and unassignment_timestamp=$unassignmentTs)"
        }
        .intercalate(sql" or ")
        .toActionBuilder

      val query =
        sql"""select source_domain_idx, unassignment_timestamp, unassignment_global_offset, assignment_global_offset
           from par_reassignments
           where
              target_domain_idx=$indexedTargetDomain and (""" ++ reassignmentIdsFilter ++ sql")"

      storage.query(
        query.as[(Int, CantonTimestamp, Option[GlobalOffset], Option[GlobalOffset])],
        functionFullName,
      )
    }

    val updateQuery =
      """update par_reassignments
       set unassignment_global_offset = ?, assignment_global_offset = ?
       where target_domain_idx = ? and source_domain_idx = ? and unassignment_timestamp = ?
    """

    val deduplicatedSourceDomains = offsets.map(_._1.sourceDomain).toSet
    val sourceDomainToIndexBiMap: HashBiMap[Source[DomainId], Int] =
      HashBiMap.create[Source[DomainId], Int]()

    lazy val task = for {
      _ <- MonadUtil.sequentialTraverse(deduplicatedSourceDomains.forgetNE.toSeq)(sd =>
        indexedDomainET(sd).map { indexedDomain =>
          sourceDomainToIndexBiMap.put(sd, indexedDomain.unwrap.index)
        }
      )
      selectData = offsets.map { case (reassignmentId, _) =>
        (sourceDomainToIndexBiMap.get(reassignmentId.sourceDomain), reassignmentId.unassignmentTs)
      }
      selected <- EitherT.right(select(selectData))
      retrievedItems = selected.map {
        case (sourceDomainIndex, unassignmentTs, unassignmentOffset, assignmentOffset) =>
          val sourceDomainId = sourceDomainToIndexBiMap.inverse().get(sourceDomainIndex)

          ReassignmentId(
            sourceDomainId,
            unassignmentTs,
          ) -> (unassignmentOffset, assignmentOffset, sourceDomainIndex)
      }.toMap

      mergedGlobalOffsets <- EitherT.fromEither[Future](offsets.forgetNE.traverse {
        case (reassignmentId, newOffsets) =>
          retrievedItems
            .get(reassignmentId)
            .toRight(UnknownReassignmentId(reassignmentId))
            .map { case (offsetOutO, offsetInO, sourceDomainIndex) =>
              sourceDomainIndex -> ReassignmentGlobalOffset
                .create(offsetOutO, offsetInO)
                .valueOr(err => throw new DbDeserializationException(err))
            }
            .flatMap { case (sourceDomainIndex, globalOffsetO) =>
              globalOffsetO
                .fold[Either[String, ReassignmentGlobalOffset]](Right(newOffsets))(
                  _.merge(newOffsets)
                )
                .leftMap(ReassignmentGlobalOffsetsMerge(reassignmentId, _))
                .map((sourceDomainIndex, reassignmentId.unassignmentTs, _))
            }
      })

      batchUpdate = DbStorage.bulkOperation_(updateQuery, mergedGlobalOffsets, storage.profile) {
        pp => mergedGlobalOffsetWithId =>
          val (sourceDomainIndex, unassignmentTs, mergedGlobalOffset) = mergedGlobalOffsetWithId

          pp >> mergedGlobalOffset.unassignment
          pp >> mergedGlobalOffset.assignment
          pp >> indexedTargetDomain
          pp >> sourceDomainIndex
          pp >> unassignmentTs
      }

      _ <- EitherT.right[ReassignmentStoreError](
        storage.queryAndUpdate(batchUpdate, functionFullName)
      )
    } yield ()

    sequentialQueue.executeE(task, "addReassignmentsOffsets")
  }

  override def completeReassignment(reassignmentId: ReassignmentId, timeOfCompletion: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, Nothing, ReassignmentStoreError, Unit] = {
    def updateSameOrUnset(indexedSourceDomain: Source[IndexedDomain]) =
      sqlu"""
        update par_reassignments
          set time_of_completion_request_counter=${timeOfCompletion.rc}, time_of_completion_timestamp=${timeOfCompletion.timestamp}
        where
          target_domain_idx=$indexedTargetDomain and source_domain_idx=$indexedSourceDomain and unassignment_timestamp=${reassignmentId.unassignmentTs}
          and (time_of_completion_request_counter is NULL
            or (time_of_completion_request_counter = ${timeOfCompletion.rc} and time_of_completion_timestamp = ${timeOfCompletion.timestamp}))
      """

    val doneE: EitherT[Future, ReassignmentStoreError, Unit] = for {
      indexedSourceDomain <- indexedDomainET(reassignmentId.sourceDomain)
      _ <- EitherT(
        storage.update(updateSameOrUnset(indexedSourceDomain), functionFullName).map { changed =>
          if (changed > 0) {
            if (changed != 1)
              logger.error(
                s"Reassignment completion query changed $changed lines. It should only change 1."
              )
            Right(())
          } else {
            if (changed != 0)
              logger.error(
                s"Reassignment completion query changed $changed lines -- this should not be negative."
              )
            Left(
              ReassignmentAlreadyCompleted(reassignmentId, timeOfCompletion): ReassignmentStoreError
            )
          }
        }
      )
    } yield ()

    CheckedT.fromEitherTNonabort((), doneE)
  }

  override def deleteReassignment(
      reassignmentId: ReassignmentId
  )(implicit traceContext: TraceContext): Future[Unit] = for {
    indexedSourceDomain <- indexedDomainF(reassignmentId.sourceDomain)
    _ <- storage.update_(
      sqlu"""delete from par_reassignments
                where target_domain_idx=$indexedTargetDomain and source_domain_idx=$indexedSourceDomain and unassignment_timestamp=${reassignmentId.unassignmentTs}""",
      functionFullName,
    )
  } yield ()

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val query = sqlu"""
       update par_reassignments
         set time_of_completion_request_counter=null, time_of_completion_timestamp=null
         where target_domain_idx=$indexedTargetDomain and time_of_completion_request_counter >= $criterionInclusive
      """
    storage.update_(query, functionFullName)
  }

  private def findPendingBase(
      onlyNotFinished: Boolean
  ) = {
    import DbStorage.Implicits.BuilderChain.*

    val domainFilter = sql"target_domain_idx=$indexedTargetDomain"

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
      filterSource: Option[Source[DomainId]],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[ReassignmentData]] = {
    import DbStorage.Implicits.BuilderChain.*

    val timestampFilter =
      filterTimestamp.fold(sql"")(ts => sql" and unassignment_timestamp=$ts")
    val submitterFilter =
      filterSubmitter.fold(sql"")(submitter => sql" and submitter_lf=$submitter")
    val limitSql = storage.limitSql(limit)
    for {
      indexedSourceDomainO <- filterSource.fold(
        Future.successful(Option.empty[Source[IndexedDomain]])
      )(sd => indexedDomainF(sd).map(Some(_)))
      sourceFilter =
        indexedSourceDomainO.fold(sql"")(indexedSourceDomain =>
          sql" and source_domain_idx=$indexedSourceDomain"
        )
      res <- storage.query(
        (findPendingBase(onlyNotFinished =
          true
        ) ++ sourceFilter ++ timestampFilter ++ submitterFilter ++ limitSql)
          .as[ReassignmentData],
        functionFullName,
      )
    } yield res
  }

  override def findAfter(
      requestAfter: Option[(CantonTimestamp, Source[DomainId])],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[ReassignmentData]] = for {
    queryData <- requestAfter.fold(
      Future.successful(Option.empty[(CantonTimestamp, Source[IndexedDomain])])
    ) { case (ts, sd) =>
      indexedDomainF(sd).map(indexedDomain => Some((ts, indexedDomain)))
    }
    res <- storage.query(
      {
        import DbStorage.Implicits.BuilderChain.*

        val timestampFilter =
          queryData.fold(sql"") { case (requestTimestamp, indexedDomain) =>
            sql" and (unassignment_timestamp, source_domain_idx) > ($requestTimestamp, $indexedDomain) "
          }
        val order = sql" order by unassignment_timestamp, source_domain_idx "
        val limitSql = storage.limitSql(limit)

        (findPendingBase(onlyNotFinished = true) ++ timestampFilter ++ order ++ limitSql)
          .as[ReassignmentData]
      },
      functionFullName,
    )
  } yield res

  private def findIncomplete(
      sourceDomain: Option[Source[DomainId]],
      validAt: GlobalOffset,
      start: Long,
  )(implicit traceContext: TraceContext): Future[Seq[ReassignmentData]] = for {
    indexedSourceDomainO <- sourceDomain.fold(
      Future.successful(Option.empty[Source[IndexedDomain]])
    )(sd => indexedDomainF(sd).map(Some(_)))
    res <- storage
      .query(
        {
          import DbStorage.Implicits.BuilderChain.*

          val outCompleted =
            sql"(unassignment_global_offset is not null and unassignment_global_offset <= $validAt) and (assignment_global_offset is null or assignment_global_offset > $validAt)"
          val inCompleted =
            sql"(assignment_global_offset is not null and assignment_global_offset <= $validAt) and (unassignment_global_offset is null or unassignment_global_offset > $validAt)"
          val incomplete = sql" and (" ++ outCompleted ++ sql" or " ++ inCompleted ++ sql")"

          val sourceDomainFilter =
            indexedSourceDomainO.fold(sql"")(indexedSourceDomain =>
              sql" and source_domain_idx=$indexedSourceDomain"
            )

          val limitSql =
            storage.limitSql(numberOfItems = DbReassignmentStore.dbQueryLimit, skipItems = start)

          val base = findPendingBase(onlyNotFinished = false)

          (base ++ incomplete ++ sourceDomainFilter ++ limitSql).as[ReassignmentData]
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
      queryFrom: (Long, TraceContext) => Future[Seq[ReassignmentData]],
  )(implicit traceContext: TraceContext) = {

    def stakeholderFilter(data: ReassignmentData): Boolean = stakeholders
      .forall(_.exists(data.contract.metadata.stakeholders))

    Monad[Future].tailRecM((Vector.empty[ReassignmentData], 0, 0L)) { case (acc, accSize, start) =>
      val missing = limit.unwrap - accSize

      if (missing <= 0)
        Future.successful(Right(acc))
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
      sourceDomain: Option[Source[DomainId]],
      validAt: GlobalOffset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): Future[Seq[IncompleteReassignmentData]] = {
    val queryFrom = (start: Long, traceContext: TraceContext) =>
      findIncomplete(
        sourceDomain = sourceDomain,
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
  ): Future[Option[(GlobalOffset, ReassignmentId, Target[DomainId])]] =
    for {
      queryResult <- storage
        .query(
          {
            val maxCompletedOffset: SQLActionBuilder =
              sql"""select min(coalesce(assignment_global_offset,${GlobalOffset.MaxValue})),
                  min(coalesce(unassignment_global_offset,${GlobalOffset.MaxValue})),
                  source_domain_idx, unassignment_timestamp
                  from par_reassignments
                  where target_domain_idx=$indexedTargetDomain and (unassignment_global_offset is null or assignment_global_offset is null)
                  group by source_domain_idx, unassignment_timestamp
                  """

            maxCompletedOffset
              .as[(Option[GlobalOffset], Option[GlobalOffset], Int, CantonTimestamp)]
          },
          functionFullName,
        )
      resultWithSourceDomainId <- MonadUtil.sequentialTraverse(queryResult.toList) {
        case (assignmentOffset, unassignmentOffset, domainSourceIndex, unassignmentTs) =>
          indexedDomainF(domainSourceIndex, "source_domain_idx")
            .map(indexedDomain =>
              (
                assignmentOffset,
                unassignmentOffset,
                Source(indexedDomain.domainId),
                unassignmentTs,
              )
            )

      }
      res = resultWithSourceDomainId
        .map { case (in, out, sourceDomainId, unassignmentTs) =>
          (
            (in.toList ++ out.toList).minOption,
            ReassignmentId(sourceDomainId, unassignmentTs),
          )
        }
        .foldLeft(
          (
            GlobalOffset.MaxValue,
            ReassignmentId(
              Source(targetDomainId.unwrap),
              CantonTimestamp.MaxValue,
            ),
          )
        )((acc: (GlobalOffset, ReassignmentId), n) =>
          n match {
            case (Some(o), tid) => if (acc._1 > o) (o, tid) else acc
            case (None, _) => acc
          }
        ) match {
        case (offset, reassignmentId) =>
          if (offset == GlobalOffset.MaxValue) None
          else Some((offset, reassignmentId, targetDomainId))
      }
    } yield res

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
      indexedSourceDomain: Source[IndexedDomain],
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
