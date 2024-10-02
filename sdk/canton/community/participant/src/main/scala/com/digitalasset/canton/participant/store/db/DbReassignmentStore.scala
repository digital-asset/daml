// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Monad
import cats.data.EitherT
import cats.syntax.either.*
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
import com.digitalasset.canton.participant.store.db.DbReassignmentStore.RawDeliveredUnassignmentResult
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.{NoOpeningErrors, SequencedEvent, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Reassignment.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, RequestCounter}
import com.google.protobuf.ByteString
import slick.jdbc.TransactionIsolation.Serializable
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class DbReassignmentStore(
    override protected val storage: DbStorage,
    domain: TargetDomainId,
    targetDomainProtocolVersion: TargetProtocolVersion,
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

  private def getResultFullUnassignmentTree(
      sourceDomainProtocolVersion: SourceProtocolVersion
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
    SerializableContract.getVersionedSetParameter(targetDomainProtocolVersion.v)

  private implicit val getResultOptionRawDeliveredUnassignmentResult
      : GetResult[Option[RawDeliveredUnassignmentResult]] = GetResult { r =>
    r.nextBytesOption().map { bytes =>
      RawDeliveredUnassignmentResult(bytes, GetResult[ProtocolVersion].apply(r))
    }
  }

  private def getResultDeliveredUnassignmentResult(
      sourceProtocolVersion: SourceProtocolVersion
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
    val sourceProtocolVersion = SourceProtocolVersion(GetResult[ProtocolVersion].apply(r))

    ReassignmentData(
      sourceProtocolVersion = sourceProtocolVersion,
      unassignmentTs = GetResult[CantonTimestamp].apply(r),
      unassignmentRequestCounter = GetResult[RequestCounter].apply(r),
      unassignmentRequest = getResultFullUnassignmentTree(sourceProtocolVersion).apply(r),
      unassignmentDecisionTime = GetResult[CantonTimestamp].apply(r),
      contract = GetResult[SerializableContract].apply(r),
      creatingTransactionId = GetResult[TransactionId].apply(r),
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
      reassignmentData.targetDomain == domain,
      s"Domain ${domain.unwrap}: Reassignment store cannot store reassignment for domain ${reassignmentData.targetDomain.unwrap}",
    )

    val reassignmentId: ReassignmentId = reassignmentData.reassignmentId
    val newEntry = ReassignmentEntry(reassignmentData, None)

    import DbStorage.Implicits.*
    val insert: DBIO[Int] = sqlu"""
        insert into par_reassignments(target_domain, origin_domain, unassignment_timestamp, unassignment_request_counter,
        unassignment_request, unassignment_decision_time, contract, creating_transaction_id, unassignment_result,
        submitter_lf, source_protocol_version, unassignment_global_offset, assignment_global_offset)
        values (
          $domain,
          ${reassignmentId.sourceDomain},
          ${reassignmentId.unassignmentTs},
          ${reassignmentData.unassignmentRequestCounter},
          ${reassignmentData.unassignmentRequest},
          ${reassignmentData.unassignmentDecisionTime},
          ${reassignmentData.contract},
          ${reassignmentData.creatingTransactionId},
          ${reassignmentData.unassignmentResult},
          ${reassignmentData.unassignmentRequest.submitter},
          ${reassignmentData.sourceProtocolVersion},
          ${reassignmentData.unassignmentGlobalOffset},
          ${reassignmentData.assignmentGlobalOffset}
        )
      """

    def insertExisting(
        existingEntry: ReassignmentEntry
    ): Checked[ReassignmentStoreError, ReassignmentAlreadyCompleted, Option[DBIO[Int]]] = {
      def update(entry: ReassignmentEntry): DBIO[Int] = {
        val id = entry.reassignmentData.reassignmentId
        val data = entry.reassignmentData
        sqlu"""
          update par_reassignments
          set unassignment_request_counter=${data.unassignmentRequestCounter},
            unassignment_request=${data.unassignmentRequest}, unassignment_decision_time=${data.unassignmentDecisionTime},
            contract=${data.contract}, creating_transaction_id=${data.creatingTransactionId},
            unassignment_result=${data.unassignmentResult}, submitter_lf=${data.unassignmentRequest.submitter},
            source_protocol_version=${data.sourceProtocolVersion},
            unassignment_global_offset=${data.unassignmentGlobalOffset}, assignment_global_offset=${data.assignmentGlobalOffset}
           where
              target_domain=$domain and origin_domain=${id.sourceDomain} and unassignment_timestamp=${data.unassignmentTs}
          """
      }
      existingEntry.mergeWith(newEntry).map(entry => Some(update(entry)))
    }

    insertDependentDeprecated(
      entryExists(reassignmentId),
      insertExisting,
      insert,
      dbError => throw dbError,
    )
      .map(_ => ())
      .toEitherT
  }

  override def lookup(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentStore.ReassignmentLookupError, ReassignmentData] =
    EitherT(storage.query(entryExists(reassignmentId), functionFullName).map {
      case None => Left(UnknownReassignmentId(reassignmentId))
      case Some(ReassignmentEntry(_, Some(timeOfCompletion))) =>
        Left(ReassignmentCompleted(reassignmentId, timeOfCompletion))
      case Some(reassignmentEntry) => Right(reassignmentEntry.reassignmentData)
    })

  private def entryExists(id: ReassignmentId): DbAction.ReadOnly[Option[ReassignmentEntry]] = sql"""
     select source_protocol_version, unassignment_timestamp, unassignment_request_counter, unassignment_request, unassignment_decision_time,
     contract, creating_transaction_id, unassignment_result, unassignment_global_offset, assignment_global_offset,
     time_of_completion_request_counter, time_of_completion_timestamp
     from par_reassignments where target_domain=$domain and origin_domain=${id.sourceDomain} and unassignment_timestamp=${id.unassignmentTs}
    """.as[ReassignmentEntry].headOption

  override def addUnassignmentResult(
      unassignmentResult: DeliveredUnassignmentResult
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    val reassignmentId = unassignmentResult.reassignmentId

    val existsRaw: DbAction.ReadOnly[Option[Option[RawDeliveredUnassignmentResult]]] = sql"""
       select unassignment_result, source_protocol_version
       from par_reassignments
       where
          target_domain=$domain and origin_domain=${reassignmentId.sourceDomain} and unassignment_timestamp=${reassignmentId.unassignmentTs}
        """.as[Option[RawDeliveredUnassignmentResult]].headOption

    val exists = existsRaw.map(_.map(_.map(_.tryCreateDeliveredUnassignmentResul(cryptoApi))))

    def update(previousResult: Option[DeliveredUnassignmentResult]) =
      previousResult
        .fold[Checked[ReassignmentStoreError, Nothing, Option[DBIO[Int]]]](
          Checked.result(Some(sqlu"""
              update par_reassignments
              set unassignment_result=$unassignmentResult
              where target_domain=$domain and origin_domain=${reassignmentId.sourceDomain} and unassignment_timestamp=${reassignmentId.unassignmentTs}
              """))
        )(previous =>
          if (previous == unassignmentResult) Checked.result(None)
          else
            Checked.abort(
              UnassignmentResultAlreadyExists(reassignmentId, previous, unassignmentResult)
            )
        )

    updateDependentDeprecated(
      exists,
      update,
      Checked.abort(UnknownReassignmentId(reassignmentId)),
      dbError => throw dbError,
    )
      .map(_ => ())
      .toEitherT
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

    val reassignmentIdsFilter = offsets
      .map { case (reassignmentId, _) =>
        sql"(origin_domain=${reassignmentId.sourceDomain} and unassignment_timestamp=${reassignmentId.unassignmentTs})"
      }
      .forgetNE
      .intercalate(sql" or ")
      .toActionBuilder

    val select =
      sql"""select origin_domain, unassignment_timestamp, unassignment_global_offset, assignment_global_offset
           from par_reassignments
           where
              target_domain=$domain and (""" ++ reassignmentIdsFilter ++ sql")"

    val updateQuery =
      """update par_reassignments
       set unassignment_global_offset = ?, assignment_global_offset = ?
       where target_domain = ? and origin_domain = ? and unassignment_timestamp = ?
    """

    lazy val task = for {
      res <- EitherT.right(
        storage.query(
          select.as[(ReassignmentId, Option[GlobalOffset], Option[GlobalOffset])],
          functionFullName,
        )
      )
      retrievedItems = res.map { case (reassignmentId, out, in) =>
        reassignmentId -> (out, in)
      }.toMap

      mergedGlobalOffsets <- EitherT.fromEither[Future](offsets.forgetNE.traverse {
        case (reassignmentId, newOffsets) =>
          retrievedItems
            .get(reassignmentId)
            .toRight(UnknownReassignmentId(reassignmentId))
            .map { case (offsetOutO, offsetInO) =>
              ReassignmentGlobalOffset
                .create(offsetOutO, offsetInO)
                .valueOr(err => throw new DbDeserializationException(err))
            }
            .flatMap(
              _.fold[Either[String, ReassignmentGlobalOffset]](Right(newOffsets))(
                _.merge(newOffsets)
              )
                .leftMap(ReassignmentGlobalOffsetsMerge(reassignmentId, _))
                .map((reassignmentId, _))
            )
      })

      batchUpdate = DbStorage.bulkOperation_(updateQuery, mergedGlobalOffsets, storage.profile) {
        pp => mergedGlobalOffsetWithId =>
          val (reassignmentId, mergedGlobalOffset) = mergedGlobalOffsetWithId

          pp >> mergedGlobalOffset.unassignment
          pp >> mergedGlobalOffset.assignment
          pp >> domain.unwrap
          pp >> reassignmentId.sourceDomain.unwrap
          pp >> reassignmentId.unassignmentTs
      }

      _ <- EitherT.right[ReassignmentStoreError](
        storage.queryAndUpdate(batchUpdate, functionFullName)
      )
    } yield ()

    sequentialQueue.executeE(task, "addReassignmentsOffsets")
  }

  override def completeReasignment(reassignmentId: ReassignmentId, timeOfCompletion: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, Nothing, ReassignmentStoreError, Unit] = {

    val updateSameOrUnset = sqlu"""
        update par_reassignments
          set time_of_completion_request_counter=${timeOfCompletion.rc}, time_of_completion_timestamp=${timeOfCompletion.timestamp}
        where
          target_domain=$domain and origin_domain=${reassignmentId.sourceDomain} and unassignment_timestamp=${reassignmentId.unassignmentTs}
          and (time_of_completion_request_counter is NULL
            or (time_of_completion_request_counter = ${timeOfCompletion.rc} and time_of_completion_timestamp = ${timeOfCompletion.timestamp}))
      """

    val doneE: EitherT[Future, ReassignmentStoreError, Unit] =
      EitherT(storage.update(updateSameOrUnset, functionFullName).map { changed =>
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
          Left(ReassignmentAlreadyCompleted(reassignmentId, timeOfCompletion))
        }
      })

    CheckedT.fromEitherTNonabort((), doneE)
  }

  override def deleteReassignment(
      reassignmentId: ReassignmentId
  )(implicit traceContext: TraceContext): Future[Unit] =
    storage.update_(
      sqlu"""delete from par_reassignments
                where target_domain=$domain and origin_domain=${reassignmentId.sourceDomain} and unassignment_timestamp=${reassignmentId.unassignmentTs}""",
      functionFullName,
    )

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val query = sqlu"""
       update par_reassignments
         set time_of_completion_request_counter=null, time_of_completion_timestamp=null
         where target_domain=$domain and time_of_completion_request_counter >= $criterionInclusive
      """
    storage.update_(query, functionFullName)
  }

  private def findPendingBase(domainId: ReassignmentDomainId = domain, onlyNotFinished: Boolean) = {
    import DbStorage.Implicits.BuilderChain.*

    val domainFilter = domainId match {
      case SourceDomainId(domainId) => sql"origin_domain=$domainId"
      case TargetDomainId(domainId) => sql"target_domain=$domainId"
    }

    val notFinishedFilter = if (onlyNotFinished)
      sql" and time_of_completion_request_counter is null and time_of_completion_timestamp is null"
    else sql" "

    val base: SQLActionBuilder = sql"""
     select source_protocol_version, unassignment_timestamp, unassignment_request_counter, unassignment_request, unassignment_decision_time,
     contract, creating_transaction_id, unassignment_result, unassignment_global_offset, assignment_global_offset
     from par_reassignments
     where
   """

    base ++ domainFilter ++ notFinishedFilter
  }

  override def find(
      filterSource: Option[SourceDomainId],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[ReassignmentData]] =
    storage.query(
      {
        import DbStorage.Implicits.BuilderChain.*
        import DbStorage.Implicits.*

        val sourceFilter = filterSource.fold(sql"")(domain => sql" and origin_domain=$domain")
        val timestampFilter =
          filterTimestamp.fold(sql"")(ts => sql" and unassignment_timestamp=$ts")
        val submitterFilter =
          filterSubmitter.fold(sql"")(submitter => sql" and submitter_lf=$submitter")
        val limitSql = storage.limitSql(limit)
        (findPendingBase(onlyNotFinished =
          true
        ) ++ sourceFilter ++ timestampFilter ++ submitterFilter ++ limitSql)
          .as[ReassignmentData]
      },
      functionFullName,
    )

  override def findAfter(
      requestAfter: Option[(CantonTimestamp, SourceDomainId)],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[ReassignmentData]] =
    storage.query(
      {
        import DbStorage.Implicits.BuilderChain.*

        val timestampFilter =
          requestAfter.fold(sql"") { case (requestTimestamp, sourceDomain) =>
            sql" and (unassignment_timestamp, origin_domain) > ($requestTimestamp, $sourceDomain) "
          }
        val order = sql" order by unassignment_timestamp, origin_domain "
        val limitSql = storage.limitSql(limit)

        (findPendingBase(onlyNotFinished = true) ++ timestampFilter ++ order ++ limitSql)
          .as[ReassignmentData]
      },
      functionFullName,
    )

  private def findIncomplete(
      sourceDomain: Option[SourceDomainId],
      validAt: GlobalOffset,
      start: Long,
  )(implicit traceContext: TraceContext): Future[Seq[ReassignmentData]] =
    storage
      .query(
        {
          import DbStorage.Implicits.BuilderChain.*

          val outCompleted =
            sql"(unassignment_global_offset is not null and unassignment_global_offset <= $validAt) and (assignment_global_offset is null or assignment_global_offset > $validAt)"
          val inCompleted =
            sql"(assignment_global_offset is not null and assignment_global_offset <= $validAt) and (unassignment_global_offset is null or unassignment_global_offset > $validAt)"
          val incomplete = sql" and (" ++ outCompleted ++ sql" or " ++ inCompleted ++ sql")"

          val sourceDomainFilter =
            sourceDomain.fold(sql"")(sourceDomain => sql" and origin_domain=$sourceDomain")

          val limitSql =
            storage.limitSql(numberOfItems = DbReassignmentStore.dbQueryLimit, skipItems = start)

          val base = findPendingBase(onlyNotFinished = false)

          (base ++ incomplete ++ sourceDomainFilter ++ limitSql).as[ReassignmentData]
        },
        functionFullName,
      )

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
      sourceDomain: Option[SourceDomainId],
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
  ): Future[Option[(GlobalOffset, ReassignmentId, TargetDomainId)]] = {
    val result = storage
      .query(
        {
          val maxCompletedOffset: SQLActionBuilder =
            sql"""select min(coalesce(assignment_global_offset,${GlobalOffset.MaxValue})),
                  min(coalesce(unassignment_global_offset,${GlobalOffset.MaxValue})),
                  origin_domain, unassignment_timestamp
                  from par_reassignments
                  where target_domain=$domain and (unassignment_global_offset is null or assignment_global_offset is null)
                  group by origin_domain, unassignment_timestamp
                  """

          maxCompletedOffset
            .as[(Option[GlobalOffset], Option[GlobalOffset], DomainId, CantonTimestamp)]
        },
        functionFullName,
      )

    result
      .map(
        _.toList
          .map { case (in, out, source, ts) =>
            ((in.toList ++ out.toList).minOption, ReassignmentId(SourceDomainId(source), ts))
          }
          .foldLeft(
            (
              GlobalOffset.MaxValue,
              ReassignmentId(SourceDomainId(domain.unwrap), CantonTimestamp.MaxValue),
            )
          )((acc: (GlobalOffset, ReassignmentId), n) =>
            n match {
              case (Some(o), tid) => if (acc._1 > o) (o, tid) else acc
              case (None, _) => acc
            }
          ) match {
          case (offset, reassignmentId) =>
            if (offset == GlobalOffset.MaxValue) None else Some((offset, reassignmentId, domain))
        }
      )
  }

  private def insertDependentDeprecated[E, W, A, R](
      exists: DBIO[Option[A]],
      insertExisting: A => Checked[E, W, Option[DBIO[R]]],
      insertFresh: DBIO[R],
      errorHandler: Throwable => E,
      operationName: String = "insertDependentDeprecated",
  )(implicit traceContext: TraceContext): CheckedT[FutureUnlessShutdown, E, W, Option[R]] =
    updateDependentDeprecated(
      exists,
      insertExisting,
      Checked.result(Some(insertFresh)),
      errorHandler,
      operationName,
    )

  private def updateDependentDeprecated[E, W, A, R](
      exists: DBIO[Option[A]],
      insertExisting: A => Checked[E, W, Option[DBIO[R]]],
      insertNonExisting: Checked[E, W, Option[DBIO[R]]],
      errorHandler: Throwable => E,
      operationName: String = "updateDependentDeprecated",
  )(implicit traceContext: TraceContext): CheckedT[FutureUnlessShutdown, E, W, Option[R]] = {
    import DbStorage.Implicits.*
    import storage.api.{DBIO as _, *}

    val readAndInsert =
      exists
        .flatMap(existing =>
          existing.fold(insertNonExisting)(insertExisting(_)).traverse {
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
        sourceProtocolVersion = SourceProtocolVersion(sourceProtocolVersion),
      )
  }

  private def tryCreateDeliveredUnassignmentResult(cryptoApi: CryptoPureApi)(
      bytes: Array[Byte],
      sourceProtocolVersion: SourceProtocolVersion,
  ) = {
    val res: ParsingResult[DeliveredUnassignmentResult] = for {
      signedContent <- SignedContent
        .fromTrustedByteArray(bytes)
        .flatMap(
          _.deserializeContent(
            SequencedEvent.fromByteStringOpen(cryptoApi, sourceProtocolVersion.v)
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
