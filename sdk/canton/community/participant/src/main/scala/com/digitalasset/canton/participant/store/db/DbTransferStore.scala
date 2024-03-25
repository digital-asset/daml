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
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferOutTree}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.TransferData.TransferGlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.{IncompleteTransferData, TransferData}
import com.digitalasset.canton.participant.store.TransferStore
import com.digitalasset.canton.participant.store.TransferStore.*
import com.digitalasset.canton.participant.store.db.DbTransferStore.RawDeliveredTransferOutResult
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{
  SerializableContract,
  SourceDomainId,
  TargetDomainId,
  TransactionId,
  TransferDomainId,
  TransferId,
}
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.{NoOpeningErrors, SequencedEvent, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, RequestCounter}
import com.google.protobuf.ByteString
import slick.jdbc.TransactionIsolation.Serializable
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class DbTransferStore(
    override protected val storage: DbStorage,
    domain: TargetDomainId,
    targetDomainProtocolVersion: TargetProtocolVersion,
    cryptoApi: CryptoPureApi,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    // TODO(#9270) clean up how we parameterize our nodes
    batchSize: Int = 500,
)(implicit ec: ExecutionContext)
    extends TransferStore
    with DbStore {
  import storage.api.*
  import storage.converters.*

  private def getResultFullTransferOutTree(
      sourceDomainProtocolVersion: SourceProtocolVersion
  ): GetResult[FullTransferOutTree] =
    GetResult(r =>
      FullTransferOutTree
        .fromByteString(cryptoApi, sourceDomainProtocolVersion)(
          ByteString.copyFrom(r.<<[Array[Byte]])
        )
        .fold[FullTransferOutTree](
          error =>
            throw new DbDeserializationException(
              s"Error deserializing transfer out request $error"
            ),
          Predef.identity,
        )
    )

  private def setResultFullTransferOutTree(
      sourceProtocolVersion: SourceProtocolVersion
  ): SetParameter[FullTransferOutTree] =
    (r: FullTransferOutTree, pp: PositionedParameters) =>
      pp >> r.toByteString(sourceProtocolVersion.v).toByteArray

  private implicit val setParameterSerializableContract: SetParameter[SerializableContract] =
    SerializableContract.getVersionedSetParameter(targetDomainProtocolVersion.v)

  private implicit val getResultOptionRawDeliveredTransferOutResult
      : GetResult[Option[RawDeliveredTransferOutResult]] = GetResult { r =>
    r.nextBytesOption().map { bytes =>
      RawDeliveredTransferOutResult(bytes, GetResult[ProtocolVersion].apply(r))
    }
  }

  private def getResultDeliveredTransferOutResult(
      sourceProtocolVersion: SourceProtocolVersion
  ): GetResult[Option[DeliveredTransferOutResult]] =
    GetResult(r =>
      r.nextBytesOption().map { bytes =>
        DbTransferStore.tryCreateDeliveredTransferOutResult(cryptoApi)(bytes, sourceProtocolVersion)
      }
    )

  private implicit val setParameterDeliveredTransferOutResult
      : SetParameter[DeliveredTransferOutResult] =
    (r: DeliveredTransferOutResult, pp: PositionedParameters) => pp >> r.result.toByteArray

  private implicit val setParameterOptionDeliveredTransferOutResult
      : SetParameter[Option[DeliveredTransferOutResult]] =
    (r: Option[DeliveredTransferOutResult], pp: PositionedParameters) =>
      pp >> r.map(_.result.toByteArray)

  private implicit val getResultTransferData: GetResult[TransferData] = GetResult { r =>
    val sourceProtocolVersion = SourceProtocolVersion(GetResult[ProtocolVersion].apply(r))

    TransferData(
      sourceProtocolVersion = sourceProtocolVersion,
      transferOutTimestamp = GetResult[CantonTimestamp].apply(r),
      transferOutRequestCounter = GetResult[RequestCounter].apply(r),
      transferOutRequest = getResultFullTransferOutTree(sourceProtocolVersion).apply(r),
      transferOutDecisionTime = GetResult[CantonTimestamp].apply(r),
      contract = GetResult[SerializableContract].apply(r),
      creatingTransactionId = GetResult[TransactionId].apply(r),
      transferOutResult = getResultDeliveredTransferOutResult(sourceProtocolVersion).apply(r),
      transferGlobalOffset = TransferGlobalOffset
        .create(
          r.nextLongOption().map(GlobalOffset.tryFromLong),
          r.nextLongOption().map(GlobalOffset.tryFromLong),
        )
        .valueOr(err => throw new DbDeserializationException(err)),
    )
  }

  private implicit val getResultTransferEntry: GetResult[TransferEntry] = GetResult(r =>
    TransferEntry(
      getResultTransferData(r),
      GetResult[Option[TimeOfChange]].apply(r),
    )
  )

  /*
   Used to ensure updates of the transfer-out/in global offsets are sequential
   Note: this safety could be removed as the callers of `addTransfersOffsets` are the multi-domain event log and the
   `InFlightSubmissionTracker` which both call this sequentially.
   */
  private val sequentialQueue = new SimpleExecutionQueue(
    "transfer-store-offsets-update",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  override def addTransfer(
      transferData: TransferData
  )(implicit traceContext: TraceContext): EitherT[Future, TransferStoreError, Unit] = {
    @unused implicit val setParameterFullTransferOutTree =
      setResultFullTransferOutTree(transferData.sourceProtocolVersion)

    ErrorUtil.requireArgument(
      transferData.targetDomain == domain,
      s"Domain ${domain.unwrap}: Transfer store cannot store transfer for domain ${transferData.targetDomain.unwrap}",
    )

    val transferId: TransferId = transferData.transferId
    val newEntry = TransferEntry(transferData, None)

    import DbStorage.Implicits.*
    val insert: DBIO[Int] = sqlu"""
        insert into par_transfers(target_domain, origin_domain, transfer_out_timestamp, transfer_out_request_counter,
        transfer_out_request, transfer_out_decision_time, contract, creating_transaction_id, transfer_out_result,
        submitter_lf, source_protocol_version, transfer_out_global_offset, transfer_in_global_offset)
        values (
          $domain,
          ${transferId.sourceDomain},
          ${transferId.transferOutTimestamp},
          ${transferData.transferOutRequestCounter},
          ${transferData.transferOutRequest},
          ${transferData.transferOutDecisionTime},
          ${transferData.contract},
          ${transferData.creatingTransactionId},
          ${transferData.transferOutResult},
          ${transferData.transferOutRequest.submitter},
          ${transferData.sourceProtocolVersion},
          ${transferData.transferOutGlobalOffset},
          ${transferData.transferInGlobalOffset}
        )
      """

    def insertExisting(
        existingEntry: TransferEntry
    ): Checked[TransferStoreError, TransferAlreadyCompleted, Option[DBIO[Int]]] = {
      @unused implicit val setParameterFullTransferOutTree =
        setResultFullTransferOutTree(existingEntry.transferData.sourceProtocolVersion)

      def update(entry: TransferEntry): DBIO[Int] = {
        val id = entry.transferData.transferId
        val data = entry.transferData
        sqlu"""
          update par_transfers
          set transfer_out_request_counter=${data.transferOutRequestCounter},
            transfer_out_request=${data.transferOutRequest}, transfer_out_decision_time=${data.transferOutDecisionTime},
            contract=${data.contract}, creating_transaction_id=${data.creatingTransactionId},
            transfer_out_result=${data.transferOutResult}, submitter_lf=${data.transferOutRequest.submitter},
            source_protocol_version=${data.sourceProtocolVersion},
            transfer_out_global_offset=${data.transferOutGlobalOffset}, transfer_in_global_offset=${data.transferInGlobalOffset}
           where
              target_domain=$domain and origin_domain=${id.sourceDomain} and transfer_out_timestamp=${data.transferOutTimestamp}
          """
      }
      existingEntry.mergeWith(newEntry).map(entry => Some(update(entry)))
    }

    insertDependentDeprecated(
      entryExists(transferId),
      insertExisting,
      insert,
      dbError => throw dbError,
    )
      .map(_ => ())
      .toEitherT
  }

  override def lookup(transferId: TransferId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferStore.TransferLookupError, TransferData] = {
    EitherT(storage.query(entryExists(transferId), functionFullName).map {
      case None => Left(UnknownTransferId(transferId))
      case Some(TransferEntry(_, Some(timeOfCompletion))) =>
        Left(TransferCompleted(transferId, timeOfCompletion))
      case Some(transferEntry) => Right(transferEntry.transferData)
    })
  }

  private def entryExists(id: TransferId): DbAction.ReadOnly[Option[TransferEntry]] = sql"""
     select source_protocol_version, transfer_out_timestamp, transfer_out_request_counter, transfer_out_request, transfer_out_decision_time,
     contract, creating_transaction_id, transfer_out_result, transfer_out_global_offset, transfer_in_global_offset,
     time_of_completion_request_counter, time_of_completion_timestamp
     from par_transfers where target_domain=$domain and origin_domain=${id.sourceDomain} and transfer_out_timestamp=${id.transferOutTimestamp}
    """.as[TransferEntry].headOption

  override def addTransferOutResult(
      transferOutResult: DeliveredTransferOutResult
  )(implicit traceContext: TraceContext): EitherT[Future, TransferStoreError, Unit] = {
    val transferId = transferOutResult.transferId

    val existsRaw: DbAction.ReadOnly[Option[Option[RawDeliveredTransferOutResult]]] = sql"""
       select transfer_out_result, source_protocol_version
       from par_transfers
       where
          target_domain=$domain and origin_domain=${transferId.sourceDomain} and transfer_out_timestamp=${transferId.transferOutTimestamp}
        """.as[Option[RawDeliveredTransferOutResult]].headOption

    val exists = existsRaw.map(_.map(_.map(_.tryCreateDeliveredTransferOutResul(cryptoApi))))

    def update(previousResult: Option[DeliveredTransferOutResult]) = {
      previousResult
        .fold[Checked[TransferStoreError, Nothing, Option[DBIO[Int]]]](Checked.result(Some(sqlu"""
              update par_transfers
              set transfer_out_result=${transferOutResult}
              where target_domain=$domain and origin_domain=${transferId.sourceDomain} and transfer_out_timestamp=${transferId.transferOutTimestamp}
              """)))(previous =>
          if (previous == transferOutResult) Checked.result(None)
          else
            Checked.abort(TransferOutResultAlreadyExists(transferId, previous, transferOutResult))
        )
    }

    updateDependentDeprecated(
      exists,
      update,
      Checked.abort(UnknownTransferId(transferId)),
      dbError => throw dbError,
    )
      .map(_ => ())
      .toEitherT
  }

  def addTransfersOffsets(offsets: Map[TransferId, TransferGlobalOffset])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] = {
    if (offsets.isEmpty) EitherT.pure[FutureUnlessShutdown, TransferStoreError](())
    else
      MonadUtil.sequentialTraverse_(offsets.toList.grouped(batchSize))(offsets =>
        addTransfersOffsetsInternal(NonEmptyUtil.fromUnsafe(offsets))
      )
  }

  /*
    Requires:
      - transfer id to be all distinct
      - size of the list be within bounds that allow for single DB queries (use `batchSize`)
   */
  private def addTransfersOffsetsInternal(
      offsets: NonEmpty[List[(TransferId, TransferGlobalOffset)]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] = {
    import DbStorage.Implicits.BuilderChain.*

    val transferIdsFilter = offsets
      .map { case (transferId, _) =>
        sql"(origin_domain=${transferId.sourceDomain} and transfer_out_timestamp=${transferId.transferOutTimestamp})"
      }
      .forgetNE
      .intercalate(sql" or ")
      .toActionBuilder

    val select =
      sql"""select origin_domain, transfer_out_timestamp, transfer_out_global_offset, transfer_in_global_offset
           from par_transfers
           where
              target_domain=$domain and (""" ++ transferIdsFilter ++ sql")"

    val updateQuery =
      """update par_transfers
       set transfer_out_global_offset = ?, transfer_in_global_offset = ?
       where target_domain = ? and origin_domain = ? and transfer_out_timestamp = ?
    """

    lazy val task = for {
      res <- EitherT.liftF(
        storage.query(
          select.as[(TransferId, Option[GlobalOffset], Option[GlobalOffset])],
          functionFullName,
        )
      )
      retrievedItems = res.map { case (transferId, out, in) => transferId -> (out, in) }.toMap

      mergedGlobalOffsets <- EitherT.fromEither[Future](offsets.forgetNE.traverse {
        case (transferId, newOffsets) =>
          retrievedItems
            .get(transferId)
            .toRight(UnknownTransferId(transferId))
            .map { case (offsetOutO, offsetInO) =>
              TransferGlobalOffset
                .create(offsetOutO, offsetInO)
                .valueOr(err => throw new DbDeserializationException(err))
            }
            .flatMap(
              _.fold[Either[String, TransferGlobalOffset]](Right(newOffsets))(_.merge(newOffsets))
                .leftMap(TransferGlobalOffsetsMerge(transferId, _))
                .map((transferId, _))
            )
      })

      batchUpdate = DbStorage.bulkOperation_(updateQuery, mergedGlobalOffsets, storage.profile) {
        pp => mergedGlobalOffsetWithId =>
          val (transferId, mergedGlobalOffset) = mergedGlobalOffsetWithId

          pp >> mergedGlobalOffset.out
          pp >> mergedGlobalOffset.in
          pp >> domain.unwrap
          pp >> transferId.sourceDomain.unwrap
          pp >> transferId.transferOutTimestamp
      }

      _ <- EitherT.liftF[Future, TransferStoreError, Unit](
        storage.queryAndUpdate(batchUpdate, functionFullName)
      )
    } yield ()

    sequentialQueue.executeE(task, "addTransfersOffsets")
  }

  override def completeTransfer(transferId: TransferId, timeOfCompletion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, Nothing, TransferStoreError, Unit] = {

    val updateSameOrUnset = sqlu"""
        update par_transfers
          set time_of_completion_request_counter=${timeOfCompletion.rc}, time_of_completion_timestamp=${timeOfCompletion.timestamp}
        where
          target_domain=$domain and origin_domain=${transferId.sourceDomain} and transfer_out_timestamp=${transferId.transferOutTimestamp}
          and (time_of_completion_request_counter is NULL
            or (time_of_completion_request_counter = ${timeOfCompletion.rc} and time_of_completion_timestamp = ${timeOfCompletion.timestamp}))
      """

    val doneE: EitherT[Future, TransferStoreError, Unit] =
      EitherT(storage.update(updateSameOrUnset, functionFullName).map { changed =>
        if (changed > 0) {
          if (changed != 1)
            logger.error(
              s"Transfer completion query changed $changed lines. It should only change 1."
            )
          Right(())
        } else {
          if (changed != 0)
            logger.error(
              s"Transfer completion query changed $changed lines -- this should not be negative."
            )
          Left(TransferAlreadyCompleted(transferId, timeOfCompletion))
        }
      })

    CheckedT.fromEitherTNonabort((), doneE)
  }

  override def deleteTransfer(
      transferId: TransferId
  )(implicit traceContext: TraceContext): Future[Unit] = {
    storage.update_(
      sqlu"""delete from par_transfers
                where target_domain=$domain and origin_domain=${transferId.sourceDomain} and transfer_out_timestamp=${transferId.transferOutTimestamp}""",
      functionFullName,
    )
  }

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val query = sqlu"""
       update par_transfers
         set time_of_completion_request_counter=null, time_of_completion_timestamp=null
         where target_domain=$domain and time_of_completion_request_counter >= $criterionInclusive
      """
    storage.update_(query, functionFullName)
  }

  // TODO(#11722) Parameter domainIsTarget can be dropped once TransferStore is owned by source domain
  private def findPendingBase(domainId: TransferDomainId = domain, onlyNotFinished: Boolean) = {
    import DbStorage.Implicits.BuilderChain.*

    val domainFilter = domainId match {
      case SourceDomainId(domainId) => sql"origin_domain=$domainId"
      case TargetDomainId(domainId) => sql"target_domain=$domainId"
    }

    val notFinishedFilter = if (onlyNotFinished)
      sql" and time_of_completion_request_counter is null and time_of_completion_timestamp is null"
    else sql" "

    val base: SQLActionBuilder = sql"""
     select source_protocol_version, transfer_out_timestamp, transfer_out_request_counter, transfer_out_request, transfer_out_decision_time,
     contract, creating_transaction_id, transfer_out_result, transfer_out_global_offset, transfer_in_global_offset
     from par_transfers
     where
   """

    base ++ domainFilter ++ notFinishedFilter
  }

  override def find(
      filterSource: Option[SourceDomainId],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]] = {
    storage.query(
      {
        import DbStorage.Implicits.BuilderChain.*
        import DbStorage.Implicits.*

        val sourceFilter = filterSource.fold(sql"")(domain => sql" and origin_domain=${domain}")
        val timestampFilter =
          filterTimestamp.fold(sql"")(ts => sql" and transfer_out_timestamp=${ts}")
        val submitterFilter =
          filterSubmitter.fold(sql"")(submitter => sql" and submitter_lf=${submitter}")
        val limitSql = storage.limitSql(limit)
        (findPendingBase(onlyNotFinished =
          true
        ) ++ sourceFilter ++ timestampFilter ++ submitterFilter ++ limitSql)
          .as[TransferData]
      },
      functionFullName,
    )
  }

  override def findAfter(
      requestAfter: Option[(CantonTimestamp, SourceDomainId)],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]] = {
    storage.query(
      {
        import DbStorage.Implicits.BuilderChain.*

        val timestampFilter =
          requestAfter.fold(sql"") { case (requestTimestamp, sourceDomain) =>
            storage.profile match {
              case Profile.Oracle(_) =>
                sql" and (transfer_out_timestamp > ${requestTimestamp} or (transfer_out_timestamp = ${requestTimestamp} and origin_domain > ${sourceDomain}))"
              case _ =>
                sql" and (transfer_out_timestamp, origin_domain) > (${requestTimestamp}, ${sourceDomain}) "
            }
          }
        val order = sql" order by transfer_out_timestamp, origin_domain "
        val limitSql = storage.limitSql(limit)

        (findPendingBase(onlyNotFinished = true) ++ timestampFilter ++ order ++ limitSql)
          .as[TransferData]
      },
      functionFullName,
    )
  }

  private def findIncomplete(
      sourceDomain: Option[SourceDomainId],
      validAt: GlobalOffset,
      start: Long,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]] =
    storage
      .query(
        {
          import DbStorage.Implicits.BuilderChain.*

          val outCompleted =
            sql"(transfer_out_global_offset is not null and transfer_out_global_offset <= $validAt) and (transfer_in_global_offset is null or transfer_in_global_offset > $validAt)"
          val inCompleted =
            sql"(transfer_in_global_offset is not null and transfer_in_global_offset <= $validAt) and (transfer_out_global_offset is null or transfer_out_global_offset > $validAt)"
          val incomplete = sql" and (" ++ outCompleted ++ sql" or " ++ inCompleted ++ sql")"

          val sourceDomainFilter =
            sourceDomain.fold(sql"")(sourceDomain => sql" and origin_domain=$sourceDomain")

          val limitSql =
            storage.limitSql(numberOfItems = DbTransferStore.dbQueryLimit, skipItems = start)

          val base = findPendingBase(onlyNotFinished = false)

          (base ++ incomplete ++ sourceDomainFilter ++ limitSql).as[TransferData]
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
      queryFrom: (Long, TraceContext) => Future[Seq[TransferData]],
  )(implicit traceContext: TraceContext) = {

    def stakeholderFilter(data: TransferData): Boolean = stakeholders
      .forall(_.exists(data.contract.metadata.stakeholders))

    Monad[Future].tailRecM((Vector.empty[TransferData], 0, 0L)) { case (acc, accSize, start) =>
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
  )(implicit traceContext: TraceContext): Future[Seq[IncompleteTransferData]] = {
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
      dbQueryLimit = DbTransferStore.dbQueryLimit,
    ).map(
      _.map(IncompleteTransferData.tryCreate(_, validAt))
        .sortBy(_.transferEventGlobalOffset.globalOffset)
    )
  }

  override def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, TransferId, TargetDomainId)]] = {
    val result = storage
      .query(
        {
          val maxCompletedOffset: SQLActionBuilder =
            sql"""select min(coalesce(transfer_in_global_offset,${GlobalOffset.MaxValue})),
                  min(coalesce(transfer_out_global_offset,${GlobalOffset.MaxValue})),
                  origin_domain, transfer_out_timestamp
                  from par_transfers
                  where target_domain=$domain and (transfer_out_global_offset is null or transfer_in_global_offset is null)
                  group by origin_domain, transfer_out_timestamp
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
            ((in.toList ++ out.toList).minOption, TransferId(SourceDomainId(source), ts))
          }
          .foldLeft(
            (
              GlobalOffset.MaxValue,
              TransferId(SourceDomainId(domain.unwrap), CantonTimestamp.MaxValue),
            )
          )((acc: (GlobalOffset, TransferId), n) =>
            n match {
              case (Some(o), tid) => if (acc._1 > o) (o, tid) else acc
              case (None, _) => acc
            }
          ) match {
          case (offset, transferId) =>
            if (offset == GlobalOffset.MaxValue) None else Some((offset, transferId, domain))
        }
      )
  }

  private def insertDependentDeprecated[E, W, A, R](
      exists: DBIO[Option[A]],
      insertExisting: A => Checked[E, W, Option[DBIO[R]]],
      insertFresh: DBIO[R],
      errorHandler: Throwable => E,
      operationName: String = "insertDependentDeprecated",
  )(implicit traceContext: TraceContext): CheckedT[Future, E, W, Option[R]] =
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
  )(implicit traceContext: TraceContext): CheckedT[Future, E, W, Option[R]] = {
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

    val result = storage.queryAndUpdate(compoundAction, operationName = operationName)

    CheckedT(result.recover[Checked[E, W, Option[R]]] { case NonFatal(x) =>
      Checked.abort(errorHandler(x))
    })
  }
}

object DbTransferStore {
  // We tend to use 1000 to limit queries
  private val dbQueryLimit = 1000

  /*
    This class is a helper to deserialize DeliveredTransferOutResult because its deserialization
    depends on the ProtocolVersion of the source domain.
   */
  final case class RawDeliveredTransferOutResult(
      result: Array[Byte],
      sourceProtocolVersion: ProtocolVersion,
  ) {
    def tryCreateDeliveredTransferOutResul(cryptoApi: CryptoPureApi): DeliveredTransferOutResult =
      tryCreateDeliveredTransferOutResult(cryptoApi)(
        bytes = result,
        sourceProtocolVersion = SourceProtocolVersion(sourceProtocolVersion),
      )
  }

  private def tryCreateDeliveredTransferOutResult(cryptoApi: CryptoPureApi)(
      bytes: Array[Byte],
      sourceProtocolVersion: SourceProtocolVersion,
  ) = {
    val res: ParsingResult[DeliveredTransferOutResult] = for {
      signedContent <- SignedContent
        .fromByteArrayUnsafe(bytes)
        .flatMap(
          _.deserializeContent(
            SequencedEvent.fromByteStringOpen(cryptoApi, sourceProtocolVersion.v)
          )
        )
      result <- DeliveredTransferOutResult
        .create(NoOpeningErrors(signedContent))
        .leftMap(err => OtherError(err.toString))
    } yield result

    res.fold(
      error =>
        throw new DbDeserializationException(
          s"Error deserializing delivered transfer out result $error"
        ),
      identity,
    )
  }
}
