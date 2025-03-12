// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.db.DbRequestJournalStore.ReplaceRequest
import com.digitalasset.canton.participant.util.TimeOfRequest
import com.digitalasset.canton.resource.DbStorage.DbAction.ReadOnly
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.DbBulkUpdateProcessor
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil, TryUtil}
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.*

import java.util.ConcurrentModificationException
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.{Success, Try}

class DbRequestJournalStore(
    indexedSynchronizer: IndexedSynchronizer,
    override protected val storage: DbStorage,
    insertBatchAggregatorConfig: BatchAggregatorConfig,
    replaceBatchAggregatorConfig: BatchAggregatorConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override private[store] implicit val ec: ExecutionContext)
    extends RequestJournalStore
    with DbStore { self =>

  import DbStorage.Implicits.*
  import storage.api.*

  implicit val getResultRequestState: GetResult[RequestState] = GetResult { r =>
    val index = r.nextInt()
    RequestState(index).getOrElse(sys.error(s"Stored request state index $index is invalid"))
  }
  implicit val setParameterRequestState: SetParameter[RequestState] =
    (s: RequestState, pp: PositionedParameters) => pp.setInt(s.index)

  implicit val getResultRequestData: GetResult[RequestData] = GetResult(r =>
    RequestData(
      GetResult[RequestCounter].apply(r),
      getResultRequestState(r),
      GetResult[CantonTimestamp].apply(r),
      GetResult[Option[CantonTimestamp]].apply(r),
    )
  )

  override def insert(data: RequestData)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    batchAggregatorInsert.run(data).flatMap(FutureUnlessShutdown.fromTry)

  private val batchAggregatorInsert = {
    val processor = new DbBulkUpdateProcessor[RequestData, Unit] {
      override protected implicit def executionContext: ExecutionContext =
        DbRequestJournalStore.this.ec
      override protected def storage: DbStorage = DbRequestJournalStore.this.storage
      override def kind: String = "request"
      override def logger: TracedLogger = DbRequestJournalStore.this.logger

      override def executeBatch(items: NonEmpty[Seq[Traced[RequestData]]])(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[Iterable[Try[Unit]]] =
        bulkUpdateWithCheck(items, "DbRequestJournalStore.insert")(traceContext, self.closeContext)

      override protected def bulkUpdateAction(items: NonEmpty[Seq[Traced[RequestData]]])(implicit
          batchTraceContext: TraceContext
      ): DBIOAction[Array[Int], NoStream, Effect.All] = {
        def setData(pp: PositionedParameters)(item: RequestData): Unit = {
          val RequestData(rc, state, requestTimestamp, commitTime) = item
          pp >> indexedSynchronizer
          pp >> rc
          pp >> state
          pp >> requestTimestamp
          pp >> commitTime
        }

        val query =
          """insert into par_journal_requests(synchronizer_idx, request_counter, request_state_index, request_timestamp, commit_time)
             values (?, ?, ?, ?, ?)
             on conflict do nothing"""
        DbStorage.bulkOperation(query, items.map(_.value).toList, storage.profile)(setData)

      }

      override protected def onSuccessItemUpdate(item: Traced[RequestData]): Try[Unit] =
        TryUtil.unit

      override protected type CheckData = RequestData
      override protected type ItemIdentifier = RequestCounter
      override protected def itemIdentifier(item: RequestData): ItemIdentifier = item.rc
      override protected def dataIdentifier(state: CheckData): ItemIdentifier = state.rc

      override protected def checkQuery(itemsToCheck: NonEmpty[Seq[ItemIdentifier]])(implicit
          batchTraceContext: TraceContext
      ): ReadOnly[immutable.Iterable[CheckData]] =
        bulkQueryDbio(itemsToCheck)

      override protected def analyzeFoundData(item: RequestData, foundData: Option[RequestData])(
          implicit traceContext: TraceContext
      ): Try[Unit] =
        foundData match {
          case None =>
            ErrorUtil.internalErrorTry(
              new IllegalStateException(show"Failed to insert data for request ${item.rc}")
            )
          case Some(data) =>
            if (data == item) TryUtil.unit
            else
              ErrorUtil.internalErrorTry(
                new IllegalStateException(
                  show"Conflicting data for request ${item.rc}: $item and $data"
                )
              )
        }

      override def prettyItem: Pretty[RequestData] = implicitly
    }

    BatchAggregator(processor, insertBatchAggregatorConfig)
  }

  override def query(
      rc: RequestCounter
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, RequestData] = {
    val query =
      sql"""select request_counter, request_state_index, request_timestamp, commit_time
              from par_journal_requests where request_counter = $rc and synchronizer_idx = $indexedSynchronizer"""
        .as[RequestData]
    OptionT(storage.query(query.headOption, functionFullName))
  }

  private def bulkQueryDbio(
      rcs: NonEmpty[Seq[RequestCounter]]
  ): DbAction.ReadOnly[immutable.Iterable[RequestData]] = {
    import DbStorage.Implicits.BuilderChain.*
    val query =
      sql"""select request_counter, request_state_index, request_timestamp, commit_time
              from par_journal_requests where synchronizer_idx = $indexedSynchronizer and """ ++ DbStorage
        .toInClause(
          "request_counter",
          rcs,
        )
    query.as[RequestData]
  }

  override def firstRequestWithCommitTimeAfter(commitTimeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RequestData]] =
    storage.profile match {
      case _: Profile.Postgres =>
        for {
          // Postgres needs to be motivated to use the idx_journal_request_commit_time index by this peculiar
          // initial query. Combining the two queries or modifying the initial query even slightly results
          // in Postgres choosing the primary key index running orders of magnitudes slower. Details in #14682
          rcMinCommittedAfterO <- storage.query(
            sql"""
                  with committed_after(request_counter) as (
                    select request_counter
                    from par_journal_requests
                    where synchronizer_idx = $indexedSynchronizer and commit_time > $commitTimeExclusive)
                  select min(request_counter) from committed_after;
              """.as[Option[RequestCounter]].headOption.map(_.flatten),
            functionFullName + ".committed_after",
          )
          requestData <- rcMinCommittedAfterO.fold(
            FutureUnlessShutdown.pure(Option.empty[RequestData])
          )(rc =>
            storage.query(
              sql"""
                    select request_counter, request_state_index, request_timestamp, commit_time
                    from par_journal_requests
                    where synchronizer_idx = $indexedSynchronizer and request_counter = $rc
                """.as[RequestData].headOption,
              functionFullName,
            )
          )
        } yield requestData
      case _: Profile.H2 =>
        storage.query(
          sql"""
                select request_counter, request_state_index, request_timestamp, commit_time
                from par_journal_requests where synchronizer_idx = $indexedSynchronizer and commit_time > $commitTimeExclusive
                order by request_counter #${storage.limit(1)}
            """.as[RequestData].headOption,
          functionFullName,
        )
    }

  override def replace(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      newState: RequestState,
      commitTime: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RequestJournalStoreError, Unit] =
    commitTime match {
      case Some(commitTime) if commitTime < requestTimestamp =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          CommitTimeBeforeRequestTime(
            rc,
            requestTimestamp,
            commitTime,
          )
        )
      case _ =>
        val request = ReplaceRequest(rc, requestTimestamp, newState, commitTime)
        EitherT(batchAggregatorReplace.run(request).flatMap(FutureUnlessShutdown.fromTry))
    }

  private val batchAggregatorReplace = {
    type Result = Either[RequestJournalStoreError, Unit]

    val processor = new DbBulkUpdateProcessor[ReplaceRequest, Result] {
      override protected implicit def executionContext: ExecutionContext =
        DbRequestJournalStore.this.ec
      override protected def storage: DbStorage = DbRequestJournalStore.this.storage
      override def kind: String = "request"
      override def logger: TracedLogger = DbRequestJournalStore.this.logger

      override def executeBatch(items: NonEmpty[Seq[Traced[DbRequestJournalStore.ReplaceRequest]]])(
          implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[Iterable[Try[Result]]] =
        bulkUpdateWithCheck(items, "DbRequestJournalStore.replace")(traceContext, self.closeContext)

      override protected def bulkUpdateAction(items: NonEmpty[Seq[Traced[ReplaceRequest]]])(implicit
          batchTraceContext: TraceContext
      ): DBIOAction[Array[Int], NoStream, Effect.All] = {
        val updateQuery =
          """update /*+ INDEX (journal_requests (request_counter, synchronizer_idx)) */ par_journal_requests
             set request_state_index = ?, commit_time = coalesce (?, commit_time)
             where synchronizer_idx = ? and request_counter = ? and request_timestamp = ?"""
        DbStorage.bulkOperation(updateQuery, items.map(_.value).toList, storage.profile) {
          pp => item =>
            val ReplaceRequest(rc, requestTimestamp, newState, commitTime) = item
            pp >> newState
            pp >> commitTime
            pp >> indexedSynchronizer
            pp >> rc
            pp >> requestTimestamp
        }
      }

      private val success: Try[Result] = Success(Either.unit)
      override protected def onSuccessItemUpdate(item: Traced[ReplaceRequest]): Try[Result] =
        success

      override protected type CheckData = RequestData
      override protected type ItemIdentifier = RequestCounter
      override protected def itemIdentifier(item: ReplaceRequest): RequestCounter = item.rc
      override protected def dataIdentifier(state: RequestData): RequestCounter = state.rc

      override protected def checkQuery(itemsToCheck: NonEmpty[Seq[RequestCounter]])(implicit
          batchTraceContext: TraceContext
      ): ReadOnly[immutable.Iterable[RequestData]] = bulkQueryDbio(itemsToCheck)

      override protected def analyzeFoundData(item: ReplaceRequest, foundData: Option[RequestData])(
          implicit traceContext: TraceContext
      ): Try[Result] = {
        val ReplaceRequest(rc, requestTimestamp, newState, commitTime) = item
        foundData match {
          case None => Success(Left(UnknownRequestCounter(rc)))
          case Some(data) =>
            if (data.requestTimestamp != requestTimestamp) {
              val inconsistent =
                InconsistentRequestTimestamp(rc, data.requestTimestamp, requestTimestamp)
              Success(Left(inconsistent))
            } else if (data.state == newState && data.commitTime == commitTime)
              // `update` may under report the number of changed rows,
              // so we're fine if the new state is already there.
              Success(Either.unit)
            else {
              val ex = new ConcurrentModificationException(
                s"Concurrent request journal modification for request $rc"
              )
              ErrorUtil.internalErrorTry(ex)
            }
        }
      }

      override def prettyItem: Pretty[DbRequestJournalStore.ReplaceRequest] = implicitly
    }

    BatchAggregator[ReplaceRequest, Try[Result]](
      processor,
      replaceBatchAggregatorConfig,
    )
  }

  @VisibleForTesting
  private[store] override def pruneInternal(
      beforeInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"""
        delete from par_journal_requests where request_timestamp <= $beforeInclusive and synchronizer_idx = $indexedSynchronizer
      """,
      functionFullName,
    )

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"""
        delete from par_journal_requests where synchronizer_idx = $indexedSynchronizer
      """,
      functionFullName,
    )

  override def size(start: CantonTimestamp, end: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    storage
      .query(
        {
          import BuilderChain.*
          val endFilter = end.fold(sql"")(ts => sql" and request_timestamp <= $ts")
          (sql"""
             select 1
             from par_journal_requests where synchronizer_idx = $indexedSynchronizer and request_timestamp >= $start
            """ ++ endFilter).as[Int]
        },
        functionFullName,
      )
      .map(_.size)

  override def deleteSince(
      fromInclusive: RequestCounter
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val statement =
      sqlu"""
        delete from par_journal_requests where synchronizer_idx = $indexedSynchronizer and request_counter >= $fromInclusive
        """
    storage.update_(statement, functionFullName)
  }

  override def totalDirtyRequests()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[NonNegativeInt] = {
    val statement =
      sql"""
        select count(*)
        from par_journal_requests where synchronizer_idx = $indexedSynchronizer and commit_time is null
        """.as[Int].head
    storage.query(statement, functionFullName).map(NonNegativeInt.tryCreate)
  }

  override def lastRequestTimeWithRequestTimestampBeforeOrAt(requestTimestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TimeOfRequest]] =
    storage.query(
      sql"""
        select request_timestamp, request_counter
        from par_journal_requests
        where synchronizer_idx = $indexedSynchronizer and request_timestamp <= $requestTimestamp
        order by (synchronizer_idx, request_timestamp) desc
        #${storage.limit(1)}
        """.as[TimeOfRequest].headOption,
      functionFullName,
    )
}

object DbRequestJournalStore {

  final case class ReplaceRequest(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      newState: RequestState,
      commitTime: Option[CantonTimestamp],
  ) extends PrettyPrinting {

    override protected def pretty: Pretty[ReplaceRequest] = prettyOfClass(
      param("rc", _.rc),
      param("new state", _.newState),
      param("request timestamp", _.requestTimestamp),
    )
  }
}
