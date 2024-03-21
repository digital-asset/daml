// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, OptionT}
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, Lifecycle}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.admin.repair.RepairContext
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.db.DbRequestJournalStore.ReplaceRequest
import com.digitalasset.canton.resource.DbStorage.DbAction.ReadOnly
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.{
  DbBulkUpdateProcessor,
  DbCursorPreheadStore,
  SequencerClientDiscriminator,
}
import com.digitalasset.canton.store.{CursorPreheadStore, IndexedDomain}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil}
import com.digitalasset.canton.{RequestCounter, RequestCounterDiscriminator}
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.*

import java.util.ConcurrentModificationException
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class DbRequestJournalStore(
    domainId: IndexedDomain,
    override protected val storage: DbStorage,
    maxItemsInSqlInClause: PositiveNumeric[Int],
    insertBatchAggregatorConfig: BatchAggregatorConfig,
    replaceBatchAggregatorConfig: BatchAggregatorConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override private[store] implicit val ec: ExecutionContext)
    extends RequestJournalStore
    with DbStore { self =>

  import DbStorage.Implicits.*
  import storage.api.*

  private val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("request-journal-store")

  private[store] override val cleanPreheadStore: CursorPreheadStore[RequestCounterDiscriminator] =
    new DbCursorPreheadStore[RequestCounterDiscriminator](
      SequencerClientDiscriminator.fromIndexedDomainId(domainId),
      storage,
      cursorTable = "head_clean_counters",
      processingTime,
      timeouts,
      loggerFactory,
    )

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
      GetResult[Option[RepairContext]].apply(r),
    )
  )

  override def insert(data: RequestData)(implicit traceContext: TraceContext): Future[Unit] =
    batchAggregatorInsert.run(data).flatMap(Future.fromTry)

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
      ): Future[Iterable[Try[Unit]]] =
        bulkUpdateWithCheck(items, "DbRequestJournalStore.insert")(traceContext, self.closeContext)

      override protected def bulkUpdateAction(items: NonEmpty[Seq[Traced[RequestData]]])(implicit
          batchTraceContext: TraceContext
      ): DBIOAction[Array[Int], NoStream, Effect.All] = {
        def setData(pp: PositionedParameters)(item: RequestData): Unit = {
          val RequestData(rc, state, requestTimestamp, commitTime, repairContext) = item
          pp >> domainId
          pp >> rc
          pp >> state
          pp >> requestTimestamp
          pp >> commitTime
          pp >> repairContext
        }

        storage.profile match {
          case _: Profile.Postgres | _: Profile.H2 =>
            val query = """insert into
                 journal_requests(domain_id, request_counter, request_state_index, request_timestamp, commit_time, repair_context)
               values (?, ?, ?, ?, ?, ?)
               on conflict do nothing"""
            DbStorage.bulkOperation(query, items.map(_.value).toList, storage.profile)(setData)

          case _: Profile.Oracle =>
            val query =
              """merge /*+ INDEX (journal_requests pk_journal_requests) */
                |into journal_requests
                |using (select ? domain_id, ? request_counter from dual) input
                |on (journal_requests.request_counter = input.request_counter and
                |    journal_requests.domain_id = input.domain_id)
                |when not matched then
                |  insert (domain_id, request_counter, request_state_index, request_timestamp, commit_time, repair_context)
                |  values (input.domain_id, input.request_counter, ?, ?, ?, ?)""".stripMargin
            DbStorage.bulkOperation(query, items.map(_.value).toList, storage.profile)(setData)
        }
      }

      private val success: Try[Unit] = Success(())
      override protected def onSuccessItemUpdate(item: Traced[RequestData]): Try[Unit] = success

      override protected type CheckData = RequestData
      override protected type ItemIdentifier = RequestCounter
      override protected def itemIdentifier(item: RequestData): ItemIdentifier = item.rc
      override protected def dataIdentifier(state: CheckData): ItemIdentifier = state.rc

      override protected def checkQuery(itemsToCheck: NonEmpty[Seq[ItemIdentifier]])(implicit
          batchTraceContext: TraceContext
      ): immutable.Iterable[ReadOnly[immutable.Iterable[CheckData]]] =
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
            if (data == item) success
            else
              ErrorUtil.internalErrorTry(
                new IllegalStateException(
                  show"Conflicting data for request ${item.rc}: $item and $data"
                )
              )
        }

      override def prettyItem: Pretty[RequestData] = implicitly
    }

    BatchAggregator(processor, insertBatchAggregatorConfig, processingTime.some)
  }

  override def query(
      rc: RequestCounter
  )(implicit traceContext: TraceContext): OptionT[Future, RequestData] =
    processingTime.optionTEvent {
      val query =
        sql"""select request_counter, request_state_index, request_timestamp, commit_time, repair_context
              from journal_requests where request_counter = $rc and domain_id = $domainId"""
          .as[RequestData]
      OptionT(storage.query(query.headOption, functionFullName))
    }

  private def bulkQueryDbio(
      rcs: NonEmpty[Seq[RequestCounter]]
  ): immutable.Iterable[DbAction.ReadOnly[immutable.Iterable[RequestData]]] =
    DbStorage.toInClauses_("request_counter", rcs, maxItemsInSqlInClause).map { inClause =>
      import DbStorage.Implicits.BuilderChain.*
      val query =
        sql"""select request_counter, request_state_index, request_timestamp, commit_time, repair_context
              from journal_requests where domain_id = $domainId and """ ++ inClause
      query.as[RequestData]
    }

  override def firstRequestWithCommitTimeAfter(commitTimeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[RequestData]] = processingTime.event {
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
                    from journal_requests
                    where domain_id = $domainId and commit_time > $commitTimeExclusive)
                  select min(request_counter) from committed_after;
              """.as[Option[RequestCounter]].headOption.map(_.flatten),
            functionFullName + ".committed_after",
          )
          requestData <- rcMinCommittedAfterO.fold(Future.successful(Option.empty[RequestData]))(
            rc =>
              storage.query(
                sql"""
                    select request_counter, request_state_index, request_timestamp, commit_time, repair_context
                    from journal_requests
                    where domain_id = $domainId and request_counter = $rc
                """.as[RequestData].headOption,
                functionFullName,
              )
          )
        } yield requestData
      case _: Profile.Oracle | _: Profile.H2 =>
        storage.query(
          sql"""
                select request_counter, request_state_index, request_timestamp, commit_time, repair_context
                from journal_requests where domain_id = $domainId and commit_time > $commitTimeExclusive
                order by request_counter #${storage.limit(1)}
            """.as[RequestData].headOption,
          functionFullName,
        )
    }

  }

  override def replace(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      newState: RequestState,
      commitTime: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): EitherT[Future, RequestJournalStoreError, Unit] =
    if (commitTime.exists(_ < requestTimestamp))
      EitherT.leftT[Future, Unit](
        CommitTimeBeforeRequestTime(
          rc,
          requestTimestamp,
          commitTime.getOrElse(
            throw new RuntimeException("An Option guarded by an exists must contain a value")
          ),
        )
      )
    else {
      val request = ReplaceRequest(rc, requestTimestamp, newState, commitTime)
      EitherT(batchAggregatorReplace.run(request).flatMap(Future.fromTry))
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
      ): Future[Iterable[Try[Result]]] =
        bulkUpdateWithCheck(items, "DbRequestJournalStore.replace")(traceContext, self.closeContext)

      override protected def bulkUpdateAction(items: NonEmpty[Seq[Traced[ReplaceRequest]]])(implicit
          batchTraceContext: TraceContext
      ): DBIOAction[Array[Int], NoStream, Effect.All] = {
        val updateQuery =
          """update /*+ INDEX (journal_requests (request_counter, domain_id)) */ journal_requests
             set request_state_index = ?, commit_time = coalesce (?, commit_time)
             where domain_id = ? and request_counter = ? and request_timestamp = ?"""
        DbStorage.bulkOperation(updateQuery, items.map(_.value).toList, storage.profile) {
          pp => item =>
            val ReplaceRequest(rc, requestTimestamp, newState, commitTime) = item
            pp >> newState
            pp >> commitTime
            pp >> domainId
            pp >> rc
            pp >> requestTimestamp
        }
      }

      private val success: Try[Result] = Success(Right(()))
      override protected def onSuccessItemUpdate(item: Traced[ReplaceRequest]): Try[Result] =
        success

      override protected type CheckData = RequestData
      override protected type ItemIdentifier = RequestCounter
      override protected def itemIdentifier(item: ReplaceRequest): RequestCounter = item.rc
      override protected def dataIdentifier(state: RequestData): RequestCounter = state.rc

      override protected def checkQuery(itemsToCheck: NonEmpty[Seq[RequestCounter]])(implicit
          batchTraceContext: TraceContext
      ): immutable.Iterable[ReadOnly[immutable.Iterable[RequestData]]] = bulkQueryDbio(itemsToCheck)

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
              Success(Right(()))
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
      processingTime.some,
    )
  }

  @VisibleForTesting
  private[store] override def pruneInternal(
      beforeAndIncluding: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] =
    storage.update_(
      sqlu"""
    delete from journal_requests where request_timestamp <= $beforeAndIncluding and domain_id = $domainId
  """,
      functionFullName,
    )

  override def size(start: CantonTimestamp, end: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): Future[Int] = processingTime.event {
    storage
      .query(
        {
          import BuilderChain.*
          val endFilter = end.fold(sql"")(ts => sql" and request_timestamp <= $ts")
          (sql"""
             select 1
             from journal_requests where domain_id = $domainId and request_timestamp >= $start
            """ ++ endFilter).as[Int]
        },
        functionFullName,
      )
      .map(_.size)
  }

  override def deleteSince(
      fromInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val statement =
      sqlu"""
        delete from journal_requests where domain_id = $domainId and request_counter >= $fromInclusive
        """
    storage.update_(statement, functionFullName)
  }

  override def repairRequests(
      fromInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Seq[RequestData]] = {
    val statement =
      sql"""
        select request_counter, request_state_index, request_timestamp, commit_time, repair_context
        from journal_requests where domain_id = $domainId and request_counter >= $fromInclusive and repair_context is not null
        order by request_counter
        """.as[RequestData]
    storage.query(statement, functionFullName)
  }

  override def onClosed(): Unit = Lifecycle.close(cleanPreheadStore)(logger)
}

object DbRequestJournalStore {

  final case class ReplaceRequest(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      newState: RequestState,
      commitTime: Option[CantonTimestamp],
  ) extends PrettyPrinting {

    override def pretty: Pretty[ReplaceRequest] = prettyOfClass(
      param("rc", _.rc),
      param("new state", _.newState),
      param("request timestamp", _.requestTimestamp),
    )
  }
}
