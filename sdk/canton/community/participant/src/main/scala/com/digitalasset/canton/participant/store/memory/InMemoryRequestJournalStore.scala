// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.store.CursorPreheadStore
import com.digitalasset.canton.store.memory.InMemoryCursorPreheadStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MapsUtil
import com.digitalasset.canton.{RequestCounter, RequestCounterDiscriminator}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class InMemoryRequestJournalStore(protected val loggerFactory: NamedLoggerFactory)
    extends RequestJournalStore
    with NamedLogging {

  override private[store] implicit val ec: ExecutionContext =
    DirectExecutionContext(noTracingLogger)

  private val requestTable = new TrieMap[RequestCounter, RequestData]

  override private[store] val cleanPreheadStore: CursorPreheadStore[RequestCounterDiscriminator] =
    new InMemoryCursorPreheadStore[RequestCounterDiscriminator](loggerFactory)

  override def insert(data: RequestData)(implicit traceContext: TraceContext): Future[Unit] =
    Future.fromTry(Try(MapsUtil.tryPutIdempotent(requestTable, data.rc, data)))

  override def query(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): OptionT[Future, RequestData] =
    OptionT.fromOption(requestTable.get(rc))

  override def firstRequestWithCommitTimeAfter(
      commitTimeExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Option[RequestData]] =
    Future.successful {
      requestTable.values.foldLeft(Option.empty[RequestData]) { (minSoFar, queryResult) =>
        if (queryResult.commitTime.forall(_ <= commitTimeExclusive)) minSoFar
        else if (minSoFar.forall(m => m.rc > queryResult.rc)) Some(queryResult)
        else minSoFar
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
      val resultE = requestTable.updateWith(rc) {
        case old @ Some(oldResult) =>
          if (oldResult.requestTimestamp != requestTimestamp) old
          else if (oldResult.state == newState && oldResult.commitTime == commitTime) old
          else Some(oldResult.tryAdvance(newState, commitTime))
        case None => None
      } match {
        case None => Left(UnknownRequestCounter(rc))
        case Some(possiblyNewResult) =>
          Either.cond(
            possiblyNewResult.requestTimestamp == requestTimestamp,
            (),
            InconsistentRequestTimestamp(rc, possiblyNewResult.requestTimestamp, requestTimestamp),
          )
      }
      EitherT.fromEither(resultE)
    }

  def delete(rc: RequestCounter)(implicit traceContext: TraceContext): Future[Unit] = {
    val oldState = requestTable.remove(rc)
    logger.debug(withRc(rc, s"Removed from the request journal. Old state $oldState"))
    Future.unit
  }

  @VisibleForTesting
  private[store] override def pruneInternal(
      beforeInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val before = requestTable.size
    val after = requestTable
      .filterInPlace((_, result) => result.requestTimestamp.isAfter(beforeInclusive))
      .size
    logger.info(s"Pruned ${before - after} contracts from the request journal")
    Future.unit
  }

  override def purge()(implicit traceContext: TraceContext): Future[Unit] = {
    requestTable.clear()
    Future.unit
  }

  def size(start: CantonTimestamp, end: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): Future[Int] = {
    val endPredicate =
      end
        .map(endTs => (ts: CantonTimestamp) => !ts.isAfter(endTs))
        .getOrElse((_: CantonTimestamp) => true)
    Future.successful(requestTable.count { case (_, result) =>
      !result.requestTimestamp.isBefore(start) && endPredicate(result.requestTimestamp)
    })
  }

  override def deleteSince(fromInclusive: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future.successful(requestTable.filterInPlace((rc, _) => rc < fromInclusive))

  override def repairRequests(
      fromInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Seq[RequestData]] = Future.successful {
    requestTable.values.iterator
      .filter(data => data.rc >= fromInclusive && data.repairContext.nonEmpty)
      .toSeq
      .sortBy(_.rc)
  }

  override def lastRequestCounterWithRequestTimestampBeforeOrAt(requestTimestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Future[Option[RequestCounter]] =
    Future.successful {
      requestTable.values.foldLeft(Option.empty[RequestCounter]) {
        (maxSoFar, queryResult: RequestData) =>
          if (queryResult.requestTimestamp > requestTimestamp) maxSoFar
          else if (maxSoFar.forall(_ < queryResult.rc)) Some(queryResult.rc)
          else maxSoFar
      }
    }

  private def withRc(rc: RequestCounter, msg: String): String = s"Request $rc: $msg"

  override def totalDirtyRequests()(implicit traceContext: TraceContext): Future[NonNegativeInt] =
    Future.successful(NonNegativeInt.tryCreate(requestTable.count { case (_, result) =>
      result.commitTime.isEmpty
    }))
}
