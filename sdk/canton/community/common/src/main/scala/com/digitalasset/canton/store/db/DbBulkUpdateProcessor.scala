// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.store.db.DbBulkUpdateProcessor.BulkUpdatePendingCheck
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil, SingleUseCell}
import slick.dbio.{DBIOAction, Effect, NoStream}

import java.sql.Statement
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Implementation of aggregated bulk update operations for DB stores */
trait DbBulkUpdateProcessor[A, B] extends BatchAggregator.Processor[A, Try[B]] {

  protected implicit def executionContext: ExecutionContext
  protected def storage: DbStorage

  /** Run the [[bulkUpdateAction]] for the given `items` and then check the reported update row counts.
    * For items where the update row count is not 1, look what is in the store and produce a corresponding
    * response by comparing the item with the found data.
    *
    * @return An [[scala.collection.Iterable]] of the same size as `items` that contains the response for `items(i)` is at index `i`.
    */
  protected def bulkUpdateWithCheck(items: NonEmpty[Seq[Traced[A]]], queryBaseName: String)(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[Iterable[Try[B]]] = {
    val bulkUpdate = bulkUpdateAction(items)
    for {
      updateCounts <- storage.queryAndUpdate(bulkUpdate, s"$queryBaseName update")
      updateCountAnalysis = analyzeUpdateCounts(items.toList, updateCounts)
      (toCheck, resultsOrCells) = updateCountAnalysis
      _ <- checkReplacements(toCheck, queryBaseName)
    } yield {
      resultsOrCells.map(_.valueOr { case BulkUpdatePendingCheck(item, cell) =>
        cell.getOrElse {
          implicit val loggingContext: ErrorLoggingContext =
            ErrorLoggingContext.fromTracedLogger(logger)(item.traceContext)
          implicit val pretty: Pretty[A] = prettyItem
          val msg = show"Failed check for update of ${item.value}"
          ErrorUtil.internalErrorTry(new RuntimeException(msg))
        }
      })
    }
  }

  protected def bulkUpdateWithCheck(items: Seq[Traced[A]], queryBaseName: String)(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[Iterable[Try[B]]] =
    NonEmpty.from(items) match {
      case Some(itemsNel) => bulkUpdateWithCheck(itemsNel, queryBaseName)
      case None => Future.successful(Iterable.empty[Try[B]])
    }

  /** Idempotent bulk DB operation for the given items. */
  protected def bulkUpdateAction(items: NonEmpty[Seq[Traced[A]]])(implicit
      batchTraceContext: TraceContext
  ): DBIOAction[Array[Int], NoStream, Effect.All]

  /** What to return for an item when the bulk operation returns an update count of 1 */
  protected def onSuccessItemUpdate(item: Traced[A]): Try[B]

  /** Inspect the update counts (same order as the items).
    * If exactly one row was updated, everything is fine.
    * In case of 0 rows or no information, we should check whether the update actually happened
    * (e.g., if the affected rows were underreported).
    *
    * To that end, return two sequences:
    * <ol>
    *   <li>The list of items to check in the DB: [[BulkUpdatePendingCheck]].
    *       The [[com.digitalasset.canton.util.SingleUseCell]] is a placeholder for the result of the check.</li>
    *   <li>A list of outcomes, which is the same length as items / updateCounts.
    *       The outcome is either a `Try[B]` or the [[BulkUpdatePendingCheck]].</li>
    * <ul>
    */
  private def analyzeUpdateCounts(
      items: Seq[Traced[A]],
      updateCounts: Array[Int],
  ): (
      Seq[BulkUpdatePendingCheck[A, B]],
      Seq[Either[BulkUpdatePendingCheck[A, B], Try[B]]],
  ) = {
    // Contains all the returned `Left`s to avoid having to look for them again.
    val failedUpdatesB = Seq.newBuilder[BulkUpdatePendingCheck[A, B]]
    val resultsOrCells = items.lazyZip(updateCounts).map { (item, count) =>
      count match {
        case 1 => Right(onSuccessItemUpdate(item))
        case 0 | Statement.SUCCESS_NO_INFO | Statement.EXECUTE_FAILED =>
          val pending = BulkUpdatePendingCheck(item, new SingleUseCell[Try[B]])
          failedUpdatesB.addOne(pending)
          Left(pending)
        case n =>
          implicit val loggingContext: ErrorLoggingContext =
            ErrorLoggingContext.fromTracedLogger(logger)(item.traceContext)
          implicit val pretty: Pretty[A] = prettyItem
          val ex = new IllegalStateException(
            show"Bulk update modified $n != 0,1 rows for $kind ${item.value}"
          )
          Right(ErrorUtil.internalErrorTry(ex))
      }
    }
    failedUpdatesB.result() -> resultsOrCells
  }

  private def checkReplacements(
      toCheck: Seq[BulkUpdatePendingCheck[A, B]],
      queryBaseName: String,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[Unit] = {
    NonEmpty.from(toCheck) match {
      case None => Future.unit
      case Some(toCheckNE) =>
        val ids = toCheckNE.map(x => itemIdentifier(x.target.value))
        val lookupQueries = checkQuery(ids)
        storage.sequentialQueryAndCombine(lookupQueries, s"$queryBaseName lookup").map {
          foundDatas =>
            val foundData = foundDatas.map(data => dataIdentifier(data) -> data).toMap
            toCheck.foreach { case BulkUpdatePendingCheck(item, cell) =>
              val response =
                analyzeFoundData(item.value, foundData.get(itemIdentifier(item.value)))(
                  item.traceContext
                )
              cell.putIfAbsent(response).discard[Option[Try[B]]]
            }
        }
    }
  }

  /** Type of data returned when checking what information the store contains for a given item. */
  protected type CheckData

  /** The type of identifier that is used to correlate items and the information retrieved from the store when checking. */
  protected type ItemIdentifier

  /** Identifier selector for items */
  protected def itemIdentifier(item: A): ItemIdentifier

  /** Identifier selector for stored data */
  protected def dataIdentifier(state: CheckData): ItemIdentifier

  /** A list of queries for the items that we want to check for */
  protected def checkQuery(itemsToCheck: NonEmpty[Seq[ItemIdentifier]])(implicit
      batchTraceContext: TraceContext
  ): immutable.Iterable[DbAction.ReadOnly[immutable.Iterable[CheckData]]]

  /** Compare the item against the data that was found in the store and produce a result.
    * It is called for each item that the update command returned an update counter not equal to 1.
    */
  protected def analyzeFoundData(item: A, foundData: Option[CheckData])(implicit
      traceContext: TraceContext
  ): Try[B]
}

object DbBulkUpdateProcessor {
  final case class BulkUpdatePendingCheck[A, B](
      target: Traced[A],
      cell: SingleUseCell[Try[B]],
  )
}
