// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.dao.events.IdPageSizing
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

private[platform] class PaginatingAsyncStream(
    override protected val loggerFactory: NamedLoggerFactory
) extends NamedLogging {

  import PaginatingAsyncStream.*

  private val directEc = DirectExecutionContext(noTracingLogger)

  /** Concatenates the results of multiple asynchronous calls into
    * a single [[Source]], injecting the offset of the next page to
    * retrieve for every call.
    *
    * This is designed to work with database limit/offset pagination and
    * in particular to break down large queries intended to serve streams
    * into smaller ones. The reason for this is that we are currently using
    * simple blocking JDBC APIs and a long-running stream would end up
    * occupying a thread in the DB pool, severely limiting the ability
    * of keeping multiple, concurrent, long-running streams while serving
    * lookup calls.
    *
    * This is not designed to page through results using the "seek method":
    * https://use-the-index-luke.com/sql/partial-results/fetch-next-page
    *
    * @param pageSize  number of items to retrieve per call
    * @param queryPage takes the offset from which to start the next page and returns that page
    * @tparam T the type of the items returned in each call
    */
  def streamFromLimitOffsetPagination[T](
      pageSize: Int
  )(queryPage: Long => Future[Vector[T]]): Source[T, NotUsed] = {
    Source
      .unfoldAsync(Option(0L)) {
        case None => Future.successful(None)
        case Some(queryOffset) =>
          queryPage(queryOffset).map { result =>
            val resultSize = result.size.toLong
            val newQueryOffset = if (resultSize < pageSize) None else Some(queryOffset + pageSize)
            Some(newQueryOffset -> result)
          }(directEc)
      }
      .flatMapConcat(Source(_))
  }

  /** Concatenates the results of multiple asynchronous calls into
    * a single [[Source]], passing the last seen event's offset to the
    * next iteration query, so it can continue reading events from this point.
    *
    * This is to implement pagination based on generic offset.
    * The main purpose of the pagination is to break down large queries
    * into smaller batches. The reason for this is that we are currently using
    * simple blocking JDBC APIs and a long-running stream would end up
    * occupying a thread in the DB pool, severely limiting the ability
    * of keeping multiple, concurrent, long-running streams while serving
    * lookup calls.
    *
    * @param startFromOffset initial offset
    * @param getOffset       function that returns a position/offset from the element of type [[T]]
    * @param query           a function that fetches results starting from provided offset
    * @tparam Off the type of the offset
    * @tparam T   the type of the items returned in each call
    */
  def streamFromSeekPagination[Off, T](startFromOffset: Off, getOffset: T => Off)(
      query: Off => Future[Vector[T]]
  ): Source[T, NotUsed] = {
    Source
      .unfoldAsync(Option(startFromOffset)) {
        case None =>
          Future.successful(None) // finished reading the whole thing
        case Some(offset) =>
          query(offset).map { result =>
            val nextPageOffset: Option[Off] = result.lastOption.map(getOffset)
            Some((nextPageOffset, result))
          }(directEc)
      }
      .flatMapConcat(Source(_))
  }

  def streamIdsFromSeekPagination(
      idPageSizing: IdPageSizing,
      idPageBufferSize: Int,
      initialFromIdExclusive: Long,
  )(
      fetchPage: IdPaginationState => Future[Vector[Long]]
  ): Source[Long, NotUsed] = {
    assert(idPageBufferSize > 0)
    val initialState = IdPaginationState(
      fromIdExclusive = initialFromIdExclusive,
      pageSize = idPageSizing.minPageSize,
    )
    Source
      .unfoldAsync[IdPaginationState, Vector[Long]](initialState) { state =>
        fetchPage(state).map { ids =>
          ids.lastOption.map(last => {
            val nextState = IdPaginationState(
              fromIdExclusive = last,
              pageSize = Math.min(state.pageSize * 4, idPageSizing.maxPageSize),
            )
            nextState -> ids
          })
        }(directEc)
      }
      .buffer(idPageBufferSize, OverflowStrategy.backpressure)
      .mapConcat(identity)
  }
}

object PaginatingAsyncStream {

  final case class IdPaginationState(fromIdExclusive: Long, pageSize: Int)
}
