// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.dao.events.IdPageSizing
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection
import scala.concurrent.Future

private[platform] class PaginatingAsyncStream(
    override protected val loggerFactory: NamedLoggerFactory
) extends NamedLogging {

  import PaginatingAsyncStream.*

  private val directEc = DirectExecutionContext(noTracingLogger)

  /** Concatenates the results of multiple asynchronous calls into a single [[Source]], passing the
    * last seen event's offset to the next iteration query, so it can continue reading events from
    * this point.
    *
    * This is to implement pagination based on generic offset. The main purpose of the pagination is
    * to break down large queries into smaller batches. The reason for this is that we are currently
    * using simple blocking JDBC APIs and a long-running stream would end up occupying a thread in
    * the DB pool, severely limiting the ability of keeping multiple, concurrent, long-running
    * streams while serving lookup calls.
    *
    * @param startFromOffset
    *   initial offset
    * @param getOffset
    *   function that returns a position/offset from the element of type [[T]]
    * @param query
    *   a function that fetches results starting from provided offset
    * @tparam Off
    *   the type of the offset
    * @tparam T
    *   the type of the items returned in each call
    */
  def streamFromSeekPagination[Off, T](startFromOffset: Off, getOffset: T => Off)(
      query: Off => Future[Vector[T]]
  ): Source[T, NotUsed] =
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

  def streamIdsFromSeekPaginationWithoutIdFilter(
      idStreamName: String,
      idPageSizing: IdPageSizing,
      idPageBufferSize: Int,
      initialFromIdExclusive: Long,
      initialEndInclusive: Long,
  )(
      fetchPageDbQuery: Connection => PaginationInput => Vector[Long]
  )(
      executeIdQuery: (Connection => Vector[Long]) => Future[Vector[Long]]
  )(implicit
      traceContext: TraceContext
  ): Source[Long, NotUsed] = {
    assert(idPageBufferSize > 0)
    def wrapIdDbQuery(paginationInput: PaginationInput): Connection => Vector[Long] = { c =>
      val started = System.nanoTime()
      val result = fetchPageDbQuery(c)(paginationInput)
      def elapsedMillis: Long = (System.nanoTime() - started) / 1000000
      logger.debug(
        s"ID query for $idStreamName for IDs returned: limit:${paginationInput.limit} from:${paginationInput.startExclusive} #IDs:${result.size} lastID:${result.lastOption} DB query took: ${elapsedMillis}ms"
      )
      result
    }
    val initialState = IdPaginationState(
      fromIdExclusive = initialFromIdExclusive,
      pageSize = idPageSizing.minPageSize,
    )
    Source
      .unfoldAsync[IdPaginationState, Vector[Long]](initialState) { state =>
        executeIdQuery(
          wrapIdDbQuery(
            PaginationInput(
              startExclusive = state.fromIdExclusive,
              endInclusive = initialEndInclusive,
              limit = state.pageSize,
            )
          )
        ).map { ids =>
          ids.lastOption.map { last =>
            val nextState = IdPaginationState(
              fromIdExclusive = last,
              pageSize = Math.min(state.pageSize * 4, idPageSizing.maxPageSize),
            )
            nextState -> ids
          }
        }(directEc)
      }
      .buffer(idPageBufferSize, OverflowStrategy.backpressure)
      .mapConcat(identity)
  }

  def streamIdsFromSeekPaginationWithIdFilter(
      idStreamName: String,
      idPageSizing: IdPageSizing,
      idPageBufferSize: Int,
      initialFromIdExclusive: Long,
      initialEndInclusive: Long,
  )(
      fetchPageDbQuery: Connection => IdFilterPaginationInput => Vector[Long]
  )(
      executeLastIdQuery: (Connection => Vector[Long]) => Future[Vector[Long]],
      idFilterQueryParallelism: Int,
      executeIdFilterQuery: (Connection => Vector[Long]) => Future[Vector[Long]],
  )(implicit
      traceContext: TraceContext
  ): Source[Long, NotUsed] = {
    assert(idPageBufferSize > 0)
    def wrapIdDbQuery(
        idFilterPaginationInput: IdFilterPaginationInput
    )(debugLogMiddle: Vector[Long] => String): Connection => Vector[Long] = { c =>
      val started = System.nanoTime()
      val result = fetchPageDbQuery(c)(idFilterPaginationInput)
      def elapsedMillis: Long = (System.nanoTime() - started) / 1000000
      logger.debug(
        s"ID query for $idStreamName ${debugLogMiddle(result)} DB query took: ${elapsedMillis}ms"
      )
      result
    }
    def lastIdDbQuery(
        paginationLastOnlyInput: PaginationLastOnlyInput
    ): Connection => Vector[Long] =
      wrapIdDbQuery(paginationLastOnlyInput)(result =>
        s"for next ID window returned: limit:${paginationLastOnlyInput.limit} from:${paginationLastOnlyInput.startExclusive} to:$result"
      )
    def idFilterDbQuery(idFilterInput: IdFilterInput): Connection => Vector[Long] =
      wrapIdDbQuery(idFilterInput)(result =>
        s"for filtered IDs returned: from:${idFilterInput.startExclusive} to:${idFilterInput.endInclusive} #IDs:${result.size}"
      )
    val initialState = IdPaginationState(
      fromIdExclusive = initialFromIdExclusive,
      pageSize = idPageSizing.minPageSize,
    )
    Source
      .unfoldAsync[IdPaginationState, PaginationFromTo](initialState) { state =>
        executeLastIdQuery(
          lastIdDbQuery(
            PaginationLastOnlyInput(
              startExclusive = state.fromIdExclusive,
              endInclusive = initialEndInclusive,
              limit = state.pageSize,
            )
          )
        ).map { ids =>
          ids.lastOption.map { last =>
            val nextState = IdPaginationState(
              fromIdExclusive = last,
              pageSize = Math.min(state.pageSize * 4, idPageSizing.maxPageSize),
            )
            nextState -> PaginationFromTo(
              fromExclusive = state.fromIdExclusive,
              toInclusive = last,
            )
          }
        }(directEc)
      }
      .mapAsync(idFilterQueryParallelism)(paginationFromTo =>
        executeIdFilterQuery(
          idFilterDbQuery(
            IdFilterInput(
              startExclusive = paginationFromTo.fromExclusive,
              endInclusive = paginationFromTo.toInclusive,
            )
          )
        )
      )
      .buffer(idPageBufferSize, OverflowStrategy.backpressure)
      .mapConcat(identity)
  }
}

object PaginatingAsyncStream {

  final case class IdPaginationState(fromIdExclusive: Long, pageSize: Int)

  final case class PaginationFromTo(
      fromExclusive: Long,
      toInclusive: Long,
  )

  sealed trait IdFilterPaginationInput
  final case class PaginationInput(
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  ) extends IdFilterPaginationInput
  final case class IdFilterInput(
      startExclusive: Long,
      endInclusive: Long,
  ) extends IdFilterPaginationInput
  final case class PaginationLastOnlyInput(
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  ) extends IdFilterPaginationInput

}
