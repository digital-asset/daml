// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.akkastreams.dispatcher

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

/** Defines how the progress on the ledger should be mapped to look-up operations */
@nowarn("msg=parameter value evidence.* is never used")
sealed abstract class SubSource[Index: Ordering, T]
    extends ((Index, Index) => Source[(Index, T), NotUsed]) {

  /** Returns a Source emitting items for the given range */
  def subSource(startExclusive: Index, endInclusive: Index): Source[(Index, T), NotUsed]

  override def apply(startExclusive: Index, endInclusive: Index): Source[(Index, T), NotUsed] =
    subSource(startExclusive, endInclusive)
}

object SubSource {

  /** Useful when range queries are not possible. For instance streaming a linked-list from Cassandra
    *
    * @param readSuccessor extracts the next index
    * @param readElement   reads the element on the given index
    */
  final case class OneAfterAnother[Index: Ordering, T](
      readSuccessor: Index => Index,
      readElement: Index => Future[T],
  ) extends SubSource[Index, T] {
    override def subSource(
        startExclusive: Index,
        endInclusive: Index,
    ): Source[(Index, T), NotUsed] = {
      Source
        .unfoldAsync[Index, (Index, T)](readSuccessor(startExclusive)) { index =>
          if (Ordering[Index].gt(index, endInclusive)) Future.successful(None)
          else {
            readElement(index).map { t =>
              val nextIndex = readSuccessor(index)
              Some((nextIndex, (index, t)))
            }(ExecutionContext.parasitic)
          }
        }
    }
  }

  /** Applicable when the persistence layer supports efficient range queries.
    *
    * @param getRange (startExclusive, endInclusive) => Source[(Index, T), NotUsed]
    */
  final case class RangeSource[Index: Ordering, T](
      getRange: (Index, Index) => Source[(Index, T), NotUsed]
  ) extends SubSource[Index, T] {
    override def subSource(
        startExclusive: Index,
        endInclusive: Index,
    ): Source[(Index, T), NotUsed] = getRange(startExclusive, endInclusive)
  }

}
