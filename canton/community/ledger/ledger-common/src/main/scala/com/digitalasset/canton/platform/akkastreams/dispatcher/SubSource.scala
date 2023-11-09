// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.akkastreams.dispatcher

import akka.NotUsed
import akka.stream.scaladsl.Source

/** Defines how the progress on the ledger should be mapped to look-up operations */
sealed abstract class SubSource[Index: Ordering, T]
    extends ((Index, Index) => Source[(Index, T), NotUsed]) {

  /** Returns a Source emitting items for the given range */
  def subSource(startExclusive: Index, endInclusive: Index): Source[(Index, T), NotUsed]

  override def apply(startExclusive: Index, endInclusive: Index): Source[(Index, T), NotUsed] =
    subSource(startExclusive, endInclusive)
}

object SubSource {

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
