// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import scalaz.{-\/, \/, \/-}

object LedgerOffsetUtil {

  private[http] val LongEitherLongLongOrdering: Ordering[Long \/ (Long, Long)] = {
    import scalaz.std.tuple._
    import scalaz.std.anyVal._
    scalaz.Order[Long \/ (Long, Long)].toScalaOrdering
  }

  implicit val AbsoluteOffsetOrdering: Ordering[LedgerOffset.Value.Absolute] =
    Ordering.by(parseOffset)(LongEitherLongLongOrdering)

  private def parseOffset(offset: LedgerOffset.Value.Absolute): Long \/ (Long, Long) = {
    parseOffsetString(offset.value)
  }

  private[http] def parseOffsetString(offset: String): Long \/ (Long, Long) = {
    offset.split('-') match {
      case Array(_, a2, a3) =>
        \/-((a2.toLong, a3.toLong))
      case Array(a1) =>
        -\/(a1.toLong)
      case _ =>
        throw new IllegalArgumentException(
          "Expected either numeric or composite offset in the format: '<block-hash>-<block-height>-<event-id>'," +
            s" got: ${offset: String}"
        )
    }
  }
}
