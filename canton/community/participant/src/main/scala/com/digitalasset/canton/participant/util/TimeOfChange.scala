// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.{LocalOffset, RequestOffset}
import com.digitalasset.canton.util.OptionUtil
import slick.jdbc.GetResult

/** The time when a change of state has happened.
  *
  * @param rc The request counter on the request that triggered the change
  * @param timestamp The timestamp when this change takes place.
  */
final case class TimeOfChange(rc: RequestCounter, timestamp: CantonTimestamp)
    extends PrettyPrinting {
  def asLocalOffset: LocalOffset = RequestOffset(timestamp, rc)

  override def pretty: Pretty[TimeOfChange] = prettyOfClass(
    param("request", _.rc),
    param("timestamp", _.timestamp),
  )
}

object TimeOfChange {
  implicit val orderingTimeOfChange: Ordering[TimeOfChange] =
    Ordering.by[TimeOfChange, (CantonTimestamp, RequestCounter)](toc => (toc.timestamp, toc.rc))

  implicit val getResultTimeOfChange: GetResult[TimeOfChange] = GetResult { r =>
    val ts = r.<<[CantonTimestamp]
    val rc = r.<<[RequestCounter]
    TimeOfChange(rc, ts)
  }

  implicit val getResultOptionTimeOfChange: GetResult[Option[TimeOfChange]] = GetResult(r =>
    OptionUtil.zipWith(
      GetResult[Option[RequestCounter]].apply(r),
      GetResult[Option[CantonTimestamp]].apply(r),
    )(TimeOfChange.apply)
  )
}
