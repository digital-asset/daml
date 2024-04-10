// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import slick.jdbc.GetResult

/** A status and when it became effective.
  *
  * @param status The status
  * @param asOf When the change became effective
  */
final case class StateChange[+Status <: PrettyPrinting](status: Status, asOf: TimeOfChange)
    extends PrettyPrinting {

  def rc: RequestCounter = asOf.rc

  def timestamp: CantonTimestamp = asOf.timestamp

  override def pretty: Pretty[StateChange.this.type] = prettyOfClass(
    param("status", _.status),
    param("asOf", _.asOf),
  )
}

object StateChange {
  def apply[Status <: PrettyPrinting](
      status: Status,
      rc: RequestCounter,
      timestamp: CantonTimestamp,
  ): StateChange[Status] = {
    StateChange[Status](status, TimeOfChange(rc, timestamp))
  }

  implicit def stateChangeGetResult[A <: PrettyPrinting](implicit
      getResultStatus: GetResult[A]
  ): GetResult[StateChange[A]] =
    GetResult(r => StateChange(r.<<[A], r.<<[TimeOfChange]))
}
