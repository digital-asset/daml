// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.tracing.TraceContext

class TestingTimeService(clock: Clock, private val getSimClocks: () => Seq[SimClock]) {

  /** Test-only hook to advance time only on locally hosted domains and participants. This provides the illusion of a
    * single-system with a single clock on all participants and domains to those test and demo environments that
    * expect "legacy time service" support
    * @param currentTime current time that needs to match with actual, current time in order to allow changing the time
    * @param newTime time to set sim-clock to
    */
  def advanceTime(currentTime: CantonTimestamp, newTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Either[String, Unit] =
    (for {
      _ <- Either.cond(
        !newTime.isBefore(currentTime),
        (),
        s"New time ${newTime.toString} is before current time ${currentTime.toString}",
      )
      ledgerTime = clock.now
      _ <- Either.cond(
        ledgerTime.compareTo(currentTime) == 0,
        (),
        s"Specified current time ${currentTime.toString} does not match ledger time ${ledgerTime.toString}",
      )
      _ = getSimClocks().foreach(_.advanceTo(newTime))
    } yield ()).left.map(e => s"Cannot advance clock: $e")

}
