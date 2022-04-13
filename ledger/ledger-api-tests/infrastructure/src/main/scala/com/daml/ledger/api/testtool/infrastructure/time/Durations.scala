// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.time

import scala.concurrent.duration.{Duration, FiniteDuration}

object Durations {

  def scaleDuration(duration: FiniteDuration, timeoutScaleFactor: Double): FiniteDuration =
    asFiniteDuration(
      duration * timeoutScaleFactor
    )

  def asFiniteDuration(duration: Duration): FiniteDuration =
    duration match {
      case duration: FiniteDuration => duration
      case _ =>
        throw new IllegalArgumentException(s"Duration $duration is not finite.")
    }

}
