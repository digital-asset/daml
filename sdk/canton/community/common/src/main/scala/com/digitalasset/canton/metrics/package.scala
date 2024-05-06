// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.data.EitherT
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle.Timer
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown

import scala.concurrent.Future

package object metrics {

  implicit class TimerExtensions(val timer: Timer) extends AnyVal {

    def timeEitherT[E, A](ev: EitherT[Future, E, A]): EitherT[Future, E, A] = {
      EitherT(Timed.future(timer, ev.value))
    }

    def timeEitherFUS[E, A](
        ev: EitherT[FutureUnlessShutdown, E, A]
    ): EitherT[FutureUnlessShutdown, E, A] = {
      EitherT(FutureUnlessShutdown(Timed.future(timer, ev.value.unwrap)))
    }
  }
}
