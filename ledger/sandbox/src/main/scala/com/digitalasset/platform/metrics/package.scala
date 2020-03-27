// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform

import com.codahale.metrics.Timer
import com.digitalasset.dec.DirectExecutionContext

import scala.concurrent.Future

package object metrics {

  def timedFuture[T](timer: Timer, future: => Future[T]): Future[T] = {
    val ctx = timer.time()
    val result = future
    result.onComplete(_ => ctx.stop())(DirectExecutionContext)
    result
  }

}
