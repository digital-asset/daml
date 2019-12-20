// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import com.codahale.metrics.Timer
import com.digitalasset.dec.DirectExecutionContext

import scala.concurrent.Future

package object metrics {
  def timedFuture[T](timer: Timer, f: => Future[T]): Future[T] = {
    val ctx = timer.time()
    val res = f
    res.onComplete(_ => ctx.stop())(DirectExecutionContext)
    res
  }
}
