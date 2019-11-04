// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.testing

import java.util.Timer

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}

object WithTimeout {

  private[this] val timer = new Timer("timeout-timer", true)

  def apply[A](t: Duration)(f: => Future[A]): Future[A] = {
    val p = Promise[A]
    timer.schedule(new TimeoutTask(p), t.toMillis)
    p.tryCompleteWith(f).future
  }

}
