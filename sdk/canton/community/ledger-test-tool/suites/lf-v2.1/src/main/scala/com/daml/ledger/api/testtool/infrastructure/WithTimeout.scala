// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import java.util.Timer
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}

object WithTimeout {

  private[this] val timer = new Timer("timeout-timer", true)

  def apply[A](t: Duration)(f: => Future[A]): Future[A] = {
    val p = Promise[A]()
    timer.schedule(new TimeoutTask(p), t.toMillis)
    p.completeWith(f).future
  }

}
