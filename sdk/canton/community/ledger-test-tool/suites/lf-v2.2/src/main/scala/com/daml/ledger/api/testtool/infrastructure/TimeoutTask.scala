// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import java.util.TimerTask
import scala.concurrent.Promise

final class TimeoutTask[A](p: Promise[A]) extends TimerTask {
  override def run(): Unit = {
    val _ = p.tryFailure(TimeoutException)
  }
}
