// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.TimerTask

import scala.concurrent.Promise

final class TimeoutTask[A](p: Promise[A]) extends TimerTask {
  override def run(): Unit = {
    val _ = p.tryFailure(TimeoutException)
  }
}
