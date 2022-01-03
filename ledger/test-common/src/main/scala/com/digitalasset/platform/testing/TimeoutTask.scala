// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import java.util.TimerTask

import scala.concurrent.Promise

final class TimeoutTask[A](p: Promise[A]) extends TimerTask {
  override def run(): Unit = {
    val _ = p.tryFailure(TimeoutException)
  }
}
