// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.testing

import java.util.TimerTask

import scala.concurrent.Promise

final class TimeoutTask[A](p: Promise[A]) extends TimerTask {
  override def run(): Unit = {
    val _ = p.tryFailure(TimeoutException)
  }
}
