// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import java.util.Timer

import scala.concurrent.{ExecutionContext, Future}

class TimerResourceOwner(acquireTimer: () => Timer) extends ResourceOwner[Timer] {
  override def acquire()(implicit _executionContext: ExecutionContext): Resource[Timer] =
    new Resource[Timer] {
      override protected val executionContext: ExecutionContext = _executionContext

      private val timer: Timer = acquireTimer()

      override protected val future: Future[Timer] = Future.successful(timer)

      override def releaseResource(): Future[Unit] = Future {
        timer.cancel()
      }
    }
}
