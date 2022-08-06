// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import java.time.Duration
import java.util.{Timer, TimerTask}

import com.daml.ledger.resources.ResourceOwner

trait TimeoutSupport {
  def startTimeout(timeout: Duration)(onTimeout: () => Unit): StartedTimeout
}

trait StartedTimeout {
  def cancel(): Unit
}

object TimeoutSupport {
  def owner(timerThreadName: String): ResourceOwner[TimeoutSupport] =
    ResourceOwner
      .forTimer(() => new Timer(timerThreadName, true))
      .map(new TimeSupportImpl(_))
}

private[tracking] class TimeSupportImpl(timer: Timer) extends TimeoutSupport {
  override def startTimeout(timeout: Duration)(onTimeout: () => Unit): StartedTimeout = {
    val timeoutTimerTask = new TimerTask {
      override def run(): Unit = onTimeout()
    }
    timer.schedule(timeoutTimerTask, timeout.toMillis)
    () => {
      timeoutTimerTask.cancel()
      ()
    }
  }
}
