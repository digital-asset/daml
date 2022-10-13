// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.noop

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricHandle.Timer.TimerStop

sealed case class NoOpTimer(name: String) extends Timer {
  override def update(duration: Long, unit: TimeUnit): Unit = ()
  override def update(duration: Duration): Unit = ()
  override def time[T](call: => T): T = call
  override def startAsync(): TimerStop = () => ()
}
