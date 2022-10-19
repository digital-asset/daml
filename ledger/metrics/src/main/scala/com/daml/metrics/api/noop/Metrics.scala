// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.noop

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricsContext

sealed case class NoOpTimer(name: String) extends Timer {
  override def update(duration: Long, unit: TimeUnit)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Unit = ()
  override def update(duration: Duration)(implicit
      context: MetricsContext
  ): Unit = ()
  override def time[T](call: => T)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): T = call
  override def startAsync()(implicit
      context: MetricsContext = MetricsContext.Empty
  ): TimerHandle = () => ()
}
