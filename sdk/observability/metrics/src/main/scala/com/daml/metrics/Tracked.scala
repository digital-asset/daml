// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.concurrent.CompletionStage

import com.daml.metrics.api.MetricHandle.{Counter, Meter}
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.MetricsContext.withEmptyMetricsContext

import scala.concurrent.{ExecutionContext, Future}

object Tracked {

  def value[T](counter: Counter, value: => T): T = withEmptyMetricsContext {
    implicit metricContext =>
      counter.inc()
      val result = value
      counter.dec()
      result
  }

  def completionStage[T](
      counter: Counter,
      future: => CompletionStage[T],
  ): CompletionStage[T] =
    withEmptyMetricsContext { implicit metricsContext =>
      counter.inc()
      future.whenComplete { (_, _) =>
        counter.dec()
        ()
      }
    }

  def future[T](counter: Counter, future: => Future[T]): Future[T] = {
    counter.inc()
    future.andThen { case _ => counter.dec() }(ExecutionContext.parasitic)
  }

  def future[T](startMeter: Meter, completedMeter: Meter, future: => Future[T])(implicit
      context: MetricsContext
  ): Future[T] = {
    startMeter.mark()
    completedMeter.mark(0)
    future.andThen { case _ => completedMeter.mark() }(
      ExecutionContext.parasitic
    )
  }
}
