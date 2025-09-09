// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.Counter
import com.daml.metrics.api.MetricsContext
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.Flow

import scala.util.chaining.*

object InstrumentedGraph {

  /** A copy-paste of [[com.daml.metrics.InstrumentedGraph.BufferedFlow]] that allows passing the
    * [[com.daml.metrics.api.MetricsContext$]]
    *
    * It's a temporary fix, since the newest DAML version breaks some tests.
    */
  implicit class BufferedFlow[In, Out, Mat](val original: Flow[In, Out, Mat]) extends AnyVal {
    def buffered(counter: Counter, size: Int)(implicit
        metricsContext: MetricsContext = MetricsContext.Empty
    ): Flow[In, Out, Mat] =
      original
        // since wireTap is not guaranteed to be executed always, we need map to prevent counter skew over time.
        .map(_.tap(_ => counter.inc()))
        .buffer(size, OverflowStrategy.backpressure)
        .map(_.tap(_ => counter.dec()))
  }
}
