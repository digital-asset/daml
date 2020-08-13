// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.{ExecutionContext, Future}

class MetricsSpec extends AsyncWordSpec with Matchers {
  "gauge registration" should {
    "succeed on multiple threads in parallel for the same metric name" in {
      val metrics = new Metrics(new MetricRegistry)
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val metricName = MetricName.DAML :+ "a" :+ "test"
      val instances =
        (1 to 1000).map(_ => Future(metrics.gauge[Double](metricName, () => () => 1.0)))
      Future.sequence(instances).map { _ =>
        succeed
      }
    }
  }
}
