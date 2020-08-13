// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.{ExecutionContext, Future}

class CacheMetricsSpec extends AsyncWordSpec with Matchers {
  "gauge registration" should {
    "succeed on multiple threads in parallel for the same metric name" in {
      val cacheMetrics = new CacheMetrics(new MetricRegistry, MetricName.DAML)
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val instances =
        (1 to 1000).map(_ =>
          Future {
            cacheMetrics.registerSizeGauge(() => 1L)
            cacheMetrics.registerWeightGauge(() => 2L)
        })
      Future.sequence(instances).map { _ =>
        succeed
      }
    }
  }
}
