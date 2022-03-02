// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class PackageSpec extends AsyncWordSpec with Matchers {
  "gauge registration" should {
    "succeed on multiple threads in parallel for the same metric name" in {
      val registry = new MetricRegistry
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val metricName = MetricName.Daml :+ "a" :+ "test"
      val instances =
        (1 to 1000).map(_ => Future(registerGauge(metricName, () => () => 1.0, registry)))
      Future.sequence(instances).map { _ =>
        succeed
      }
    }
  }
}
