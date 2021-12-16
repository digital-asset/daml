// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.codahale.metrics.MetricRegistry
import org.scalatest.BeforeAndAfterAll

trait MetricsAround extends BeforeAndAfterAll {
  self: org.scalatest.Suite =>

  @volatile protected var metricRegistry: MetricRegistry = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    metricRegistry = new MetricRegistry
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }
}
