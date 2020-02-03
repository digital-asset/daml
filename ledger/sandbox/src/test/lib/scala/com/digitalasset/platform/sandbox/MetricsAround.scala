// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import com.codahale.metrics.MetricRegistry
import org.scalatest.BeforeAndAfterAll

trait MetricsAround extends BeforeAndAfterAll {
  self: org.scalatest.Suite =>

  @volatile protected var metrics: MetricRegistry = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    metrics = new MetricRegistry
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }
}
