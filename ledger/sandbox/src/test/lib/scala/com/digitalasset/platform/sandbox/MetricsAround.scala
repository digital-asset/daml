package com.digitalasset.platform.sandbox

import com.digitalasset.platform.sandbox.metrics.MetricsManager
import org.scalatest.BeforeAndAfterAll

trait MetricsAround extends BeforeAndAfterAll {
  self: org.scalatest.Suite =>

  @volatile implicit var metricsManager: MetricsManager = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    metricsManager = MetricsManager()
  }

  override protected def afterAll(): Unit = {
    metricsManager.close()
    super.afterAll()
  }
}
