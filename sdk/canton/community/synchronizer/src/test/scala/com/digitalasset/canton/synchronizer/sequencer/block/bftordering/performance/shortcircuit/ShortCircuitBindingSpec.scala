// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.shortcircuit

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.{
  BftBenchmarkConfig,
  BftBenchmarkTool,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import pureconfig.ConfigSource
import pureconfig.generic.auto.*

class ShortCircuitBindingSpec extends AnyWordSpec with BaseTest with Matchers {

  "ShortCircuitBinding" should {
    "run" in {
      val config = ConfigSource
        .resources("bftbenchmark-shortcircuit.conf")
        .load[BftBenchmarkConfig]
        .getOrElse(throw new RuntimeException("Invalid configuration"))
      val metricReport = new BftBenchmarkTool(ShortCircuitBindingFactory, loggerFactory).run(config)

      val globalWritesCount =
        metricReport("global.writes.successful.meter.count").asInstanceOf[Long]
      val globalReadsCount = metricReport("global.reads.meter.count").asInstanceOf[Long]
      globalWritesCount should be > 0L
      globalReadsCount should be > 0L
      globalReadsCount should be(globalWritesCount)
      metricReport("global.writes.failed.meter.count").asInstanceOf[Long] should be(0)
    }
  }
}
