// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.shortcircuit

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.{
  BftBenchmarkConfig,
  BftBenchmarkTool,
}
import pureconfig.ConfigSource
import pureconfig.generic.auto.*

object ShortcircuitBenchmarkTool extends App {

  private val config = ConfigSource
    .resources("bftbenchmark-shortcircuit.conf")
    .load[BftBenchmarkConfig]
    .getOrElse(throw new RuntimeException("Invalid configuration"))

  new BftBenchmarkTool(ShortCircuitBindingFactory).run(config)
}
