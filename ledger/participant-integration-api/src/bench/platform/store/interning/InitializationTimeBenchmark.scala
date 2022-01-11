// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interning

import com.daml.logging.LoggingContext
import org.openjdk.jmh.annotations.{
  Benchmark,
  BenchmarkMode,
  Fork,
  Level,
  Measurement,
  Mode,
  OutputTimeUnit,
  Setup,
  Warmup,
}

import java.util.concurrent.TimeUnit
import scala.concurrent.Await

class InitializationTimeBenchmark extends BenchmarkState {
  @Setup(Level.Invocation)
  def setupIteration(): Unit = {
    interning = BenchmarkState.createInterning(entries)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(value = 5)
  @Warmup(iterations = 5)
  @Measurement(iterations = 5)
  def run(): Unit = {
    Await.result(interning.update(stringCount)(LoggingContext.ForTesting), perfTestTimeout)
  }
}
