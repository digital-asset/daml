// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.logging.NamedLoggerFactory
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

  private val loggerFactory = NamedLoggerFactory.root

  @Setup(Level.Invocation)
  def setupIteration(): Unit = {
    interning = new StringInterningView(loggerFactory)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(value = 5)
  @Warmup(iterations = 5)
  @Measurement(iterations = 5)
  def run(): Unit = {
    Await.result(
      interning
        .update(stringCount)(BenchmarkState.loadStringInterningEntries(entries)),
      perfTestTimeout,
    )
  }
}
