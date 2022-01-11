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
  Setup,
  Warmup,
}

import scala.concurrent.Await

class UpdateTimeBenchmark extends BenchmarkState {
  // Set up some extra entries for the repeated update() calls
  override def extraStringCount = 10000000

  @Setup(Level.Iteration)
  def setupIteration(): Unit = {
    interning = BenchmarkState.createInterning(entries)

    interningEnd = stringCount
    Await.result(interning.update(interningEnd)(LoggingContext.ForTesting), perfTestTimeout)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @Fork(value = 5)
  @Warmup(iterations = 5)
  @Measurement(iterations = 5)
  def run(): Unit = {
    interningEnd = interningEnd + 1
    if (interningEnd > entries.length) throw new RuntimeException("Can't ingest any more strings")

    Await.result(interning.update(interningEnd)(LoggingContext.ForTesting), perfTestTimeout)
  }
}
