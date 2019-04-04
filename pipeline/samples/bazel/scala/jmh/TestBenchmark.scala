// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package foo

import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

class TestBenchmark {
  @State(Scope.Benchmark)
  class BenchmarkState {
    val myScalaType = ScalaType(100)
    val myJavaType = new JavaType
  }

  @Benchmark
  def sumIntegersBenchmark: Int =
    AddNumbers.addUntil1000
}