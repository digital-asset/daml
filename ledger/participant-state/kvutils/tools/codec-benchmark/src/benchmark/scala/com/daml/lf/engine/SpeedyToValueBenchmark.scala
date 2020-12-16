// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, DecodedValue, assertDecode}
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.speedy.SValue
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class SpeedyToValueBenchmark extends BenchmarkWithLedgerExport {

  private var speedyValues: Vector[SValue] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    val decodedValues = submissions.values.map(_.mapValue(assertDecode)).toVector
    val translator = new ValueTranslator(submissions.compiledPackages)
    speedyValues = decodedValues.map(assertTranslate(translator))
  }

  @Benchmark
  def run(): Vector[DecodedValue] =
    speedyValues.map(_.toValue)

}
