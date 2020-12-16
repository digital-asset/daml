// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, DecodedValueWithType, assertDecode}
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.speedy.SValue
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class ValueTranslatorBenchmark extends BenchmarkWithLedgerExport {

  private var translator: ValueTranslator = _
  private var decodedValues: Vector[DecodedValueWithType] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    decodedValues = submissions.values.map(_.mapValue(assertDecode)).toVector
    translator = new ValueTranslator(submissions.compiledPackages)
  }

  @Benchmark
  def run(): Vector[SValue] =
    decodedValues.map(assertTranslate(translator))

}
