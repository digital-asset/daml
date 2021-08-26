// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, assertDecode}
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class SpeedyToValueBenchmark extends BenchmarkWithLedgerExport {

  private var speedyValues: Vector[SValue] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    val decodedValues = submissions.values.map(_.mapValue(assertDecode)).toVector
    val translator =
      new ValueTranslator(
        interface = submissions.compiledPackages.interface,
        requireV1ContractId = false,
        requireV1ContractIdSuffix = false,
      )
    speedyValues = decodedValues.map(x => assertTranslate(translator)(x.mapValue(_.value)))
  }

  @Benchmark
  def run(): Vector[Value[ContractId]] =
    speedyValues.map(_.toUnnormalizedValue)

}
