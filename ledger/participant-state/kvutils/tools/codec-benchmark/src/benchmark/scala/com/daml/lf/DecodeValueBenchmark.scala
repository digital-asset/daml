// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, DecodedValue, EncodedValue, assertDecode}
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class DecodeValueBenchmark extends BenchmarkWithLedgerExport {

  var encodedValues: Vector[EncodedValue] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    encodedValues = submissions.values.map(_.value).toVector
  }

  @Benchmark
  def run(): Vector[DecodedValue] =
    encodedValues.map(assertDecode)

}
