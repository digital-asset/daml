// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, EncodedValue}
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class SerializeValueBenchmark extends BenchmarkWithLedgerExport {

  var encodedValues: Vector[EncodedValue] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    encodedValues = submissions.values.map(_.value).toVector
  }

  @Benchmark
  def run(): Vector[ByteString] =
    encodedValues.map(_.toByteString)

}
