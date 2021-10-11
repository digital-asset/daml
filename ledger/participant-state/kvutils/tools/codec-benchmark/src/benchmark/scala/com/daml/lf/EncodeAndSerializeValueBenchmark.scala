// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark._
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class EncodeAndSerializeValueBenchmark extends BenchmarkWithLedgerExport {

  var decodedValues: Vector[DecodedValue] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    decodedValues = submissions.values.map(_.value).map(assertDecode).toVector
  }

  @Benchmark
  def run(): Vector[com.google.protobuf.ByteString] =
    decodedValues.map(assertEncode(_).toByteString)

}
