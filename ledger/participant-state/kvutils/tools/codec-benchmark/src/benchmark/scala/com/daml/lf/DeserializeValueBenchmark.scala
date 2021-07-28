// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, EncodedValue}
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class DeserializeValueBenchmark extends BenchmarkWithLedgerExport {

  var protobufValues: Vector[ByteString] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    val encodedValues = submissions.values.map(_.value).toVector
    protobufValues = encodedValues.map(_.toByteString)
  }

  @Benchmark
  def run(): Vector[EncodedValue] =
    protobufValues.map(EncodedValue.deserialize)

}
