// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark._
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class DeserializeAndDecodeValueBenchmark extends BenchmarkWithLedgerExport {

  var protobufValues: Vector[com.google.protobuf.ByteString] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    protobufValues = submissions.values.map(_.value.toByteString).toVector
  }

  @Benchmark
  def run(): Vector[DecodedValue] =
    protobufValues.map(p => assertDecode(EncodedValue.deserialize(p)))

}
