// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, EncodedTransaction}
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class SerializeTransactionBenchmark extends BenchmarkWithLedgerExport {

  var encodedTransactions: Vector[EncodedTransaction] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    encodedTransactions = submissions.transactions
  }

  @Benchmark
  def run(): Vector[ByteString] =
    encodedTransactions.map(_.toByteString)

}
