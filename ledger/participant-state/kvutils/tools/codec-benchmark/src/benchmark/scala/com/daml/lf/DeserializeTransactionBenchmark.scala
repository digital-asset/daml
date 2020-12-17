// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, EncodedTransaction}
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class DeserializeTransactionBenchmark extends BenchmarkWithLedgerExport {

  var protobufTransactions: Vector[ByteString] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    val encodedTransactions = submissions.transactions
    protobufTransactions = encodedTransactions.map(_.toByteString)
  }

  @Benchmark
  def run(): Vector[EncodedTransaction] =
    protobufTransactions.map(EncodedTransaction.deserialize)

}
