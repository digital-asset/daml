// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark._
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class EncodeAndSerializeTransactionBenchmark extends BenchmarkWithLedgerExport {

  private[this] var decodedTransactions: Vector[DecodedTransaction] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    decodedTransactions = submissions.transactions.map(assertDecode)
  }

  @Benchmark
  def run(): Vector[com.google.protobuf.ByteString] =
    decodedTransactions.map(assertEncode(_).toByteString)

}
