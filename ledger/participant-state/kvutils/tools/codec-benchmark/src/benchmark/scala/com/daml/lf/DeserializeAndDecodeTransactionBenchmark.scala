// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark._
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class DeserializeAndDecodeTransactionBenchmark extends BenchmarkWithLedgerExport {

  private[this] var protobufTransactions: Vector[com.google.protobuf.ByteString] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    protobufTransactions = submissions.transactions.map(_.toByteString)
  }

  @Benchmark
  def run(): Vector[DecodedTransaction] =
    protobufTransactions.map(p => assertDecode(EncodedTransaction.deserialize(p)))

}
