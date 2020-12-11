package com.daml.lf

import com.daml.lf.benchmark._
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class EncodeTransactionBenchmark extends BenchmarkWithLedgerExport {

  var decodedTransactions: Vector[DecodedTransaction] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    val encodedTransactions = submissions.transactions
    decodedTransactions = encodedTransactions.map(assertDecode)
  }

  @Benchmark
  def run(): Vector[EncodedTransaction] =
    decodedTransactions.map(assertEncode)

}
