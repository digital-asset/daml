package com.daml.lf

import com.daml.lf.benchmark._
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class EncodeValueBenchmark extends BenchmarkWithLedgerExport {

  var decodedValues: Vector[DecodedValue] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    val encodedValues = submissions.values.map(_.value).toVector
    decodedValues = encodedValues.map(assertDecode)
  }

  @Benchmark
  def run(): Vector[EncodedValue] =
    decodedValues.map(assertEncode)

}
