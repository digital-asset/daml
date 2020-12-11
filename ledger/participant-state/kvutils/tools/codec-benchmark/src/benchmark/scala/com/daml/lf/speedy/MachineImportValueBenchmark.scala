// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, DecodedValue, assertDecode}
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class MachineImportValueBenchmark extends BenchmarkWithLedgerExport {

  private var machine: Speedy.Machine = _
  private var decodedValues: Vector[DecodedValue] = _

  @Setup
  override def setup(): Unit = {
    super.setup()
    decodedValues = submissions.values.map(_.value).map(assertDecode).toVector
    machine = Speedy.Machine.dummy(submissions.compiledPackages)
  }

  @Benchmark
  def run(): Vector[Unit] =
    decodedValues.map(machine.importValue)

}
