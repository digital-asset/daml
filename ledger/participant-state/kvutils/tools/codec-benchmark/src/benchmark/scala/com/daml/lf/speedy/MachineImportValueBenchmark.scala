// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.CompiledPackages
import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, DecodedValue, assertDecode}
import com.daml.lf.data.Time
import com.daml.lf.speedy.Speedy.Machine
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class MachineImportValueBenchmark extends BenchmarkWithLedgerExport {

  private var machine: Speedy.Machine = _
  private var decodedValues: Vector[DecodedValue] = _

  // Construct a machine for testing.
  private def testingMachine(
      compiledPackages: CompiledPackages,
  ): Machine = Machine(
    compiledPackages = compiledPackages,
    submissionTime = Time.Timestamp.MinValue,
    initialSeeding = InitialSeeding.NoSeed,
    expr = null,
    globalCids = Set.empty,
    committers = Set.empty,
  )

  @Setup
  override def setup(): Unit = {
    super.setup()
    decodedValues = submissions.values.map(_.value).map(assertDecode).toVector
    machine = testingMachine(submissions.compiledPackages)
  }

  @Benchmark
  def run(): Vector[Unit] =
    decodedValues.map(machine.importValue)

}
