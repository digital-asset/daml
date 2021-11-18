// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.CompiledPackages
import com.daml.lf.benchmark.{BenchmarkWithLedgerExport, TypedValue, assertDecode}
import com.daml.lf.data.Time
import com.daml.lf.speedy.Speedy.Machine
import com.daml.lf.value.Value
import org.openjdk.jmh.annotations.{Benchmark, Setup}

class MachineImportValueBenchmark extends BenchmarkWithLedgerExport {

  private var machine: Speedy.Machine = _
  private var decodedValues: Vector[TypedValue[Value]] = _

  // Construct a machine for testing.
  private def testingMachine(
      compiledPackages: CompiledPackages
  ): Machine = Machine(
    compiledPackages = compiledPackages,
    submissionTime = Time.Timestamp.MinValue,
    initialSeeding = InitialSeeding.NoSeed,
    expr = null,
    committers = Set.empty,
    readAs = Set.empty,
  )

  @Setup
  override def setup(): Unit = {
    super.setup()
    decodedValues = submissions.values.map(_.mapValue(assertDecode(_).unversioned)).toVector
    machine = testingMachine(submissions.compiledPackages)
  }

  @Benchmark
  def run(): Vector[Unit] =
    decodedValues.map { case TypedValue(value, typ) => machine.importValue(typ, value) }

}
