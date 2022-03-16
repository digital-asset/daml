// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package testing.snapshot

import com.daml.lf.data.Ref
import org.openjdk.jmh.annotations._

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
class ReplayBenchmark {

  @Param(Array())
  // choiceName of the exercise to benchmark
  // format: "ModuleName:TemplateName:ChoiceName"
  var choiceName: String = _

  @Param(Array("0"))
  var exerciseIndex: Int = _

  @Param(Array(""))
  // path of the darFile
  var darFile: String = _

  @Param(Array())
  // path of the ledger export
  var ledgerFile: String = _

  private var benchmark: TransactionSnapshot = _

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(): Unit =
    assert(benchmark.replay().isRight)

  @Setup(Level.Trial)
  def init(): Unit = {
    val Array(modNameStr, tmplNameStr, name) = choiceName.split(":")
    val choice = (
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(modNameStr),
        Ref.DottedName.assertFromString(tmplNameStr),
      ),
      Ref.Name.assertFromString(name),
    )
    benchmark = TransactionSnapshot.loadBenchmark(
      Paths.get(ledgerFile),
      choice,
      exerciseIndex,
      None,
    )
    if (darFile.nonEmpty) {
      val loadedPackages = TransactionSnapshot.loadDar(Paths.get(darFile))
      benchmark = benchmark.adapt(loadedPackages)
    }

    assert(benchmark.validate().isRight)
  }

}
