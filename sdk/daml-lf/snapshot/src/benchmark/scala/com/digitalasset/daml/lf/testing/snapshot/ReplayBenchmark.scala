// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data.Ref
import org.openjdk.jmh.annotations._

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
class ReplayBenchmark {

  @Param(Array("true", "false"))
  var gasOn: Boolean = _

  @Param(Array())
  // choiceName of the exercise to benchmark
  // format: "ModuleName:TemplateName:ChoiceName"
  var choiceName: String = _

  @Param(Array("0"))
  var choiceIndex: Int = _

  @Param(Array(""))
  // path of the darFile
  var darFile: String = _

  @Param(Array())
  // path of the ledger entries - i.e. path to file of saved transaction trees
  var entriesFile: String = _

  private var benchmark: TransactionSnapshot = _

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(counters: ReplayBenchmark.EventCounter): Unit = {
    counters.reset()
    val result = benchmark.replay()
    assert(result.isRight)
    val Right(metrics) = result
    counters.stepCount += {
      val (stepBatchCount, stepCount) = metrics.totalStepCount
      stepBatchCount * metrics.batchSize + stepCount
    }
    counters.transactionNodeCount += metrics.transactionNodeCount
  }

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
      dumpFile = Paths.get(entriesFile),
      choice = choice,
      index = choiceIndex,
      profileDir = None,
      gasBudget = Some(Long.MaxValue).filter(_ => gasOn),
    )
    if (darFile.nonEmpty) {
      val loadedPackages = TransactionSnapshot.loadDar(Paths.get(darFile))
      benchmark = benchmark.copy(pkgs = loadedPackages)
    }

    assert(benchmark.validate().isRight)
  }

}

object ReplayBenchmark {
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  class EventCounter {
    var stepCount: Long = 0

    var transactionNodeCount: Long = 0

    def reset(): Unit = {
      stepCount = 0
      transactionNodeCount = 0
    }
  }
}
