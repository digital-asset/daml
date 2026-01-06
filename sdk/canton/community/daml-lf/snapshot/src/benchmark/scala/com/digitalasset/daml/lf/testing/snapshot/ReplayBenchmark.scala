// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.speedy.metrics.{StepCount, TxNodeCount}
import com.digitalasset.daml.lf.value.ContractIdVersion
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

  @Param(Array("V1"))
  // the contract ID version to use for locally created contracts
  var contractIdVersion: String = _

  private var benchmark: TransactionSnapshot = _

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(counters: ReplayBenchmark.EventCounter): Unit = {
    counters.reset()
    val result = benchmark.replay()
    assert(result.isRight)
    val Right(metrics) = result
    counters.stepCount += metrics.totalCount[StepCount].get
    counters.transactionNodeCount += metrics.totalCount[TxNodeCount].get
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
    val contractIdVersionParsed = contractIdVersion match {
      case "V1" => ContractIdVersion.V1
      case "V2" => ContractIdVersion.V2
    }
    benchmark = TransactionSnapshot.loadBenchmark(
      dumpFile = Paths.get(entriesFile),
      choice = choice,
      index = choiceIndex,
      profileDir = None,
      contractIdVersion = contractIdVersionParsed,
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
