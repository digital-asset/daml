// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.replay

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import com.daml.lf.data._
import com.daml.lf.engine.Engine
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class ReplayBenchmark {

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
  // path of the ledger export
  var ledgerFile: String = _

  private var engine: Engine = _
  private var benchmark: BenchmarkState = _

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(): Unit = {
    val result = benchmark.replay(engine)
    assert(result.isRight)
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
    benchmark = Replay.loadBenchmark(
      Paths.get(ledgerFile),
      choice,
      choiceIndex,
    )
    if (darFile.nonEmpty) {
      val loadedPackages = Replay.loadDar(Paths.get(darFile))
      benchmark = Replay.adapt(loadedPackages, benchmark)
    }

    engine = Replay.compile(benchmark.pkgs)

    // before running the bench, we validate the transaction first to be sure everything is fine.
    val result = benchmark.validate(engine)
    remy.log(result)
    assert(result.isRight)
  }

}
