// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.replay

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import com.daml.lf.data._
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class ReplayBenchmark {

  @Param(Array())
  // choiceName of the exercise to benchmark
  // format: "ModuleName:TemplateName:ChoiceName"
  var choiceName: String = _

  @Param(Array())
  // path of the darFile
  var darFile: String = _

  @Param(Array())
  // path of the ledger export
  var ledgerFile: String = _

  @Param(Array("false"))
  // if 'true' try to adapt the benchmark to the dar
  var adapt: Boolean = _

  private var readDarFile: Option[String] = None
  private var loadedPackages: Map[Ref.PackageId, Ast.Package] = _
  private var engine: Engine = _
  private var benchmarksFile: Option[String] = None
  private var benchmarks: Map[String, BenchmarkState] = _
  private var benchmark: BenchmarkState = _

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(): Unit = {
    val result = benchmark.replay(engine)
    assert(result.isRight)
  }

  @Setup(Level.Trial)
  def init(): Unit = {
    if (!readDarFile.contains(darFile)) {
      loadedPackages = Replay.loadDar(Paths.get(darFile))
      engine = Replay.compile(loadedPackages)
      readDarFile = Some(darFile)
    }
    if (!benchmarksFile.contains(ledgerFile)) {
      benchmarks = Replay.loadBenchmarks(Paths.get(ledgerFile))
      benchmarksFile = Some(ledgerFile)
    }

    benchmark =
      if (adapt)
        Replay.adapt(
          loadedPackages,
          engine.compiledPackages().packageLanguageVersion,
          benchmarks(choiceName),
        )
      else benchmarks(choiceName)

    // before running the bench, we validate the transaction first to be sure everything is fine.
    val result = benchmark.validate(engine)
    assert(result.isRight)
  }

}
