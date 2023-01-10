// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import java.io.File

import akka.stream.scaladsl.Sink
import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.test.ModelTestDar
import org.openjdk.jmh.annotations.Benchmark

class SimpleBenchState extends PerfBenchState with DummyCommands with InfAwait

class SimpleBench extends DummyCommands with InfAwait {

  override protected def darFile: File = new File(rlocation(ModelTestDar.path))

  @Benchmark
  def ingest10kCommands(state: SimpleBenchState): Unit = {
    val commandCount = 10000L
    await(
      dummyCreates(state.ledger.ledgerId)
        .take(commandCount)
        .mapAsync(100)(state.ledger.commandService.submitAndWait _)
        .runWith(Sink.ignore)(state.mat)
    )
    ()
  }
}
