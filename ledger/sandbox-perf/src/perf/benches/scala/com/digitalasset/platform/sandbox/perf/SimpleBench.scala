// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import java.io.File

import akka.stream.scaladsl.Sink
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import org.openjdk.jmh.annotations.Benchmark

class SimpleBench extends DummyCommands with InfAwait {

  override protected def darFile: File = new File(rlocation("ledger/sandbox/Test.dar"))
  @Benchmark
  def ingest10kCommands(state: PerfBenchState): Unit = {
    val commandCount = 10000L
    await(
      dummyCreates(state.ledger.ledgerId)
        .take(commandCount)
        .mapAsync(100)(state.ledger.commandService.submitAndWait _)
        .runWith(Sink.ignore)(state.mat))
    ()
  }
}
