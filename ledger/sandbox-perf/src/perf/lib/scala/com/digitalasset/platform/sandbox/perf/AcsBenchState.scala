// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import java.io.File

import akka.stream.scaladsl.Sink
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import org.openjdk.jmh.annotations.{Level, Setup}

class AcsBenchState extends PerfBenchState with DummyCommands with InfAwait {

  def commandCount = 10000L
  @Setup(Level.Invocation)
  def submitCommands(): Unit = {
    await(
      dummyCreates(ledger.ledgerId)
        .take(commandCount)
        .mapAsync(100)(ledger.commandService.submitAndWait)
        .runWith(Sink.ignore)(mat))
    ()
  }

  override protected def darFile: File = new File(rlocation("ledger/sandbox/Test.dar"))
}
