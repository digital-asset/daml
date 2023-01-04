// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import java.io.File

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.testing.utils.Resource
import com.daml.ledger.resources.ResourceContext
import com.daml.ledger.test.ModelTestDar
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._

@State(Scope.Benchmark)
abstract class PerfBenchState extends InfiniteRetries {

  def darFile: File = new File(rlocation(ModelTestDar.path))

  private var akkaState: AkkaState = _
  private var server: Resource[LedgerContext] = _

  // Unfortunately this must be a constant literal
  // Valid values are LedgerContext.mem and LedgerContext.sql
  @Param(Array("InMemory", "Postgres"))
  var store: String = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    akkaState = new AkkaState()
    akkaState.setup()
    server = LedgerFactories.createSandboxResource(store, List(darFile), akkaState)(
      ResourceContext(mat.executionContext)
    )
    server.setup()
  }

  @TearDown(Level.Trial)
  def close(): Unit = {
    server.close()
    server = null
    akkaState.close()
    akkaState = null
  }

  @TearDown(Level.Invocation)
  def reset(): Unit = {
    val _ = Await.result(server.value.reset()(system), 5.seconds)
  }

  def ledger: LedgerContext = server.value

  def mat: Materializer = akkaState.mat

  def system: ActorSystem = akkaState.sys

  def esf: ExecutionSequencerFactory = akkaState.esf

}
