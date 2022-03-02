// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class AkkaState {

  private var _sys: ActorSystem = null
  private var _mat: Materializer = null
  private var _esf: ExecutionSequencerFactory = null

  @Setup(Level.Trial)
  def setup(): Unit = {
    println("Starting Client Akka Infrastructure")
    _sys = ActorSystem()
    _mat = Materializer(_sys)
    _esf = new AkkaExecutionSequencerPool("clientPool")(sys)
  }

  @TearDown(Level.Trial)
  def close(): Unit = {
    println("Stopping Client Akka Infrastructure")
    _esf.close()
    _esf = null
    _mat.shutdown()
    _mat = null
    _sys.terminate() // does not wait
    _sys = null
  }

  def sys: ActorSystem = _sys

  def mat: Materializer = _mat

  def esf: ExecutionSequencerFactory = _esf
}
