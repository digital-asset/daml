// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class AkkaState {

  private var _sys: ActorSystem = null
  private var _mat: ActorMaterializer = null
  private var _esf: ExecutionSequencerFactory = null

  @Setup(Level.Trial)
  def setup(): Unit = {
    println("Starting Client Akka Infrastructure")
    _sys = ActorSystem()
    _mat = ActorMaterializer()(_sys)
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

  def mat: ActorMaterializer = _mat

  def esf: ExecutionSequencerFactory = _esf
}
