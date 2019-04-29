// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.apitesting.LedgerFactories.SandboxStore
import com.digitalasset.platform.apitesting.{LedgerContext, LedgerFactories}
import com.digitalasset.platform.sandbox.utils.InfiniteRetries
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._

@State(Scope.Benchmark)
class PerfBenchState extends InfiniteRetries {

  private var akkaState: AkkaState = null
  private var server: Resource[LedgerContext] = null

  def config: PlatformApplications.Config = PlatformApplications.Config.default

  @Param(Array("Postgres"))
  var store: String = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    akkaState = new AkkaState()
    akkaState.setup()
    server = LedgerFactories.createSandboxResource(config, SandboxStore(store))(akkaState.esf)
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
    @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
    implicit val ec = system.dispatcher
    Await.result(
      for {
        _ <- server.value.ledgerIdentityService
          .getLedgerIdentity(GetLedgerIdentityRequest())
        _ <- server.value.reset()(system, mat)
        _ <- retry(
          server.value.ledgerIdentityService.getLedgerIdentity(GetLedgerIdentityRequest()))(system)
      } yield (),
      5.seconds
    )
  }

  def ledger: LedgerContext = server.value

  def mat: ActorMaterializer = akkaState.mat

  def system: ActorSystem = akkaState.sys

  def esf: ExecutionSequencerFactory = akkaState.esf

}
