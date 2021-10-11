// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import java.io.File
import java.net.InetAddress

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.bazeltools.BazelRunfiles._
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.auth.client.LedgerCallCredentials
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.daml.ledger.client.services.testing.time.StaticTime
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.test.ModelTestDar
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.DbInfo
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import io.grpc.Channel
import org.scalatest.Suite
import scalaz.syntax.tag._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

trait AbstractSandboxFixture extends AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def darFile = new File(rlocation(ModelTestDar.path))

  protected def ledgerId(token: Option[String] = None): domain.LedgerId =
    domain.LedgerId(
      LedgerIdentityServiceGrpc
        .blockingStub(channel)
        .withCallCredentials(token.map(new LedgerCallCredentials(_)).orNull)
        .getLedgerIdentity(GetLedgerIdentityRequest())
        .ledgerId
    )

  protected def getTimeProviderForClient(implicit
      mat: Materializer,
      esf: ExecutionSequencerFactory,
  ): TimeProvider = {
    Try(TimeServiceGrpc.stub(channel))
      .map(StaticTime.updatedVia(_, ledgerId().unwrap)(mat, esf))
      .fold[TimeProvider](_ => TimeProvider.UTC, Await.result(_, 30.seconds))
  }

  protected def config: SandboxConfig =
    SandboxConfig.defaultConfig.copy(
      port = Port.Dynamic,
      damlPackages = packageFiles,
      timeProviderType = Some(TimeProviderType.Static),
      scenario = scenario,
      ledgerIdMode = LedgerIdMode.Static(LedgerId("sandbox-server")),
      seeding = Some(Seeding.Weak),
      engineMode = SandboxConfig.EngineMode.Dev,
      authService = authService,
    )

  protected def packageFiles: List[File] = List(darFile)

  protected def authService: Option[AuthService] = None

  protected def scenario: Option[String] = None

  protected def database: Option[ResourceOwner[DbInfo]] = None

  protected def serverHost: String = InetAddress.getLoopbackAddress.getHostName

  protected def serverPort: Port

  protected def channel: Channel
}
