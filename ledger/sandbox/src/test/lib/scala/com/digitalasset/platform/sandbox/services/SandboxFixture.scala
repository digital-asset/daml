// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.io.File

import akka.stream.Materializer
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.{Resource, SuiteResource}
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.{TimeModel, TimeProviderType}
import io.grpc.Channel
import org.scalatest.Suite

import scalaz.syntax.tag._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

import com.digitalasset.ledger.api.domain.LedgerId

trait SandboxFixture extends SuiteResource[Channel] {
  self: Suite =>

  protected def darFile = new File(rlocation("ledger/test-common/Test-stable.dar"))

  protected def channel: Channel = suiteResource.value

  protected def ledgerIdOnServer: domain.LedgerId =
    domain.LedgerId(
      LedgerIdentityServiceGrpc
        .blockingStub(channel)
        .getLedgerIdentity(GetLedgerIdentityRequest())
        .ledgerId)

  protected def getTimeProviderForClient(
      implicit mat: Materializer,
      esf: ExecutionSequencerFactory): TimeProvider = {
    Try(TimeServiceGrpc.stub(channel))
      .map(StaticTime.updatedVia(_, ledgerIdOnServer.unwrap)(mat, esf))
      .fold[TimeProvider](_ => TimeProvider.UTC, Await.result(_, 30.seconds))
  }

  protected def config: SandboxConfig =
    SandboxConfig.default
      .copy(
        port = 0, //dynamic port allocation
        damlPackages = packageFiles,
        timeProviderType = TimeProviderType.Static,
        timeModel = TimeModel.reasonableDefault,
        scenario = scenario,
        ledgerIdMode = LedgerIdMode.Static(LedgerId("sandbox-server"))
      )

  protected def packageFiles: List[File] = List(darFile)

  protected def scenario: Option[String] = None

  protected def ledgerId: domain.LedgerId = ledgerIdOnServer

  private lazy val sandboxResource = new SandboxServerResource(config)

  protected override lazy val suiteResource: Resource[Channel] = sandboxResource

  def getSandboxPort: Int = sandboxResource.getPort

}
