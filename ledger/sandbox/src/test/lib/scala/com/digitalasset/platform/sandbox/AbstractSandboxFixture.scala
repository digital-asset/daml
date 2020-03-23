// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.File
import java.net.InetAddress

import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.TimeModel
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.auth.client.LedgerCallCredentials
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.ports.Port
import com.digitalasset.resources.ResourceOwner
import io.grpc.Channel
import org.scalatest.Suite
import scalaz.syntax.tag._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

trait AbstractSandboxFixture extends AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def darFile = new File(rlocation("ledger/test-common/Test-stable.dar"))

  protected def ledgerId(token: Option[String] = None): domain.LedgerId =
    domain.LedgerId(
      LedgerIdentityServiceGrpc
        .blockingStub(channel)
        .withCallCredentials(token.map(new LedgerCallCredentials(_)).orNull)
        .getLedgerIdentity(GetLedgerIdentityRequest())
        .ledgerId)

  protected def getTimeProviderForClient(
      implicit mat: Materializer,
      esf: ExecutionSequencerFactory
  ): TimeProvider = {
    Try(TimeServiceGrpc.stub(channel))
      .map(StaticTime.updatedVia(_, ledgerId().unwrap)(mat, esf))
      .fold[TimeProvider](_ => TimeProvider.UTC, Await.result(_, 30.seconds))
  }

  protected def config: SandboxConfig =
    SandboxConfig.default.copy(
      port = Port.Dynamic,
      damlPackages = packageFiles,
      timeProviderType = Some(TimeProviderType.Static),
      timeModel = TimeModel.reasonableDefault,
      scenario = scenario,
      ledgerIdMode = LedgerIdMode.Static(LedgerId("sandbox-server")),
    )

  protected def packageFiles: List[File] = List(darFile)

  protected def scenario: Option[String] = None

  protected def database: Option[ResourceOwner[String]] = None

  protected def serverHost: String = InetAddress.getLoopbackAddress.getHostName

  protected def serverPort: Port

  protected def channel: Channel
}
