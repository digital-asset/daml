// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.http.HttpServiceTestFixture.UseTls
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ports.Port
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ledger.resources.ResourceContext
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandboxnext.Runner
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.Suite

import java.io.File
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

trait SandboxNextTestLedger extends SuiteResource[Port] {
  self: Suite =>

  protected def testId: String
  protected def packageFiles: List[File]
  protected def authService: Option[AuthService] = None

  implicit val resourceContext: ResourceContext

  def useTls: UseTls

  private val List(serverCrt, serverPem, caCrt, clientCrt, clientPem) = {
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      Some(new File(rlocation("ledger/test-common/test-certificates/" + src)))
    }
  }
  private val serverTlsConfig = TlsConfiguration(enabled = true, serverCrt, serverPem, caCrt)
  private val clientTlsConfig = TlsConfiguration(enabled = true, clientCrt, clientPem, caCrt)

  def ledgerId = LedgerId(testId)

  protected def config: SandboxConfig = SandboxConfig.defaultConfig.copy(
    port = Port.Dynamic,
    damlPackages = packageFiles,
    timeProviderType = Some(TimeProviderType.WallClock),
    tlsConfig = if (useTls) Some(serverTlsConfig) else None,
    ledgerIdMode = LedgerIdMode.Static(ledgerId),
    authService = authService,
    seeding = Some(Seeding.Weak),
  )

  def clientCfg(token: Option[String], testName: String): LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = testName,
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = if (useTls) clientTlsConfig.client() else None,
      token = token,
    )

  def usingLedger[A](testName: String, token: Option[String] = None)(
      testFn: (Port, DamlLedgerClient, LedgerId) => Future[A]
  )(implicit
      esf: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[A] = {

    val clientF: Future[DamlLedgerClient] = for {
      ledgerPort <- Future(serverPort)
    } yield DamlLedgerClient.singleHost(
      "localhost",
      ledgerPort.value,
      clientCfg(token, testName),
    )(ec, esf)

    val fa: Future[A] = for {
      ledgerPort <- Future(serverPort)
      client <- clientF
      a <- testFn(ledgerPort, client, ledgerId)
    } yield a

    fa
  }

  override protected lazy val suiteResource: Resource[Port] = {
    new OwnedResource[ResourceContext, Port](
      for {
        jdbcUrl <- SandboxBackend.H2Database.owner
          .map(info => Some(info.jdbcUrl))
        port <- new Runner(config.copy(jdbcUrl = jdbcUrl))
      } yield port,
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }

  protected def serverPort: Port = suiteResource.value

}
