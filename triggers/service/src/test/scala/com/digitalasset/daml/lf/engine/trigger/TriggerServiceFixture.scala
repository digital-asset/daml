// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.io.File
import java.net.InetAddress
import java.time.Duration

import akka.actor.ActorSystem
import akka.actor.typed.{ActorSystem => TypedActorSystem}
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.daml.bazeltools.BazelRunfiles
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref._
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.{LockedFreePort, Port}
import com.daml.timer.RetryStrategy
import eu.rekawek.toxiproxy._

import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process.Process

object TriggerServiceFixture {

  // Use a small initial interval so we can test restart behaviour more easily.
  private val minRestartInterval = FiniteDuration(1, duration.SECONDS)

  def withTriggerService[A](
      testName: String,
      dars: List[File],
      encodedDar: Option[Dar[(PackageId, DamlLf.ArchivePayload)]],
      jdbcConfig: Option[JdbcConfig],
  )(testFn: (Uri, LedgerClient, Proxy) => Future[A])(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {
    val host = InetAddress.getLoopbackAddress
    val isWindows: Boolean = sys.props("os.name").toLowerCase.contains("windows")

    // Launch a Toxiproxy server. Wait on it to be ready to accept connections and
    // then create a client.
    val toxiproxyExe =
      if (!isWindows) BazelRunfiles.rlocation("external/toxiproxy_dev_env/bin/toxiproxy-cmd")
      else BazelRunfiles.rlocation("external/toxiproxy_dev_env/toxiproxy-server-windows-amd64.exe")
    val toxiproxyF: Future[(Process, ToxiproxyClient)] = for {
      toxiproxyPort <- Future(LockedFreePort.find())
      toxiproxyServer <- Future(
        Process(Seq(toxiproxyExe, "--port", toxiproxyPort.port.value.toString)).run())
      _ <- RetryStrategy.constant(attempts = 3, waitTime = 2.seconds)((_, _) =>
        Future(toxiproxyPort.testAndUnlock(host)))
      toxiproxyClient = new ToxiproxyClient(host.getHostName, toxiproxyPort.port.value)
    } yield (toxiproxyServer, toxiproxyClient)

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)
    val ledgerF = for {
      ledger <- Future(new SandboxServer(ledgerConfig(Port.Dynamic, dars, ledgerId), mat))
      ledgerPort <- ledger.portF
      ledgerProxyPort = LockedFreePort.find()
      (_, toxiproxyClient) <- toxiproxyF
      ledgerProxy = toxiproxyClient.createProxy(
        "sandbox",
        s"${host.getHostName}:${ledgerProxyPort.port}",
        s"${host.getHostName}:$ledgerPort")
      _ = ledgerProxyPort.unlock()
    } yield (ledger, ledgerPort, ledgerProxyPort, ledgerProxy)
    // 'ledgerProxyPort' is managed by the toxiproxy instance and
    // forwards to the real sandbox port.

    // Configure this client with the ledger's *actual* port.
    val clientF: Future[LedgerClient] = for {
      (_, ledgerPort, _, _) <- ledgerF
      client <- LedgerClient.singleHost(
        host.getHostName,
        ledgerPort.value,
        clientConfig(applicationId),
      )
    } yield client

    // Configure the service with the ledger's *proxy* port.
    val serviceF: Future[(ServerBinding, TypedActorSystem[Message])] = for {
      (_, _, ledgerProxyPort, _) <- ledgerF
      ledgerConfig = LedgerConfig(
        host.getHostName,
        ledgerProxyPort.port.value,
        TimeProviderType.Static,
        Duration.ofSeconds(30),
        ServiceConfig.DefaultMaxInboundMessageSize,
      )
      restartConfig = TriggerRestartConfig(
        minRestartInterval,
        ServiceConfig.DefaultMaxRestartInterval,
      )
      service <- ServiceMain.startServer(
        host.getHostName,
        Port(0).value,
        ledgerConfig,
        restartConfig,
        encodedDar,
        jdbcConfig,
        noSecretKey = true // That's ok, use the default.
      )
    } yield service

    // For adding toxics.
    val ledgerProxyF: Future[Proxy] = for {
      (_, _, _, ledgerProxy) <- ledgerF
    } yield ledgerProxy

    val fa: Future[A] = for {
      client <- clientF
      binding <- serviceF
      ledgerProxy <- ledgerProxyF
      uri = Uri.from(scheme = "http", host = "localhost", port = binding._1.localAddress.getPort)
      a <- testFn(uri, client, ledgerProxy)
    } yield a

    fa.onComplete { _ =>
      serviceF.foreach({ case (_, system) => system ! Stop })
      ledgerF.foreach(_._1.close())
      toxiproxyF.foreach(_._1.destroy)
    }

    fa
  }

  private def ledgerConfig(
      ledgerPort: Port,
      dars: List[File],
      ledgerId: LedgerId
  ): SandboxConfig =
    sandbox.DefaultConfig.copy(
      port = ledgerPort,
      damlPackages = dars,
      timeProviderType = Some(TimeProviderType.Static),
      ledgerIdMode = LedgerIdMode.Static(ledgerId),
      authService = None,
    )

  private def clientConfig[A](
      applicationId: ApplicationId,
      token: Option[String] = None): LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = None,
      token = token,
    )
}
