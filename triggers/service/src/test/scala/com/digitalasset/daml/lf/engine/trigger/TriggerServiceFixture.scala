// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.io.File
import java.net.{InetAddress, ServerSocket, Socket}
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
import com.daml.ports.Port
import com.daml.timer.RetryStrategy
import eu.rekawek.toxiproxy._

import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process.Process

object TriggerServiceFixture {

  // Might throw IOException (unlikely). Best effort. There's a small
  // chance that having found one, it gets taken before we get to use
  // it.
  private def findFreePort(): Port = {
    val socket = new ServerSocket(Port(0).value)
    try {
      Port(socket.getLocalPort)
    } finally {
      socket.close()
    }
  }

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

    // Launch a toxiproxy instance. Wait on it to be ready to accept
    // connections.
    val toxiProxyExe =
      if (!isWindows)
        BazelRunfiles.rlocation("external/toxiproxy_dev_env/bin/toxiproxy-cmd")
      else
        BazelRunfiles.rlocation("external/toxiproxy_dev_env/toxiproxy-server-windows-amd64.exe")
    val toxiProxyPort = findFreePort()
    val toxiProxyProc = Process(Seq(toxiProxyExe, "--port", toxiProxyPort.value.toString)).run()
    RetryStrategy.constant(attempts = 3, waitTime = 2.seconds) { (_, _) =>
      for {
        channel <- Future(new Socket(host, toxiProxyPort.value))
      } yield channel.close()
    }
    val toxiProxyClient = new ToxiproxyClient(host.getHostName, toxiProxyPort.value)

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)
    val ledgerF = for {
      ledger <- Future(new SandboxServer(ledgerConfig(Port.Dynamic, dars, ledgerId), mat))
      sandboxPort <- ledger.portF
      ledgerPort = sandboxPort.value
      ledgerProxyPort = findFreePort()
      ledgerProxy = toxiProxyClient.createProxy(
        "sandbox",
        s"${host.getHostName}:$ledgerProxyPort",
        s"${host.getHostName}:$ledgerPort")
    } yield (ledger, ledgerPort, ledgerProxyPort, ledgerProxy)
    // 'ledgerProxyPort' is managed by the toxiproxy instance and
    // forwards to the real sandbox port.

    // Now we have a ledger port, launch a ref-ledger-authentication
    // instance.
    val refLedgerAuthExe: String =
      if (!isWindows)
        BazelRunfiles.rlocation("triggers/service/ref-ledger-authentication-binary")
      else
        BazelRunfiles.rlocation("triggers/service/ref-ledger-authentication-binary.exe")
    val refLedgerAuthProcF: Future[(Port, Process)] = for {
      refLedgerAuthPort <- Future { findFreePort() }
      (_, ledgerPort, _, _) <- ledgerF
      proc <- Future {
        Process(
          Seq(refLedgerAuthExe),
          None,
          ("DABL_AUTHENTICATION_SERVICE_ADDRESS", host.getHostAddress),
          ("DABL_AUTHENTICATION_SERVICE_PORT", refLedgerAuthPort.toString),
          (
            "DABL_AUTHENTICATION_SERVICE_LEDGER_URL",
            "http://" + host.getHostAddress + ":" + ledgerPort.toString)
        ).run()
      }
    } yield (refLedgerAuthPort, proc)
    // Wait on the ref-ledger-authentication instance to be ready to
    // accept connections.
    RetryStrategy.constant(attempts = 3, waitTime = 2.seconds) { (_, _) =>
      for {
        (refLedgerAuthPort, _) <- refLedgerAuthProcF
        channel <- Future(new Socket(host, refLedgerAuthPort.value))
      } yield channel.close()
    }

    // Configure this client with the ledger's *actual* port.
    val clientF: Future[LedgerClient] = for {
      (_, ledgerPort, _, _) <- ledgerF
      client <- LedgerClient.singleHost(host.getHostName, ledgerPort, clientConfig(applicationId))
    } yield client

    // Configure the service with the ledger's *proxy* port.
    val serviceF: Future[(ServerBinding, TypedActorSystem[Message])] = for {
      (_, _, ledgerProxyPort, _) <- ledgerF
      ledgerConfig = LedgerConfig(
        host.getHostName,
        ledgerProxyPort.value,
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
      refLedgerAuthProcF.foreach({ case (_, proc) => proc.destroy })
      ledgerF.foreach(_._1.close())
      toxiProxyProc.destroy
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
