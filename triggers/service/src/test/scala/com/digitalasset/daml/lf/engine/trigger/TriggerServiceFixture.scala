// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.io.File
import java.time.Duration

import akka.actor.ActorSystem
import akka.actor.typed.{ActorSystem => TypedActorSystem}
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.daml.bazeltools.BazelRunfiles
import com.daml.timer.RetryStrategy
import org.scalatest.Assertions._

import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process.Process
import java.net.{InetAddress, ServerSocket, Socket}

import com.daml.lf.engine.trigger.dao.DbTriggerDao
import eu.rekawek.toxiproxy._

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

  def withTriggerService[A](
      testName: String,
      dars: List[File],
      dar: Option[Dar[(PackageId, Package)]],
      jdbcConfig: Option[JdbcConfig],
  )(testFn: (Uri, LedgerClient, Proxy) => Future[A])(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {
    // Launch a toxiproxy instance. Wait on it to be ready to accept
    // connections.
    val host = InetAddress.getLoopbackAddress
    val toxiProxyExe = BazelRunfiles.rlocation(System.getProperty("com.daml.toxiproxy"))
    val toxiProxyPort = findFreePort()
    val toxiProxyProc = Process(Seq(toxiProxyExe, "--port", toxiProxyPort.value.toString)).run()
    RetryStrategy.constant(attempts = 3, waitTime = 2.seconds) { (_, _) =>
      for {
        channel <- Future(new Socket(host, toxiProxyPort.value))
      } yield (channel.close())
    }
    val toxiProxyClient = new ToxiproxyClient(host.getHostName, toxiProxyPort.value);

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

    // Configure this client with the ledger's *actual* port.
    val clientF: Future[LedgerClient] = for {
      (_, ledgerPort, _, _) <- ledgerF
      client <- LedgerClient.singleHost(host.getHostName, ledgerPort, clientConfig(applicationId))
    } yield client

    // Construct a database DAO for initialization and clean up if provided a JDBC config.
    // Fail the test immediately if database initialization fails.
    val dbTriggerDao: Option[DbTriggerDao] =
      jdbcConfig.map(c => {
        val dao = DbTriggerDao(c)
        dao.initialize match {
          case Left(err) => fail(err)
          case Right(()) => dao
        }
      })

    // Configure the service with the ledger's *proxy* port.
    val serviceF: Future[(ServerBinding, TypedActorSystem[Server.Message])] = for {
      (_, _, ledgerProxyPort, _) <- ledgerF
      ledgerConfig = LedgerConfig(
        host.getHostName,
        ledgerProxyPort.value,
        TimeProviderType.Static,
        Duration.ofSeconds(30))
      service <- ServiceMain.startServer(
        host.getHostName,
        Port(0).value,
        ledgerConfig,
        ServiceConfig.DefaultMaxInboundMessageSize,
        ServiceConfig.DefaultMaxFailureNumberOfRetries,
        ServiceConfig.DefaultFailureRetryTimeRange,
        dar,
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
      serviceF.foreach({ case (_, system) => system ! Server.Stop })
      ledgerF.foreach(_._1.close())
      toxiProxyProc.destroy
      dbTriggerDao.foreach(dao =>
        dao.destroy match {
          case Left(err) => fail(err)
          case Right(()) =>
      })
    }

    fa
  }

  private def ledgerConfig(
      ledgerPort: Port,
      dars: List[File],
      ledgerId: LedgerId
  ): SandboxConfig =
    SandboxConfig.default.copy(
      port = ledgerPort,
      damlPackages = dars,
      timeProviderType = Some(TimeProviderType.Static),
      ledgerIdMode = LedgerIdMode.Static(ledgerId),
      authService = None
    )

  private def clientConfig[A](
      applicationId: ApplicationId,
      token: Option[String] = None): LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = None,
      token = token
    )
}
