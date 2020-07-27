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
import akka.http.scaladsl.model.Uri.Path
import akka.stream.Materializer
import com.daml.bazeltools.BazelRunfiles
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.JwksVerifier
import com.daml.ledger.api.auth.AuthServiceJWT
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
//import com.daml.ledger.client.LedgerClient
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
import scalaz.syntax.std.option._

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
      auth: Boolean,
  )(testFn: (Uri, Proxy) => Future[A])(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {
    val host = InetAddress.getLoopbackAddress
    val isWindows: Boolean = sys.props("os.name").toLowerCase.contains("windows")

    // Set up an authentication service if enabled
    val authServiceAdminLedger: Future[Option[(SandboxServer, Port)]] =
      if (!auth) Future(None)
      else {
        val adminLedgerId = LedgerId("auth-service-admin-ledger")
        for {
          ledger <- Future(
            new SandboxServer(
              SandboxConfig.defaultConfig.copy(
                port = Port.Dynamic,
                ledgerIdMode = LedgerIdMode.Static(adminLedgerId),
              ),
              mat))
          ledgerPort <- ledger.portF
        } yield Some((ledger, ledgerPort))
      }

    val authServiceBinaryLoc: String = {
      val extension = if (isWindows) ".exe" else ""
      BazelRunfiles.rlocation("triggers/service/ref-ledger-authentication-binary" + extension)
    }

    val authServiceInstanceF: Future[Option[(Process, Uri)]] =
      authServiceAdminLedger.flatMap {
        case None => Future(None)
        case Some((_, adminLedgerPort)) =>
          for {
            authServicePort <- Future(LockedFreePort.find())
            ledgerUri = Uri.from(
              scheme = "http",
              host = host.getHostAddress,
              port = adminLedgerPort.value)
            process <- Future {
              Process(
                Seq(authServiceBinaryLoc),
                None,
                ("DABL_AUTHENTICATION_SERVICE_ADDRESS", host.getHostAddress),
                ("DABL_AUTHENTICATION_SERVICE_PORT", authServicePort.port.toString),
                ("DABL_AUTHENTICATION_SERVICE_LEDGER_URL", ledgerUri.toString),
                ("DABL_AUTHENTICATION_SERVICE_TEST_MODE", "true") // Needed for initial authorize call with basic credentials
              ).run()
            }
            // Wait for the auth service instance to be ready to accept connections.
            _ <- RetryStrategy.constant(attempts = 10, waitTime = 4.seconds) ((_, _) =>
              Future(authServicePort.testAndUnlock(host)))
            authServiceBaseUrl = Uri.from(
              scheme = "http",
              host = host.getHostAddress,
              port = authServicePort.port.value)
          } yield Some((process, authServiceBaseUrl))
      }

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
      (_, toxiproxyClient) <- toxiproxyF
      authServiceInstance <- authServiceInstanceF
      authServiceBaseUrl = authServiceInstance.map(_._2)
      authServiceJwksUrl = authServiceBaseUrl.map(_.withPath(Path("/sa/jwks")))
      ledger <- Future(
        new SandboxServer(ledgerConfig(Port.Dynamic, dars, ledgerId, authServiceJwksUrl), mat))
      ledgerPort <- ledger.portF
      ledgerProxyPort = LockedFreePort.find()
      ledgerProxy = toxiproxyClient.createProxy(
        "sandbox",
        s"${host.getHostName}:${ledgerProxyPort.port}",
        s"${host.getHostName}:$ledgerPort")
      _ = ledgerProxyPort.unlock()
    } yield (ledger, ledgerPort, ledgerProxyPort, ledgerProxy, authServiceBaseUrl)
    // 'ledgerProxyPort' is managed by the toxiproxy instance and
    // forwards to the real sandbox port.

    // Configure this client with the ledger's *actual* port.
    // val clientF: Future[LedgerClient] = for {
    //   (_, ledgerPort, _, _, _) <- ledgerF
    //   client <- LedgerClient.singleHost(
    //     host.getHostName,
    //     ledgerPort.value,
    //     clientConfig(applicationId),
    //   )
    // } yield client

    // Configure the service with the ledger's *proxy* port.
    val serviceF: Future[(ServerBinding, TypedActorSystem[Message])] = for {
      (_, _, ledgerProxyPort, _, authServiceBaseUrl) <- ledgerF
      ledgerConfig = LedgerConfig(
        host.getHostName,
        ledgerProxyPort.port.value,
        ledgerId,
        TimeProviderType.Static,
        Duration.ofSeconds(30),
        ServiceConfig.DefaultMaxInboundMessageSize,
      )
      restartConfig = TriggerRestartConfig(
        minRestartInterval,
        ServiceConfig.DefaultMaxRestartInterval,
      )
      servicePort = LockedFreePort.find()
      service <- ServiceMain.startServer(
        host.getHostName,
        servicePort.port.value,
        ledgerConfig,
        restartConfig,
        encodedDar,
        jdbcConfig,
        noSecretKey = true,
        authServiceBaseUrl
      )
    } yield service

    // For adding toxics.
    val ledgerProxyF: Future[Proxy] = for {
      (_, _, _, ledgerProxy, _) <- ledgerF
    } yield ledgerProxy

    val fa: Future[A] = for {
//      client <- clientF
      binding <- serviceF
      ledgerProxy <- ledgerProxyF
      uri = Uri.from(scheme = "http", host = "localhost", port = binding._1.localAddress.getPort)
      a <- testFn(uri, ledgerProxy)
    } yield a

    fa.transformWith { ta =>
      for {
        se <- optErr(serviceF.flatMap {
          case (_, system) =>
            system ! Stop
            system.whenTerminated
        })
        le <- optErr(ledgerF.map(_._1.close()))
        te <- optErr(toxiproxyF.map {
          case (proc, _) =>
            proc.destroy()
            proc.exitValue() // destroy is async
        })
        ase <- optErr(authServiceInstanceF.map(_.map {
          case (proc, _) =>
            proc.destroy()
            proc.exitValue() // destroy is async
        }))
        ale <- optErr(authServiceAdminLedger.map(_.map(_._1.close())))
        result <- (ta.failed.toOption orElse se orElse le orElse te orElse ase orElse ale)
          .cata(Future.failed, Future fromTry ta)
      } yield result
    }
  }

  private def optErr(fut: Future[_])(implicit ec: ExecutionContext): Future[Option[Throwable]] =
    fut transform (te => scala.util.Success(te.failed.toOption))

  private def ledgerConfig(
      ledgerPort: Port,
      dars: List[File],
      ledgerId: LedgerId,
      authServiceJwksUrl: Option[Uri],
  ): SandboxConfig =
    sandbox.DefaultConfig.copy(
      port = ledgerPort,
      damlPackages = dars,
      timeProviderType = Some(TimeProviderType.Static),
      ledgerIdMode = LedgerIdMode.Static(ledgerId),
      authService = authServiceJwksUrl.map(url => AuthServiceJWT(JwksVerifier(url.toString))),
    )

  private def clientConfig(
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
