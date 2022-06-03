// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.net.InetAddress
import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.testing.utils.{OwnedResource, SuiteResource, Resource => TestResource}
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.services.admin.PackageManagementClient
import com.daml.ledger.client.withoutledgerid.LedgerClient
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.{
  AbstractSandboxFixture,
  SandboxBackend,
  SandboxRequiringAuthorizationFuns,
}
import com.daml.ledger.sandbox.{BridgeConfigAdaptor, SandboxOnXRunner}
import com.daml.ports.{LockedFreePort, Port}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.sandbox.SandboxOnXForTest.{ConfigAdaptor, ParticipantId}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.timer.RetryStrategy
import com.google.protobuf
import eu.rekawek.toxiproxy._
import io.grpc.Channel
import org.scalatest.{BeforeAndAfterEach, Suite}

import java.io.File
import java.nio.file.Files
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.Process

// Fixture for Sandbox Next behind toxiproxy to simulate failures.
trait ToxicSandboxFixture
    extends AbstractSandboxFixture
    with SandboxRequiringAuthorizationFuns
    with SuiteResource[(Port, Channel, Port, ToxiproxyClient, Proxy)]
    with BeforeAndAfterEach {
  self: Suite =>
  private def toxiproxy(ledger: Port): ResourceOwner[(Port, ToxiproxyClient, Proxy)] =
    new ResourceOwner[(Port, ToxiproxyClient, Proxy)] {
      val host = InetAddress.getLoopbackAddress
      val isWindows: Boolean = sys.props("os.name").toLowerCase.contains("windows")
      override def acquire()(implicit
          context: ResourceContext
      ): Resource[(Port, ToxiproxyClient, Proxy)] = {
        def start(): Future[(Port, ToxiproxyClient, Proxy, Process)] = {
          val toxiproxyExe =
            if (!isWindows) BazelRunfiles.rlocation("external/toxiproxy_dev_env/bin/toxiproxy-cmd")
            else
              BazelRunfiles.rlocation(
                "external/toxiproxy_dev_env/toxiproxy-server-windows-amd64.exe"
              )
          for {
            toxiproxyPort <- Future(LockedFreePort.find())
            toxiproxyServer <- Future(
              Process(Seq(toxiproxyExe, "--port", toxiproxyPort.port.value.toString)).run()
            )
            _ <- RetryStrategy.constant(attempts = 3, waitTime = 2.seconds)((_, _) =>
              Future(toxiproxyPort.testAndUnlock(host))
            )
            toxiproxyClient = new ToxiproxyClient(host.getHostName, toxiproxyPort.port.value)
            ledgerProxyPort = LockedFreePort.find()
            proxy = toxiproxyClient.createProxy(
              "ledger",
              s"${host.getHostName}:${ledgerProxyPort.port}",
              s"${host.getHostName}:$ledger",
            )
            _ = ledgerProxyPort.unlock()
          } yield (ledgerProxyPort.port, toxiproxyClient, proxy, toxiproxyServer)
        }
        def stop(r: (Port, ToxiproxyClient, Proxy, Process)) = Future {
          r._4.destroy()
          val _ = r._4.exitValue()
          ()
        }
        Resource(start())(stop).map { case (port, toxiproxyClient, proxy, _) =>
          (port, toxiproxyClient, proxy)
        }
      }
    }
  override protected def serverPort = suiteResource.value._1
  override protected def channel: Channel = suiteResource.value._2
  protected def proxiedPort: Port = suiteResource.value._3
  protected def proxyClient: ToxiproxyClient = suiteResource.value._4
  protected def proxy: Proxy = suiteResource.value._5

  private def adminLedgerClient(
      port: Port,
      config: com.daml.ledger.runner.common.Config,
  ): LedgerClient = {
    val sslContext = config.participants.head._2.apiServer.tls.flatMap(_.client())
    val clientConfig = LedgerClientConfiguration(
      applicationId = "admin-client",
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = Some(toHeader(adminTokenStandardJWT)),
    )
    com.daml.ledger.client.withoutledgerid.LedgerClient.singleHost(
      hostIp = "localhost",
      port = port.value,
      configuration = clientConfig,
      channelConfig = LedgerClientChannelConfiguration(sslContext),
    )(
      system.dispatcher,
      executionSequencerFactory,
    )
  }

  private def uploadDarFiles(
      client: PackageManagementClient,
      files: List[File],
  )(implicit
      ec: ExecutionContext
  ): Future[List[Unit]] =
    if (files.isEmpty) Future.successful(List())
    else
      Future.sequence(files.map(uploadDarFile(client)))

  private def uploadDarFile(client: PackageManagementClient)(file: File): Future[Unit] =
    client.uploadDarFile(
      protobuf.ByteString.copyFrom(Files.readAllBytes(file.toPath))
    )

  private def uploadDarFiles(
      client: LedgerClient,
      files: List[File],
  )(implicit
      ec: ExecutionContext
  ): Future[List[Unit]] =
    uploadDarFiles(client.packageManagementClient, files)

  override protected lazy val suiteResource
      : TestResource[(Port, Channel, Port, ToxiproxyClient, Proxy)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Port, Channel, Port, ToxiproxyClient, Proxy)](
      for {
        jdbcUrl <- database
          .getOrElse(SandboxBackend.H2Database.owner)
          .map(info => info.jdbcUrl)
        participantDataSource = Map(ParticipantId -> ParticipantDataSourceConfig(jdbcUrl))
        cfg = config.copy(
          dataSource = participantDataSource
        )
        configAdaptor: BridgeConfigAdaptor = new ConfigAdaptor(
          authService
        )
        port <- SandboxOnXRunner.owner(configAdaptor, cfg, bridgeConfig)
        channel <- GrpcClientResource.owner(port)
        client = adminLedgerClient(port, cfg)
        _ <- ResourceOwner.forFuture(() => uploadDarFiles(client, packageFiles)(system.dispatcher))
        (proxiedPort, proxyClient, proxy) <- toxiproxy(port)
      } yield (port, channel, proxiedPort, proxyClient, proxy),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }

  override protected def beforeEach() = proxyClient.reset()
}
