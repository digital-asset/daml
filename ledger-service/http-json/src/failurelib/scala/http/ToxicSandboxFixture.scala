// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.testing.utils.{OwnedResource, SuiteResource, Resource => TestResource}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.sandbox.SandboxOnXForTest.{ConfigAdaptor, dataSource}
import com.daml.ledger.sandbox.SandboxOnXRunner
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.{
  AbstractSandboxFixture,
  SandboxBackend,
  SandboxRequiringAuthorizationFuns,
  UploadPackageHelper,
}
import com.daml.ports.{LockedFreePort, Port}
import com.daml.timer.RetryStrategy
import eu.rekawek.toxiproxy._
import io.grpc.Channel
import org.scalatest.{BeforeAndAfterEach, Suite}

import java.net.InetAddress
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
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
            if (!isWindows)
              BazelRunfiles.rlocation("external/toxiproxy_dev_env/bin/toxiproxy-server")
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

  override protected lazy val suiteResource
      : TestResource[(Port, Channel, Port, ToxiproxyClient, Proxy)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Port, Channel, Port, ToxiproxyClient, Proxy)](
      for {
        jdbcUrl <- database
          .getOrElse(SandboxBackend.H2Database.owner)
          .map(info => info.jdbcUrl)
        cfg = config.withDataSource(dataSource(jdbcUrl))
        port <- SandboxOnXRunner.owner(ConfigAdaptor(authService), cfg, bridgeConfig)
        channel <- GrpcClientResource.owner(port)
        client = UploadPackageHelper.adminLedgerClient(port, cfg, jwtSecret)(
          system.dispatcher,
          executionSequencerFactory,
        )
        _ <- ResourceOwner.forFuture(() =>
          UploadPackageHelper.uploadDarFiles(client, packageFiles)(system.dispatcher)
        )
        (proxiedPort, proxyClient, proxy) <- toxiproxy(port)
      } yield (port, channel, proxiedPort, proxyClient, proxy),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }

  override protected def beforeEach() = proxyClient.reset()
}
