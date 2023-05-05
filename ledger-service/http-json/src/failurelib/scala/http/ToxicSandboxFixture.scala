// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.ports.{LockedFreePort, Port}
import com.daml.timer.RetryStrategy
import eu.rekawek.toxiproxy._
import io.grpc.Channel
import org.scalatest.{BeforeAndAfterEach, Suite}

import java.net.InetAddress
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.sys.process.Process

import com.daml.lf.integrationtest.CantonFixtureWithResource
import java.io.File
import java.nio.file.Path
import com.daml.platform.services.time.TimeProviderType
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.domain.LedgerId

// Fixture for Canton behind toxiproxy to simulate failures.
trait ToxicSandboxFixture
    extends CantonFixtureWithResource[(Channel, Port, ToxiproxyClient, Proxy)]
    with BeforeAndAfterEach {
  self: Suite =>

  protected def authSecret: Option[String] = None
  protected def darFiles: List[Path] = packageFiles.map(_.toPath)
  protected def devMode: Boolean = false
  protected def nParticipants: Int = 1
  protected def timeProviderType: TimeProviderType = TimeProviderType.WallClock
  protected def tlsEnable: Boolean = false
  protected def applicationId: ApplicationId = ApplicationId("toxic-proxy-sandbox")

  protected def packageFiles: List[File]

  protected def serverPort = ports.head
  protected def channel: Channel = additional._1
  protected def proxiedPort: Port = additional._2
  protected def proxyClient: ToxiproxyClient = additional._3
  protected def proxy: Proxy = additional._4

  override protected def beforeEach() = proxyClient.reset()

  protected def ledgerId: LedgerId = LedgerId("participant0")

  protected def makeToxiproxyResource(ledger: Port): ResourceOwner[(Port, ToxiproxyClient, Proxy)] =
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

  override protected def makeAdditionalResource(
      ports: Vector[Port]
  ): ResourceOwner[(Channel, Port, ToxiproxyClient, Proxy)] =
    for {
      channel <- GrpcClientResource.owner(ports.head)
      (port, client, proxy) <- makeToxiproxyResource(ports.head)
    } yield (channel, port, client, proxy)
}
