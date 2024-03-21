// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.integrationtest.{CantonFixture, CantonFixtureWithResource}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ports.{LockedFreePort, Port}
import com.daml.timer.RetryStrategy
import eu.rekawek.toxiproxy._
import io.grpc.Channel
import org.scalatest.{BeforeAndAfterEach, Suite}
import org.scalatest.OptionValues._

import java.io.File
import java.net.InetAddress
import java.nio.file.Path
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.sys.process.Process

// Fixture for Canton behind toxiproxy to simulate failures.
trait ToxicSandboxFixture
    extends CantonFixtureWithResource[(Channel, Port, ToxiproxyClient, Proxy)]
    with BeforeAndAfterEach {
  self: Suite =>

  override lazy protected val darFiles: List[Path] = packageFiles.map(_.toPath)

  protected def packageFiles: List[File]

  protected def serverPort = ports.head
  protected def channel: Channel = additional._1
  protected def proxiedPort: Port = additional._2
  protected def proxyClient: ToxiproxyClient = additional._3
  protected def proxy: Proxy = additional._4

  override protected def beforeEach() = proxyClient.reset()

  protected def ledgerId: LedgerId = LedgerId(config.ledgerIds.headOption.value)

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
              rlocation("external/toxiproxy_dev_env/bin/toxiproxy-server")
            else
              rlocation(
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
      ports: Vector[CantonFixture.LedgerPorts]
  ): ResourceOwner[(Channel, Port, ToxiproxyClient, Proxy)] =
    for {
      channel <- config.channelResource(ports.head.ledgerPort)
      (port, client, proxy) <- makeToxiproxyResource(ports.head.ledgerPort)
    } yield (channel, port, client, proxy)
}
