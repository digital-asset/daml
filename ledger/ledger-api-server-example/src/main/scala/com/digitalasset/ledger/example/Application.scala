// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.example

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.ledger.server.LedgerApiServer.LedgerApiServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.SandboxResetService
import com.digitalasset.platform.server.services.testing.TimeServiceBackend
import com.digitalasset.platform.services.time.TimeProviderType
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.io.Source

/**
  * This is an example Ledger Server API application, which uses the custom LedgerBackend
  * to provide the DAML ledger api.
  */
object Application {

  val version = "0.0.1"

  private val logger = LoggerFactory.getLogger(this.getClass)

  class Server(actorSystemName: String, config: => SandboxConfig) extends AutoCloseable {

    @volatile private var system: ActorSystem = _
    @volatile private var materializer: ActorMaterializer = _
    @volatile private var server: LedgerApiServer = _
    @volatile private var ledgerId: String = _
    @volatile private var shutdownTasks: () => Unit = () => ()

    @volatile var port: Int = config.port

    def getMaterializer: ActorMaterializer = materializer

    /** the reset service is special, since it triggers a server shutdown
      * TODO this is deprecated and expected to be handled more cleanly in the future. */
    private val resetService: SandboxResetService = new SandboxResetService(
      () => ledgerId,
      () => server.getServer,
      () => materializer.executionContext,
      () => {
        shutdownTasks()
        server.closeAllServices()
      },
      () => {
        server.close() // fully tear down the old server.
        buildAndStartServer()
      },
    )

    private def buildAndStartServer(): Unit = {
      implicit val mat: ActorMaterializer = materializer
      implicit val ec: ExecutionContext = mat.system.dispatcher

      ledgerId = config.ledgerIdMode.ledgerId()

      val mbLedgerTime: Option[Instant] = None // TODO

      val (timeProvider, timeServiceBackendO: Option[TimeServiceBackend]) =
        (mbLedgerTime, config.timeProviderType) match {
          case (None, TimeProviderType.WallClock) => (TimeProvider.UTC, None)
          case (None, _) =>
            val ts = TimeServiceBackend.simple(Instant.EPOCH)
            (ts, Some(ts))
          case (Some(ledgerTime), _) =>
            val ts = TimeServiceBackend.simple(ledgerTime)
            (ts, Some(ts))
        }

      val ledgerBackend = new Backend(
        ledgerId,
        config.damlPackageContainer,
        config.timeModel,
        timeProvider,
        materializer)

      server = LedgerApiServer(
        ledgerBackend,
        timeProvider,
        Engine(),
        config,
        timeServiceBackendO,
        Some(resetService),
      )

      shutdownTasks = () => ledgerBackend.shutdownTasks()

      // NOTE(JM): Re-use the same port after reset.
      port = server.port

      // see the resources folder
      showBanner("banner.x.txt")

      logger.info(
        "Initialized Ledger API server version {} with ledger-id = {}, port = {}, dar file = {}, time mode = {}, daml-engine = {}",
        version,
        ledgerId,
        port.toString,
        config.damlPackageContainer: AnyRef,
        config.timeProviderType
      )
    }

    def start(): Unit = {
      system = ActorSystem(actorSystemName)
      materializer = ActorMaterializer()(system)
      buildAndStartServer()
    }

    override def close(): Unit = {
      shutdownTasks()
      Option(server).foreach(_.close())
      Option(materializer).foreach(_.shutdown())
      Option(system).foreach(s => Await.result(s.terminate(), 30.seconds))
    }
  }

  def apply(config: => SandboxConfig): Server = {

    new Server(
      "LedgerApiServer",
      config
    )
  }

  def showBanner(resourceName: String): Unit = {
    if (getClass.getClassLoader.getResource(resourceName) != null)
      Console.out.println(
        Source
          .fromResource(resourceName)
          .getLines
          .mkString("\n"))
  }

}
