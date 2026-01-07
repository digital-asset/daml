// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins.tinyproxy

import better.files.{File, Resource}
import com.digitalasset.canton.admin.api.client.data.NodeStatus
import com.digitalasset.canton.config.*
import com.digitalasset.canton.console.RemoteParticipantReference
import com.digitalasset.canton.integration.plugins.UseExternalProcess
import com.digitalasset.canton.integration.plugins.tinyproxy.UseTinyProxy.{
  PROCESS_NAME,
  TinyProxyConfig,
}
import com.digitalasset.canton.integration.util.BackgroundRunnerHandler
import com.digitalasset.canton.integration.{EnvironmentSetupPlugin, TestConsoleEnvironment}
import com.digitalasset.canton.{BaseTest, UniquePortGenerator}

import scala.concurrent.duration.*

/** Test plugin for using tinyproxy to place a proxy in front of a node.
  */
final case class UseTinyProxy(tinyProxyConfig: TinyProxyConfig)
    extends EnvironmentSetupPlugin
    with BaseTest {
  private val backgroundProcessHandler = new BackgroundRunnerHandler[Unit](timeouts, loggerFactory)
  private val tinyProxyPort = UniquePortGenerator.next.unwrap

  // We run proxied nodes in an external process so we can set the https.Proxy... system parameters
  // on that node only without messing with the rest of the tests / nodes
  private lazy val externalPlugin = new UseExternalProcess(
    loggerFactory,
    externalParticipants = Set(tinyProxyConfig.participantName),
    fileNameHint = this.getClass.getSimpleName,
  )

  def stop(): Unit = backgroundProcessHandler.tryKill(PROCESS_NAME)
  def start(): Unit = backgroundProcessHandler.tryStart(PROCESS_NAME)

  override def beforeTests(): Unit =
    externalPlugin.beforeTests()

  override def afterTests(): Unit =
    externalPlugin.afterTests()

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    setup(config)

  private def waitUntilRunning(participant: RemoteParticipantReference): Unit =
    eventually(60.seconds) {
      participant.health.status match {
        case NodeStatus.Success(_) => ()
        case err => fail(s"remote participant ${participant.name} is not starting up: $err")
      }
    }

  override def afterEnvironmentCreated(
      config: CantonConfig,
      environment: TestConsoleEnvironment,
  ): Unit = {
    externalPlugin.afterEnvironmentCreated(config, environment)
    externalPlugin.start(
      tinyProxyConfig.participantName,
      Map(
        (
          "JAVA_TOOL_OPTIONS",
          // JVM options that instruct the node to go through the webproxy
          s"-Dhttps.proxyHost=localhost " +
            s"-Dhttps.proxyPort=$tinyProxyPort " +
            // Set to empty string, otherwise by default localhost addresses are not proxied
            s"-Dhttp.nonProxyHosts=\"\"",
        )
      ),
    )
    waitUntilRunning(environment.rp(tinyProxyConfig.participantName))
  }

  private def setup(config: CantonConfig): CantonConfig = {
    val tinyProxyConfigFile = File
      .newTemporaryFile()
      .writeText(
        File(Resource.getUrl("tinyproxy.conf")).contentAsString
          .replace("PORT_PLACEHOLDER", tinyProxyPort.toString)
      )
      .pathAsString

    backgroundProcessHandler.tryAdd(
      PROCESS_NAME,
      Seq("tinyproxy", "-d", "-c", tinyProxyConfigFile),
      Map.empty,
      (),
      manualStart = false,
    )

    eventually() {
      backgroundProcessHandler.tryIsRunning(PROCESS_NAME) shouldBe true
    }

    externalPlugin.beforeEnvironmentCreated(config)
  }

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit = {
    externalPlugin.afterEnvironmentDestroyed(config)
    backgroundProcessHandler.killAndRemove()
  }

  override def beforeEnvironmentDestroyed(environment: TestConsoleEnvironment): Unit =
    externalPlugin.beforeEnvironmentDestroyed(environment)
}

object UseTinyProxy {
  val PROCESS_NAME = "tinyproxy"
  // Only supports proxying of a single participant node at the moment
  // TODO(i29276) Extend when adding support for other nodes
  final case class TinyProxyConfig(participantName: String)
}
