// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.config.LocalNodeConfig
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.ConfigTransforms.ConfigNodeType
import com.digitalasset.canton.integration.plugins.toxiproxy.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseConfigTransforms,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.health.HealthReportingTestHelper
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import eu.rekawek.toxiproxy.model.ToxicDirection
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import sttp.client3.*

trait HealthServiceDatabaseFaultTest extends HealthReportingTestHelper {
  def proxyConf: ProxyConfig

  lazy val toxiProxyConf: UseToxiproxy.ToxiproxyConfig =
    UseToxiproxy.ToxiproxyConfig(List(proxyConf))
  lazy val toxiproxyPlugin = new UseToxiproxy(toxiProxyConf)

  protected val getToxiProxy: () => RunningProxy

  protected def setupPluginsWithToxiproxy(
      storagePlugin: EnvironmentSetupPlugin,
      otherPlugins: EnvironmentSetupPlugin*
  ): () => RunningProxy = {
    registerPlugin(storagePlugin)
    registerPlugin(toxiproxyPlugin)
    otherPlugins.foreach(registerPlugin)

    // Return a function that returns the running proxy during tests
    () => toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Manual
      .withSetup { implicit env =>
        import env.*
        sequencer1.start()
        mediator1.start()
        participant1.start()
        bootstrapSynchronizer(mediator1)
      }
      .addConfigTransforms(
        ConfigTransforms.addMonitoringEndpointAllNodes*
      )
      .withTeardown { _ =>
        ToxiproxyHelpers.removeAllProxies(
          toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
        )
      }

  private val backend = HttpURLConnectionBackend()

  private def checkHttpHealth(
      port: Port
  )(expectedCode: Int) = {
    val request = basicRequest.get(uri"http://localhost:$port/health")
    val response = request.send(backend)
    response.code.code shouldBe expectedCode
  }

  protected def nodeConfig(env: TestConsoleEnvironment): LocalNodeConfig

  "report not serving when storage goes down" in { implicit env =>
    val config = nodeConfig(env)
    withHealthStubs(
      Seq(
        config.monitoring.grpcHealthServer.value
      )
    ) { case Seq(nodeHealth) =>
      eventually() {
        checkServing(nodeHealth)
      }
      checkHttpHealth(config.monitoring.httpHealthServer.value.port)(200)

      loggerFactory.suppressWarnings {
        // Slow down and terminate the connection
        val toxic = clue("Stopping DB connection") {
          getToxiProxy().underlying
            .toxics()
            .timeout("db-con-timeout", ToxicDirection.UPSTREAM, 1000)
        }

        // Wait until participant shows NOT_SERVING
        clue("Waiting for not serving") {
          eventually() {
            checkNotServing(nodeHealth)
          }
          checkHttpHealth(config.monitoring.httpHealthServer.value.port)(503)
        }

        // Re-enable connection
        toxic.remove()
      }

      clue("Waiting for serving") {
        eventually() {
          checkServing(nodeHealth)
        }
        checkHttpHealth(config.monitoring.httpHealthServer.value.port)(200)
      }
    }
  }

}

trait HealthServiceSequencerDatabaseFaultTest extends HealthServiceDatabaseFaultTest {
  override def proxyConf: ProxyConfig =
    // Setting `dbTimeout` here sets the `connectionTimeout` parameter when building the Postgres config
    SequencerToPostgres(s"sequencer1-to-postgres", "sequencer1", dbTimeout = 2000L)

  // Lower `failedToFatalDelay` so it triggers quickly
  protected lazy val lowerFailedToFatalDelay: ConfigTransform =
    ConfigTransforms.modifyAllStorageConfigs {
      case (ConfigNodeType.Sequencer, _nodeName, config) =>
        config match {
          case dbConfig: Postgres =>
            dbConfig.modify(parameters =
              dbConfig.parameters
                .focus(_.failedToFatalDelay)
                .replace(canton.config.NonNegativeFiniteDuration.ofSeconds(3))
            )
          case other => other
        }
      case (_, _, config) => config
    }

  "report liveness not serving when storage goes down for longer than timeout" in { implicit env =>
    val config = nodeConfig(env)
    withHealthStubs(
      Seq(
        config.monitoring.grpcHealthServer.value
      )
    ) { case Seq(nodeHealth) =>
      eventually() {
        checkServing(nodeHealth)
      }

      loggerFactory.suppressWarnings {
        // Slow down and terminate the connection
        val toxic = clue("Stopping DB connection") {
          getToxiProxy().underlying
            .toxics()
            .timeout("db-con-timeout", ToxicDirection.UPSTREAM, 1000)
        }

        // Wait until sequencer liveness shows NOT_SERVING
        clue("Waiting for not serving") {
          eventually(timeUntilSuccess = 60.seconds) {
            checkLivenessNotServing(nodeHealth)
          }
        }

        // Re-enable connection
        toxic.remove()
      }
    }
  }

}

class HealthServiceParticipantFaultBftOrderingIntegrationTestPostgres
    extends HealthServiceDatabaseFaultTest {

  override def proxyConf: ProxyConfig =
    ParticipantToPostgres(s"participant1-to-postgres", "participant1")

  override protected def nodeConfig(
      env: TestConsoleEnvironment
  ): ParticipantNodeConfig =
    env.actualConfig.participantsByString("participant1")

  override protected val getToxiProxy: () => RunningProxy =
    setupPluginsWithToxiproxy(
      new UsePostgres(loggerFactory),
      new UseBftSequencer(loggerFactory),
    )
}

class HealthServiceSequencerFaultBftOrderingIntegrationTestPostgres
    extends HealthServiceSequencerDatabaseFaultTest {

  override protected def nodeConfig(env: TestConsoleEnvironment): LocalNodeConfig =
    env.actualConfig.sequencersByString("sequencer1")

  override protected val getToxiProxy: () => RunningProxy =
    setupPluginsWithToxiproxy(
      new UsePostgres(loggerFactory),
      // Run after the DB plugin to alter the DB configuration
      new UseConfigTransforms(Seq(lowerFailedToFatalDelay), loggerFactory),
      new UseBftSequencer(loggerFactory),
    )
}

class HealthServiceMediatorFaultBftOrderingIntegrationTestPostgres
    extends HealthServiceDatabaseFaultTest {

  override def proxyConf: ProxyConfig =
    MediatorToPostgres(s"mediator1-to-postgres", "mediator1")

  override protected def nodeConfig(env: TestConsoleEnvironment): LocalNodeConfig =
    env.actualConfig.mediatorsByString("mediator1")

  override protected val getToxiProxy: () => RunningProxy =
    setupPluginsWithToxiproxy(
      new UsePostgres(loggerFactory),
      new UseBftSequencer(loggerFactory),
    )
}
