// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import cats.syntax.either.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.console.{CommandFailure, RemoteParticipantReference}
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.config.{ParticipantNodeConfig, RemoteParticipantConfig}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*

trait LoadBalancedHAParticipantIntegrationTestBase { self: CommunityIntegrationTest =>
  protected val loadBalancedParticipantName = "loadBalancedParticipant"
  val healthCheckInterval = 3

  def loadBalancedParticipant(implicit
      env: TestConsoleEnvironment
  ): RemoteParticipantReference = {
    import env.*

    rp(loadBalancedParticipantName)
  }

  protected val addRemoteLoadBalancedParticipant: ConfigTransform =
    _.focus(_.remoteParticipants).modify {
      _ + (InstanceName.tryCreate(loadBalancedParticipantName) -> RemoteParticipantConfig(
        // the address and ports below will be later replaced with the load balancer frontend
        // once that has been started
        adminApi = FullClientConfig("localhost", Port.tryCreate(0)),
        ledgerApi = FullClientConfig("localhost", Port.tryCreate(0)),
      ))
    }

  protected def mkLoadBalancerSetup(
      external: UseExternalProcess,
      loadBalancedPar1Name: String,
      loadBalancedPar2Name: String,
      apiName: String,
  )(setup: HAProxySetup): HAProxyConfig = {
    assert(Seq("admin-api", "ledger-api").contains(apiName))
    def remoteParticipantConfig(name: String): ParticipantNodeConfig =
      external
        .configs(name)
        .participants
        .headOption
        .getOrElse(fail(s"missing participant: $name"))
        ._2

    def ports: Seq[Port] =
      Seq(loadBalancedPar1Name, loadBalancedPar2Name)
        .map(remoteParticipantConfig)
        .flatMap { case participantConfig =>
          Seq(
            participantConfig.ledgerApi.port,
            participantConfig.adminApi.port,
            participantConfig.monitoring.httpHealthServer
              .valueOrFail("http server not defined")
              .port,
          )
        }

    def haproxyConfig: String = {
      def backendServers: Seq[String] = {
        val portSelector: ParticipantNodeConfig => Port =
          if (apiName == "admin-api") _.adminApi.port else _.ledgerApi.port

        Seq(loadBalancedPar1Name, loadBalancedPar2Name)
          .map(remoteParticipantConfig)
          .zipWithIndex
          .map {
            case (
                  participantConfig,
                  index,
                ) => // check every $healthCheckInterval seconds, need one healthy response to consider a server as healthy
              val port = participantConfig.monitoring.httpHealthServer
                .valueOrFail("http server is missing")
                .port
              s"server $apiName$index ${setup.hostAddress}:${portSelector(participantConfig).unwrap} proto h2 check port ${port.unwrap} inter ${healthCheckInterval}s rise 1"
          }
      }

      s"""
         |global
         |    log stdout format raw local0
         |
         |defaults
         |    log global
         |    mode http
         |    option httplog
         |    # enabled so long running connections are logged immediately upon connect
         |    option logasap
         |    # to wait when trying to connect to a backend server
         |    timeout connect 10s
         |    # waiting time for server to send back data
         |    timeout server 30s
         |    # waiting time for client to send data
         |    timeout client 30s
         |
         |frontend $apiName
         |    bind :${setup.haproxyPort} proto h2
         |    default_backend ${apiName}_backend
         |
         |backend ${apiName}_backend
         |    # enable http health checks
         |    option httpchk
         |    # required to create a separate connection to query the load balancer.
         |    # this is particularly important as the health http server does not support h2
         |    # which would otherwise be the default.
         |    http-check connect
         |    # set the health check uri
         |    http-check send meth GET uri /health
         |
         |${backendServers.map(server => s"    $server").mkString(System.lineSeparator())}
         |""".stripMargin
    }

    logger.info(s"HAProxy is serving $apiName on port ${setup.haproxyPort}")

    HAProxyConfig(ports, haproxyConfig)
  }

  def setRemoteParticipantToLoadBalancedAdminApi(host: String, port: Port): ConfigTransform =
    ConfigTransforms.updateRemoteParticipantConfig(loadBalancedParticipantName) {
      _.focus(_.adminApi).modify(
        _.copy(address = host, port = port)
      )
    }

  def setRemoteParticipantToLoadBalancedLedgerApi(host: String, port: Port): ConfigTransform =
    ConfigTransforms.updateRemoteParticipantConfig(loadBalancedParticipantName) {
      _.focus(_.ledgerApi).modify(
        _.copy(address = host, port = port)
      )
    }
}

class LoadBalancedHAParticipantIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with LoadBalancedHAParticipantIntegrationTestBase {

  private val (participant1Name, participant2Name) = ("participant1", "participant2")

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4S1M1_Manual
      .addConfigTransforms(ConfigTransforms.addMonitoringEndpointAllNodes*)
      .addConfigTransform(
        ConfigTransforms.enableReplicatedParticipants(participant1Name, participant2Name)
      )
      .addConfigTransform(addRemoteLoadBalancedParticipant)
      // Increase the timeout because the individual tests start even before the external processes are fully spined up
      // which can add enough delay to fail the test under the default timeout
      .addConfigTransform(
        _.focus(_.parameters.timeouts.console.unbounded)
          .replace(config.NonNegativeDuration.tryFromDuration(3.minutes))
          .focus(_.parameters.timeouts.processing.unbounded)
          .replace(config.NonNegativeDuration.tryFromDuration(3.minutes))
      )
      .withSetup { implicit env =>
        import env.*
        sequencer1.start()
        mediator1.start()
        external.start(remoteParticipant1.name)
        external.start(remoteParticipant2.name)

        bootstrap.synchronizer(
          daName.unwrap,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )
      }

  protected val external =
    new UseExternalProcess(
      loggerFactory,
      externalParticipants = Set(participant1Name, participant2Name),
      fileNameHint = this.getClass.getSimpleName,
    )
  registerPlugin(
    new UsePostgres(loggerFactory)
  ) // needs to be before the external process such that we pick up the postgres config changes
  registerPlugin(
    UseSharedStorage.forParticipants(participant1Name, Seq(participant2Name), loggerFactory)
  )
  registerPlugin(
    new UseConfigTransforms(
      Seq(ConfigTransforms.setParticipantMaxConnections(PositiveInt.tryCreate(16))),
      loggerFactory,
    )
  )
  registerPlugin(external)
  registerPlugin(
    new UseHAProxy(
      mkLoadBalancerSetup(external, participant1Name, participant2Name, "admin-api"),
      setRemoteParticipantToLoadBalancedAdminApi,
      loggerFactory,
    )
  )
  registerPlugin(new UseBftSequencer(loggerFactory))

  "load balanced participant should eventually appear running" in { implicit env =>
    // This basically means that one of the participant instances have become healthy and that the load balancer
    // has noticed that instance is healthy and is forwarding requests to it.
    loadBalancedParticipant.health.wait_for_running()
    loadBalancedParticipant.health.wait_for_initialized()

    // this should be the case for the load balancer to have forwarded requests to it,
    // but double check as this is a test
    loadBalancedParticipant.health.status.trySuccess.active shouldBe true
  }

  "can connect to a synchronizer and ping myself with the load balanced participant" in {
    implicit env =>
      import env.*
      loadBalancedParticipant.synchronizers.connect_local(sequencer1, alias = daName)

      loadBalancedParticipant.health.ping(loadBalancedParticipant)
  }

  "the other participant becomes active and is usable after killing the originally active participant" in {
    implicit env =>
      import env.*

      // work out which participant is actually the active one by just querying them directly
      val activeParticipantName =
        actualConfig.remoteParticipantsByString.keySet
          .filterNot(_ == loadBalancedParticipantName)
          .find(name =>
            rp(name).health.status.successOption.exists(_.active)
          ) // One of these should be active
          .getOrElse(fail("No remote participants were active"))

      // knock them out
      logger.debug(s"Killing $activeParticipantName")
      external.kill(activeParticipantName, force = true)

      // wait for the other participant to become active and switch to it so admin commands work again
      logger.debug("Waiting for participant to appear active")
      loadBalancedParticipant.health.wait_for_running()
      // Retrying as sometimes HAproxy thinks no server is available due to Layer4 connection problems
      // "Server admin-api/admin-api1 is DOWN, reason: Layer4 connection problem, info: "Connection refused", check duration: 0ms."
      // So far I have seen this error only on CI, not locally
      utils.retry_until_true(60.seconds) {
        logger.debug("Pinging")
        loggerFactory.assertLogsUnorderedOptional(
          Either
            .catchOnly[CommandFailure](loadBalancedParticipant.health.ping(loadBalancedParticipant))
            .isRight,
          (
            LogEntryOptionality.Optional,
            _.errorMessage should include(
              "Request failed for loadBalancedParticipant. Is the server running?"
            ),
          ),
        )

      }
  }
}
