// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.console.{ParticipantReference, RemoteParticipantReference}
import com.digitalasset.canton.integration.plugins.UseExternalProcess
import com.digitalasset.canton.integration.tests.ReplicatedParticipantTestSetup
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  IsolatedEnvironments,
}
import sttp.client3.*

class ParticipantFailoverIntegrationTestBase
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HasExecutionContext
    with ReplicatedParticipantTestSetup {

  protected lazy val externalPlugin =
    new UseExternalProcess(
      loggerFactory,
      externalParticipants = Set(activeParticipantName, passiveParticipantName),
      fileNameHint = this.getClass.getSimpleName,
    )

  protected lazy val extraPluginsToEnable: Seq[EnvironmentSetupPlugin] = Seq()

  override protected def setupPlugins(
      storagePlugin: EnvironmentSetupPlugin,
      additionalPlugins: Seq[EnvironmentSetupPlugin],
  ): Unit = {
    super.setupPlugins(storagePlugin, additionalPlugins ++ extraPluginsToEnable)
    registerPlugin(externalPlugin)
  }

  protected def restartActiveReplica(
      participant1: RemoteParticipantReference,
      participant2: RemoteParticipantReference,
      participant3: ParticipantReference,
  ): Unit = {

    // Check if p1 is active or p2
    val (activeParticipant, passiveParticipant) = getActiveAndPassive(participant1, participant2)

    // Make sure the active can ping
    activePing(activeParticipant, participant3)

    // Crash active replica, wait for passive to take over, then restart former active replica
    logger.debug(s"Crashing active replica $activeParticipant")
    externalPlugin.kill(activeParticipant.name)
    waitActive(passiveParticipant)
    externalPlugin.start(activeParticipant.name)
    waitPassive(activeParticipant)

    activePing(passiveParticipant, participant3)
  }

  protected def isHealthy(participantName: String): Boolean = {
    val conf = externalPlugin.configs.get(participantName).value
    conf.participants
      .get(InstanceName.tryCreate(participantName))
      .flatMap(_.monitoring.httpHealthServer) match {
      case Some(value) =>
        val request = basicRequest.get(
          uri"http://${value.address}:${value.port}/health"
        )
        val backend = HttpURLConnectionBackend()
        val response = request.send(backend)
        response.is200
      case None => fail("can not access participant config")
    }
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition
      .addConfigTransforms(ConfigTransforms.addMonitoringEndpointAllNodes*)
      .withSetup { implicit env =>
        import env.*

        setupReplicas(env, Some(externalPlugin))

        // start a non-replicated participant and connect to the synchronizer
        participant3.synchronizers.connect_local(sequencer1, alias = daName)

        // We don't need participant4
        participant4.stop()
      }
}
