// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.*
import com.digitalasset.canton.admin.api.client.data.ComponentHealthState
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.RemoteMediatorReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseExternalProcess,
  UsePostgres,
  UseReferenceBlockSequencer,
  UseSharedStorage,
}
import com.digitalasset.canton.integration.tests.*
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.client.SequencerClient

trait MediatorFailoverIntegrationTest
    extends ReliabilityTestSuite
    with ReplicatedMediatorTestSetup {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  protected def startAndGet(external: UseExternalProcess)(
      mediatorName: String
  )(implicit env: TestConsoleEnvironment): RemoteMediatorReference = {
    val mediator = env.rm(mediatorName)
    external.start(mediator.name)
    mediator
  }

  protected val externalPlugin =
    new UseExternalProcess(
      loggerFactory,
      externalMediators = Set(mediator1Name, mediator2Name),
      fileNameHint = this.getClass.getSimpleName,
    )

  protected def setupPluginsForMediator(
      storagePlugin: EnvironmentSetupPlugin,
      additionalPlugins: Seq[EnvironmentSetupPlugin] = Seq.empty,
  ): Unit = {
    // Order of the plugins is important here
    registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
    registerPlugin(storagePlugin)
    additionalPlugins.foreach(registerPlugin)
    registerPlugin(UseSharedStorage.forMediators(mediator1Name, Seq(mediator2Name), loggerFactory))
    registerPlugin(externalPlugin)
  }

  // Add a setup step before network bootstrap to start the external mediators
  override protected def preNetworkBootstrapSetup(env: TestConsoleEnvironment): Unit = {
    // Start first m1 and wait for it to be active. We need this because we use m1 in the network bootstrap so it
    // needs to be the active one
    val m1 = startAndGet(externalPlugin)(mediator1Name)(env)
    m1.health.wait_for_running()
    waitActive(m1, allowNonInit = true)
    val m2 = startAndGet(externalPlugin)(mediator2Name)(env)
    m2.health.wait_for_running()
  }

  // Change the network bootstraper to point to the external mediator
  override protected def networkBootstrapper(implicit
      env: TestConsoleEnvironment
  ): NetworkBootstrapper = {
    import env.*
    new NetworkBootstrapper(
      NetworkTopologyDescription(
        "synchronizer1",
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(rm(mediator1Name)),
      )
    )
  }

  override lazy val baseEnvironmentDefinition: EnvironmentDefinition =
    super.baseEnvironmentDefinition.withSetup { env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, daName)
      participant2.synchronizers.connect_local(sequencer1, daName)
    }
}

trait ReplicatedMediatorFailoverIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with MediatorFailoverIntegrationTest {

  private def setupReplicas(
      mediator1: RemoteMediatorReference,
      mediator2: RemoteMediatorReference,
  ): (RemoteMediatorReference, RemoteMediatorReference) = {
    // Active check only works after initialized
    val (activeMediator, passiveMediator) =
      if (isActive(mediator1)) (mediator1, mediator2)
      else if (isActive(mediator2)) (mediator2, mediator1)
      else fail("no mediator is active")

    // ensure active mediator is active
    waitActive(activeMediator)

    (activeMediator, passiveMediator)
  }

  "A replicated mediator" must {

    "fail-over when the active mediator replica crashes" taggedAs
      ReliabilityTest(
        Component("Participant", "connected to replicated mediator"),
        AdverseScenario(
          dependency = "mediator",
          details = "active mediator process is forcefully stopped",
        ),
        Remediation(
          remediator = "passive mediators",
          action =
            "passive mediator recognizes that the active mediator is offline, becomes active, and connects to the " +
              "sequencer to receive future requests",
        ),
        outcome = "transaction processing continuously possible between fail-overs",
      ) in { implicit env =>
        import env.*

        val m1 = rm(mediator1Name)
        val m2 = rm(mediator2Name)

        val (activeMediator, passiveMediator) = setupReplicas(m1, m2)

        // Make sure we can ping with the current active mediator
        participant1.health.ping(participant2)

        logger.debug(s"Crashing active mediator: ${activeMediator.name}")
        externalPlugin.kill(activeMediator.name)
        waitActive(passiveMediator)
        externalPlugin.start(activeMediator.name)

        // Ensure previous active mediator is now passive
        waitPassive(activeMediator)

        // A ping must eventually succeed
        eventually() {
          val newActiveComponents =
            passiveMediator.health.status.trySuccess.components.map(c => c.name -> c.state)
          newActiveComponents.contains(
            DbStorage.healthName -> ComponentHealthState.Ok()
          ) shouldBe true
          newActiveComponents.contains(
            SequencerClient.healthName -> ComponentHealthState.Ok()
          ) shouldBe true
          participant1.health.ping(participant2)
        }
      }
  }

}

class ReplicatedMediatorFailoverIntegrationTestPostgres
    extends ReplicatedMediatorFailoverIntegrationTest {
  setupPluginsForMediator(new UsePostgres(loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition
}
