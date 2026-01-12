// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.option.*
import com.digitalasset.canton.config.AdminServerConfig
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres, UseSharedStorage}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.protocol.LocalRejectError.TimeRejects.LocalTimeout
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.{UniquePortGenerator, config}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

trait ReplicatedMediatorTestSetup extends ReplicatedNodeHelper {
  self: CommunityIntegrationTest & EnvironmentSetup =>
  protected lazy val mediator1Name = "mediatorReplicated1"
  protected lazy val mediator2Name = "mediatorReplicated2"

  protected def setupPluginsReplicatedMediator(
      storagePlugin: EnvironmentSetupPlugin
  ): Unit = {
    registerPlugin(new UseBftSequencer(loggerFactory))
    registerPlugin(storagePlugin)
    registerPlugin(UseSharedStorage.forMediators(mediator1Name, Seq(mediator2Name), loggerFactory))
  }

  private def addTwoReplicatedMediators: ConfigTransform = {
    def mkReplicatedMediatorNodeConfig = MediatorNodeConfig(
      adminApi = AdminServerConfig(internalPort = UniquePortGenerator.next.some)
    )

    _.focus(_.mediators)
      .replace(
        Map(
          InstanceName.tryCreate(mediator1Name) -> mkReplicatedMediatorNodeConfig,
          InstanceName.tryCreate(mediator2Name) -> mkReplicatedMediatorNodeConfig,
        )
      )
      .focus(_.parameters.nonStandardConfig)
      .replace(testedProtocolVersion.isAlpha)
  }

  protected def preNetworkBootstrapSetup(env: TestConsoleEnvironment): Unit = {
    import env.*
    // Start m1 first to make sure it's always the active one when bootstrapping the synchronizer
    m(mediator1Name).start()
    m(mediator1Name).health.wait_for_running()
    waitActive(m(mediator1Name), allowNonInit = true)
  }

  protected def networkBootstrapper(implicit
      env: TestConsoleEnvironment
  ): NetworkBootstrapper = {
    import env.*
    new NetworkBootstrapper(
      NetworkTopologyDescription(
        synchronizerAlias = daName,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(m(mediator1Name)),
      )
    )
  }

  protected def baseEnvironmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 1,
        // 0 mediators because we add them manually below with a custom config
        numMediators = 0,
      )
      .withManualStart
      .addConfigTransforms(
        addTwoReplicatedMediators,
        ConfigTransforms.enableReplicatedMediators(),
        ConfigTransforms.setPassiveCheckPeriodMediators(config.PositiveFiniteDuration.ofSeconds(3)),
        // The default of 20 seconds is too low after switching to the AZ runners
        ConfigTransforms.setDelayLoggingThreshold(config.NonNegativeFiniteDuration.ofSeconds(30)),
      )
      .withSetup(preNetworkBootstrapSetup)
      .withNetworkBootstrap(networkBootstrapper(_))
      .withSetup { env =>
        import env.*
        mediators.local.foreach(_.start())
        participants.local.foreach(_.start())
      }
      .addConfigTransforms(ConfigTransforms.dontWarnOnDeprecatedPV*)
}

trait ReplicatedMediatorIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ReplicatedMediatorTestSetup {

  "wait for everything to be started" in { implicit env =>
    import env.*

    Seq[InstanceReference](sequencer1, m(mediator1Name), m(mediator2Name)).foreach(
      _.health.wait_for_running()
    )
  }

  "passive mediators are automatically initialized" in { implicit env =>
    import env.*

    Seq(m(mediator1Name), m(mediator2Name)).foreach { m =>
      logger.debug(s"Waiting for mediator $m to be initialized")
      m.health.wait_for_initialized()
    }
  }

  "connect participant to synchronizer" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)
  }

  "ping itself successfully" in { implicit env =>
    import env.*

    participant1.health.ping(participant1)
  }

  "take down active mediator but still eventually be able to ping as passive mediator will take over" in {
    implicit env =>
      import env.*

      val activeMediator = waitUntilOneActive(m(mediator1Name), m(mediator2Name))

      logger.debug(s"Stopping active mediator $activeMediator")
      activeMediator.stop()

      def expectedMsg(entries: Seq[LogEntry]): Assertion = {
        val valid =
          Seq(
            LocalTimeout.id,
            "as exceeded the max-sequencing-time",
            "Sequencing result message timed out",
          )
        forAll(entries) { entry =>
          assert(valid.exists(entry.message.contains), entry)
        }
      }

      logger.debug("Starting second ping")
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        eventually() {
          participant1.health.ping(participant1)
        },
        expectedMsg,
      )

      logger.debug(s"Bring back former active mediator $activeMediator")
      activeMediator.start()
  }

}

class ReplicatedMediatorIntegrationTestPostgres extends ReplicatedMediatorIntegrationTest {
  setupPluginsReplicatedMediator(new UsePostgres(loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition
}
