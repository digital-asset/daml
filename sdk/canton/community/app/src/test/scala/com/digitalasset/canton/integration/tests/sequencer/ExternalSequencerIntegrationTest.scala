// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  ApiLoggingConfig,
  CantonConfig,
  CantonFeatures,
  DbConfig,
  LoggingConfig,
  MonitoringConfig,
  NonNegativeFiniteDuration as NonNegativeFiniteDurationConfig,
}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseExternalProcess.ShutdownPhase
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UseExternalProcess,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import monocle.macros.syntax.lens.*

abstract class ExternalSequencerIntegrationTest(override val name: String)
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext
    with BasicSequencerTest
    with SequencerRestartTest {

  private val sequencerNames = ("sequencer1", "sequencer2")
  private val sequencerNamesList = List(sequencerNames._1, sequencerNames._2)

  private val external =
    new UseExternalProcess(
      loggerFactory,
      externalSequencers = sequencerNamesList.toSet,
      fileNameHint = this.getClass.getSimpleName,
      shutdownPhase = ShutdownPhase.AfterEnvironment,
    )

  registerPlugin(
    new EnvironmentSetupPlugin {
      override def afterEnvironmentDestroyed(config: CantonConfig): Unit =
        // we want the sequencers to be killed before the UseReferenceBlockSequencer cleans up the database,
        // but only after participants and mediators have been stopped (after environment destroyed)
        sequencerNamesList.foreach(external.kill(_))

      override protected def loggerFactory: NamedLoggerFactory =
        sys.error(s"logging was used but shouldn't be")
    }
  )
  registerPlugin(sequencerPlugin)
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(external)
  registerPlugin(QuickSequencerReconnection(loggerFactory))

  protected def sequencerPlugin: EnvironmentSetupPlugin

  final override protected def restartSequencers()(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    sequencerNamesList.foreach(external.kill(_))
    sequencerNamesList.foreach(external.start)
    // wait for the sequencers to have fully restarted
    val sequencers = sequencerNamesList.map(rs)
    sequencers.foreach(_.health.wait_for_running())
  }

  override final lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition(
      CantonConfig(
        sequencers = sequencerNamesList.map { sequencerName =>
          InstanceName.tryCreate(sequencerName) -> SequencerNodeConfig()
        }.toMap,
        mediators = Map(InstanceName.tryCreate(s"mediator1") -> MediatorNodeConfig()),
        participants = (1 to 2).map { i =>
          InstanceName.tryCreate(s"participant$i") -> LocalParticipantConfig()
        }.toMap,
        monitoring = MonitoringConfig(
          tracing = TracingConfig(propagation = Propagation.Enabled),
          logging = LoggingConfig(api = ApiLoggingConfig(messagePayloads = true)),
        ),
        features = CantonFeatures(enableRepairCommands = true),
      )
    )
      .addConfigTransforms(ConfigTransforms.defaultsForNodes*)
      .withManualStart
      .withSetup { implicit env =>
        import env.*
        val sequencers = sequencerNamesList.map(rs)
        logger.info(s"$name external sequencers are starting")
        sequencerNamesList.foreach(external.start)
        sequencers.foreach(_.health.wait_for_running())
        logger.info(s"$name external sequencers are running")
      }
      .withNetworkBootstrap { implicit env =>
        import env.*
        val sequencers = sequencerNamesList.map(rs)
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            synchronizerAlias = daName,
            synchronizerOwners = sequencers,
            synchronizerThreshold = PositiveInt.tryCreate(sequencers.size),
            sequencers = sequencers,
            mediators = Seq(mediator1),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*

        val (sequencer1, sequencer2) = { import sequencerNames.*; (rs(_1), rs(_2)) }
        logger.info(s"synchronizer on $name external sequencers is starting")
        // wait for the synchronizer to have fully started
        sequencer1.health.wait_for_initialized()
        sequencer2.health.wait_for_initialized()
        logger.info(s"synchronizer on $name external sequencer has started")

        logger.info(s"$name external sequencer environment: starting participants")
        participant1.start()
        participant2.start()

        logger.info(s"$name external sequencer environment: connecting participants to sequencers")
        participant1.synchronizers.connect_local(
          sequencer1,
          alias = daName,
        )
        participant2.synchronizers.connect_local(
          sequencer2,
          alias = daName,
        )

        sequencer1.health.status.trySuccess.connectedParticipants should contain(participant1.id)
        sequencer2.health.status.trySuccess.connectedParticipants should contain(participant2.id)
        logger.info(s"$name external sequencer environment is ready")
      }
}

// this forces the sequencer clients to never take too long to reattempt to connect to the sequencer.
// this is important to avoid timeouts due to long retry intervals.
// doing this config change as a plugin so that it happens after the external sequencer plugin has completed its
// config changes. if this was done as part of the env definition, it would get lost.
private[tests] final case class QuickSequencerReconnection(
    protected val loggerFactory: NamedLoggerFactory
) extends EnvironmentSetupPlugin {
  override def beforeEnvironmentCreated(
      config: CantonConfig
  ): CantonConfig =
    ConfigTransforms.updateAllSequencerClientConfigs_(c =>
      // warnDisconnectDelay has to be smaller than maxConnectionRetryDelay
      // otherwise the bft sequencer connection retries forever, never fails, never fails-over
      c.focus(_.warnDisconnectDelay)
        .replace(NonNegativeFiniteDurationConfig.ofMillis(500))
        .focus(_.maxConnectionRetryDelay)
        .replace(NonNegativeFiniteDurationConfig.ofSeconds(1))
    )(config)
}

class ExternalReferenceSequencerIntegrationTest
    extends ExternalSequencerIntegrationTest("reference")
    with SequencerRestartTest {

  override protected lazy val sequencerPlugin
      : UseCommunityReferenceBlockSequencer[DbConfig.Postgres] =
    new UseCommunityReferenceBlockSequencer[Postgres](loggerFactory)
}

// The BFT Orderer currently does not fully support crash tolerance, therefore it cannot be
// reliably tested for that. So for now we only test BasicSequencerTest (ping and bong).
// When crash tolerance is supported the test below can be uncommented out and the following one removed.
// TODO(#16761): re-enable the test below (and remove the replacement) once crash recovery is implemented

//class ExternalBftOrderingSequencerIntegrationTest
//    extends ExternalSequencerIntegrationTest(BftOrderingBlockSequencerFactory.ShortName)
//    with SequencerRestartTest {
//
//  override protected lazy val sequencerPlugin
//      : UseBftOrderingBlockSequencer =
//    new UseBftOrderingBlockSequencer(loggerFactory)
//}

class ExternalBftOrderingSequencerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext
    with BasicSequencerTest {

  registerPlugin(new UseBftSequencer(loggerFactory))
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S2M1.withSetup { implicit env =>
      import env.*
      // wait for the synchronizer to have fully started
      sequencer1.health.wait_for_initialized()
      sequencer2.health.wait_for_initialized()
      logger.info(s"synchronizer on $name external sequencer has started")
      logger.info(s"$name external sequencer environment: connecting participants to sequencers")
      participant1.synchronizers.connect_local(sequencer1, daName)
      participant2.synchronizers.connect_local(sequencer2, daName)
      sequencer1.health.status.trySuccess.connectedParticipants should contain(participant1.id)
      sequencer2.health.status.trySuccess.connectedParticipants should contain(participant2.id)
      logger.info(s"$name external sequencer environment is ready")
    }
  override def name: String = BftSequencerFactory.ShortName
}
