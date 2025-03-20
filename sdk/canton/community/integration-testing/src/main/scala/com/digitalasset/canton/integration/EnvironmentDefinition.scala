// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import better.files.{File, Resource}
import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  ApiLoggingConfig,
  CantonConfig,
  CantonFeatures,
  CryptoConfig,
  EnterpriseCantonEdition,
  LoggingConfig,
  MonitoringConfig,
  TestingConfigInternal,
}
import com.digitalasset.canton.console.{ConsoleEnvironment, InstanceReference, TestConsoleOutput}
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import com.digitalasset.canton.{BaseTest, SynchronizerAlias}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import monocle.macros.syntax.lens.*

/** Definition of how a environment should be configured and setup.
  * @param baseConfig
  *   the base config to use (typically loaded from a pre-canned config file or sample)
  * @param testingConfig
  *   the testing specifics bits of the config
  * @param setups
  *   a function to configure the environment before tests can be run.
  * @param teardown
  *   a function to perform cleanup after the environment has been destroyed.
  * @param configTransforms
  *   transforms to perform on the base configuration before starting the environment (typically
  *   making ports unique or some other specialization for the particular tests you're running)
  */
final case class EnvironmentDefinition(
    baseConfig: CantonConfig,
    testingConfig: TestingConfigInternal =
      TestingConfigInternal(warnOnAcsCommitmentDegradation = false),
    setups: List[TestConsoleEnvironment => Unit] = Nil,
    teardown: Unit => Unit = _ => (),
    configTransforms: Seq[ConfigTransform] = ConfigTransforms.defaults,
    staticSynchronizerParametersMap: Map[String, StaticSynchronizerParameters] = Map.empty,
) {

  /** Create a canton configuration by applying the configTransforms to the base config. Some
    * transforms may have side-effects (such as incrementing the next available port number) so only
    * do before constructing an environment.
    */
  def generateConfig: CantonConfig =
    configTransforms.foldLeft(baseConfig)((config, transform) => transform(config))

  def withManualStart: EnvironmentDefinition =
    copy(baseConfig = baseConfig.focus(_.parameters.manualStart).replace(true))

  def withSetup(setup: TestConsoleEnvironment => Unit): EnvironmentDefinition =
    copy(setups = setups :+ setup)

  def withTeardown(teardown: Unit => Unit): EnvironmentDefinition =
    copy(teardown = teardown)

  def withNetworkBootstrap(
      networkBootstrapFactory: TestConsoleEnvironment => NetworkBootstrapper
  ): EnvironmentDefinition =
    withSetup(env => networkBootstrapFactory(env).bootstrap())

  def clearConfigTransforms(): EnvironmentDefinition =
    copy(
      configTransforms = Seq()
    )

  def addConfigTransforms(transforms: ConfigTransform*): EnvironmentDefinition =
    transforms.foldLeft(this)((ed, ct) => ed.addConfigTransform(ct))

  def addConfigTransform(transform: ConfigTransform): EnvironmentDefinition =
    copy(configTransforms = this.configTransforms :+ transform)

  def updateTestingConfig(
      update: TestingConfigInternal => TestingConfigInternal
  ): EnvironmentDefinition =
    copy(testingConfig = testingConfig.focus().modify(update))

  private def setStaticSynchronizerParameters(
      map: Map[String, StaticSynchronizerParameters]
  ): EnvironmentDefinition =
    copy(staticSynchronizerParametersMap = map)

  def createTestConsole(
      environment: Environment,
      loggerFactory: NamedLoggerFactory,
  ): TestConsoleEnvironment =
    new ConsoleEnvironment(
      environment,
      new TestConsoleOutput(loggerFactory),
    ) with TestEnvironment
}

/** Default testing environments for integration tests
  *
  * Our typical configurations are provided in [[EnvironmentDefinition..singleSynchronizer]] and
  * [[EnvironmentDefinition..multiSynchronizer]], these load the configuration files
  * `single-synchronizer-topology.conf` and `multi-synchronizer-topology.conf` respectively.
  * Environment setup steps which are appropriate for all test cases in the class should be
  * performed using [[EnvironmentDefinition..withSetup]]. This allows test cases sharing an
  * environment to still be run individually.
  *
  * Config transforms are applied to the [[config.CantonConfig]] loaded from the base configuration
  * files. By default the tests will automatically assign unique port numbers and h2 database names
  * to ensure that concurrent environments will not interfere with one another (see
  * [[ConfigTransforms.defaults]]). These default transforms can be disabled for a test case by
  * using [[EnvironmentDefinition..clearConfigTransforms]], this is useful if using a configuration
  * known to not collide with other environments (see
  * [[com.digitalasset.canton.integration.tests.ExampleIntegrationTest]]). Additional transforms are
  * available to enable other common configuration on top of the default configuration files:
  *   - [[ConfigTransforms.allInMemory]]: for ensuring all nodes are using in-memory stores
  *   - [[ConfigTransforms.realCrypto]]: for enabling real crypto providers rather than symbolic
  *     These can be added to your test case by using [[EnvironmentDefinition..addConfigTransform]].
  *     A config transform is a function with the signature {{{CantonConfig => CantonConfig}}}
  *     enabling you to define your own if required. See
  *     [[com.digitalasset.canton.integration.tests.CommandResubmissionIntegrationTest]] for an
  *     example of custom config transforms using helpers from [[ConfigTransforms]] to just update
  *     config of specific synchronizers and participants.
  */
object EnvironmentDefinition extends LazyLogging {

  lazy val defaultStaticSynchronizerParameters: StaticSynchronizerParameters =
    StaticSynchronizerParameters.fromConfig(
      SynchronizerParametersConfig(),
      CryptoConfig(),
      BaseTest.testedProtocolVersion,
    )

  def buildBaseEnvironmentDefinition(
      numParticipants: Int,
      numSequencers: Int,
      numMediators: Int,
      withRemote: Boolean = false,
  ): EnvironmentDefinition = {

    val sequencers = (1 to numSequencers).map { i =>
      InstanceName.tryCreate(s"sequencer$i") -> SequencerNodeConfig()
    }.toMap

    val mediators = (1 to numMediators).map { i =>
      InstanceName.tryCreate(s"mediator$i") -> MediatorNodeConfig()
    }.toMap
    val participants = (1 to numParticipants).map { i =>
      InstanceName.tryCreate(s"participant$i") -> LocalParticipantConfig()
    }.toMap

    val configWithDefaults = CantonConfig(
      sequencers = sequencers,
      mediators = mediators,
      participants = participants,
      monitoring = MonitoringConfig(
        tracing = TracingConfig(propagation = Propagation.Enabled),
        logging = LoggingConfig(api = ApiLoggingConfig(messagePayloads = true)),
      ),
      features = CantonFeatures(enableRepairCommands = true),
    )

    def toRemote[L, R](map: Map[InstanceName, L])(asRemote: L => R): Map[InstanceName, R] =
      if (withRemote) map.map { case (k, v) =>
        (
          InstanceName.tryCreate("remote" + k.unwrap.take(1).toUpperCase + k.unwrap.drop(1)),
          asRemote(v),
        )
      }
      else Map.empty

    EnvironmentDefinition(
      configWithDefaults
        .validate(EnterpriseCantonEdition)
        .fold(
          err =>
            throw new IllegalArgumentException(
              s"Error while validating the config: ${err.mkString(",")}"
            ),
          _ => configWithDefaults,
        )
    ).addConfigTransforms(ConfigTransforms.defaultsForNodes*)
      .addConfigTransform(c =>
        c.focus(_.remoteParticipants)
          .replace(toRemote(c.participants)(_.toRemoteConfig))
          .focus(_.remoteMediators)
          .replace(toRemote(c.mediators)(_.toRemoteConfig))
          .focus(_.remoteSequencers)
          .replace(toRemote(c.sequencers)(_.toRemoteConfig))
      )
  }

  def allNodeNames(config: CantonConfig): Set[String] =
    (config.participants.keySet ++
      config.sequencers.keySet ++
      config.mediators.keySet).map(_.unwrap)

  lazy val simpleTopology: EnvironmentDefinition =
    fromResource("examples/01-simple-topology/simple-topology.conf")

  def S1M1(implicit env: TestConsoleEnvironment): NetworkTopologyDescription = {
    import env.*

    NetworkTopologyDescription(
      daName,
      synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
      synchronizerThreshold = PositiveInt.one,
      sequencers = Seq(sequencer1),
      mediators = Seq(mediator1),
    )
  }

  private def S2M1(implicit env: TestConsoleEnvironment): NetworkTopologyDescription = {
    import env.*

    NetworkTopologyDescription(
      daName,
      synchronizerOwners = Seq(sequencer1, sequencer2),
      synchronizerThreshold = PositiveInt.one,
      sequencers = Seq(sequencer1, sequencer2),
      mediators = Seq(mediator1),
    )
  }

  def S2M2(implicit env: TestConsoleEnvironment): NetworkTopologyDescription = {
    import env.*

    NetworkTopologyDescription(
      daName,
      synchronizerOwners = Seq(sequencer1, sequencer2),
      synchronizerThreshold = PositiveInt.one,
      sequencers = Seq(sequencer1, sequencer2),
      mediators = Seq(mediator1, mediator2),
    )
  }

  private def S2M1_S2M1(implicit
      env: TestConsoleEnvironment
  ): Seq[NetworkTopologyDescription] = {
    import env.*

    Seq(
      NetworkTopologyDescription(
        daName,
        synchronizerOwners = Seq(sequencer1, sequencer2),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1, sequencer2),
        mediators = Seq(mediator1),
      ),
      NetworkTopologyDescription(
        acmeName,
        synchronizerOwners = Seq(sequencer3, sequencer4),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer3, sequencer4),
        mediators = Seq(mediator2),
      ),
    )
  }

  def S1M1_S1M1(implicit env: TestConsoleEnvironment): Seq[NetworkTopologyDescription] = {
    import env.*

    Seq(
      NetworkTopologyDescription(
        daName,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      ),
      NetworkTopologyDescription(
        acmeName,
        synchronizerOwners = Seq(sequencer2),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer2),
        mediators = Seq(mediator2),
      ),
    )
  }

  private def S1M1_S1M1_S1M1(implicit
      env: TestConsoleEnvironment
  ): Seq[NetworkTopologyDescription] = {
    import env.*

    Seq(
      NetworkTopologyDescription(
        daName,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      ),
      NetworkTopologyDescription(
        acmeName,
        synchronizerOwners = Seq(sequencer2),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer2),
        mediators = Seq(mediator2),
      ),
      NetworkTopologyDescription(
        repairSynchronizerName,
        synchronizerOwners = Seq(sequencer3),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer3),
        mediators = Seq(mediator3),
      ),
    )
  }

  private def S1M1_S1M1_S1M1_S1M1(implicit
      env: TestConsoleEnvironment
  ): Seq[NetworkTopologyDescription] = {
    import env.*

    Seq(
      NetworkTopologyDescription(
        daName,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      ),
      NetworkTopologyDescription(
        acmeName,
        synchronizerOwners = Seq(sequencer2),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer2),
        mediators = Seq(mediator2),
      ),
      NetworkTopologyDescription(
        repairSynchronizerName,
        synchronizerOwners = Seq(sequencer3),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer3),
        mediators = Seq(mediator3),
      ),
      NetworkTopologyDescription(
        devSynchronizerName,
        synchronizerOwners = Seq(sequencer4),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer4),
        mediators = Seq(mediator4),
      ),
    )
  }

  /**   - 0 participants
    *   - 1 sequencer
    *   - 1 mediator
    */
  lazy val P0S1M1_Manual: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 0,
      numSequencers = 1,
      numMediators = 1,
    ).withManualStart

  /**   - 0 participants
    *   - 1 synchronizer with 1 sequencer and 1 mediator
    */
  lazy val P0_S1M1: EnvironmentDefinition = buildBaseEnvironmentDefinition(
    numParticipants = 0,
    numSequencers = 1,
    numMediators = 1,
  )
    .withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S1M1)
    }

  /**   - 0 participants
    *   - 1 synchronizer with 2 sequencers and 1 mediators
    */
  lazy val P0_S2M1: EnvironmentDefinition = buildBaseEnvironmentDefinition(
    numParticipants = 0,
    numSequencers = 2,
    numMediators = 1,
  )
    .withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S2M1)
    }

  private lazy val P1S1M1_Config = buildBaseEnvironmentDefinition(
    numParticipants = 1,
    numSequencers = 1,
    numMediators = 1,
  )

  private lazy val P1S4M4_Config = buildBaseEnvironmentDefinition(
    numParticipants = 1,
    numSequencers = 4,
    numMediators = 4,
  )

  /**   - 1 participant '''not''' connected to any synchronizer
    *   - 1 synchronizer with 1 sequencer and 1 mediator
    */
  lazy val P1_S1M1: EnvironmentDefinition =
    P1S1M1_Config.withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S1M1)
    }

  /**   - 1 participant '''not''' connected to any synchronizer
    *   - 4 synchronizers with 1 sequencer and 1 mediator
    */
  lazy val P1_S1M1_S1M1_S1M1_S1M1: EnvironmentDefinition =
    P1S4M4_Config.withNetworkBootstrap { implicit env =>
      NetworkBootstrapper(S1M1_S1M1_S1M1_S1M1)
    }

  /**   - 1 participant '''not''' connected to any synchronizer
    *   - 1 sequencer and 1 mediator
    *   - no initialized synchronizer
    */
  lazy val P1S1M1_Manual: EnvironmentDefinition =
    P1S1M1_Config.withManualStart

  /**   - 1 participant '''not''' connected to any synchronizer
    *   - 1 synchronizer with 2 sequencers and 2 mediators
    */
  lazy val P1_S2M2: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 1,
      numSequencers = 2,
      numMediators = 2,
    ).withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S2M2)
    }

  lazy val P2S3M3_Config: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 2,
      numSequencers = 3,
      numMediators = 3,
    )

  /**   - 2 participants
    *   - 3 sequencers
    *   - 3 mediators
    *   - no initialized synchronizer
    */
  lazy val P2S3M3_Manual: EnvironmentDefinition =
    P2S3M3_Config.withManualStart

  lazy val P2S1M1_Config: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 2,
      numSequencers = 1,
      numMediators = 1,
    )

  /**   - 2 participants '''not''' connected to the synchronizer
    *   - 1 synchronizer with 1 sequencer and 1 mediator
    */
  lazy val P2_S1M1: EnvironmentDefinition =
    P2S1M1_Config.withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S1M1)
    }

  /**   - 2 participants
    *   - 1 sequencer
    *   - 1 mediator
    *   - no initialized synchronizer
    */
  lazy val P2S1M1_Manual: EnvironmentDefinition =
    P2S1M1_Config.withManualStart

  /**   - 2 participants
    *   - 1 sequencer
    *   - 2 mediators
    *   - no initialized synchronizer
    */
  lazy val P2S1M2_Config: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 2,
      numSequencers = 1,
      numMediators = 2,
    )

  lazy val P2S2M1_Config: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 2,
      numSequencers = 2,
      numMediators = 1,
    )

  lazy val P2S2M2_Config: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 2,
      numSequencers = 2,
      numMediators = 2,
    )

  lazy val P4S2M2_Config: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 4,
      numSequencers = 2,
      numMediators = 2,
    )

  /**   - 2 participants
    *   - 2 sequencers
    *   - 1 mediators
    *   - no initialized synchronizer
    */
  lazy val P2S2M1_Manual: EnvironmentDefinition =
    P2S2M1_Config.withManualStart

  /**   - 2 participants '''not''' connected to the synchronizer
    *   - 1 synchronizer with 2 sequencers and 2 mediators
    */
  lazy val P2_S2M2: EnvironmentDefinition =
    P2S2M2_Config.withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S2M2)
    }

  /**   - 2 participants '''not''' connected to the synchronizer
    *   - 2 sequencers and 2 mediators
    *   - no initialized synchronizer
    */
  lazy val P2S2M2_Manual: EnvironmentDefinition =
    P2S2M2_Config.withManualStart

  /**   - 2 participants '''not''' connected to any synchronizer
    *   - 2 synchronizers with 1 sequencer and 1 mediator each
    */
  lazy val P2_S1M1_S1M1: EnvironmentDefinition =
    P2S2M2_Config.withNetworkBootstrap { implicit env =>
      NetworkBootstrapper(S1M1_S1M1)
    }

  /**   - 2 participants '''not''' connected to any synchronizer
    *   - 3 synchronizers with 1 sequencer and 1 mediator each
    */
  lazy val P2_S1M1_S1M1_S1M1: EnvironmentDefinition =
    P2S3M3_Config.withNetworkBootstrap { implicit env =>
      NetworkBootstrapper(S1M1_S1M1_S1M1)
    }

  /**   - 4 participants '''not''' connected to any synchronizer
    *   - 3 synchronizers with 1 sequencer and 1 mediator each
    */
  def P4_S1M1_S1M1_S1M1(
      staticSynchronizerParametersMap: Map[String, StaticSynchronizerParameters] = Map.empty
  ): EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 4,
      numSequencers = 3,
      numMediators = 3,
    ).setStaticSynchronizerParameters(staticSynchronizerParametersMap).withNetworkBootstrap {
      implicit env =>
        import env.*

        def paramsOrDefaults(alias: SynchronizerAlias): StaticSynchronizerParameters =
          staticSynchronizerParametersMap
            .getOrElse(alias.unwrap, defaultStaticSynchronizerParameters)

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
            staticSynchronizerParameters = paramsOrDefaults(daName),
          ),
          NetworkTopologyDescription(
            acmeName,
            synchronizerOwners = Seq(sequencer2),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer2),
            mediators = Seq(mediator2),
            staticSynchronizerParameters = paramsOrDefaults(acmeName),
          ),
          NetworkTopologyDescription(
            repairSynchronizerName,
            synchronizerOwners = Seq(sequencer3),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer3),
            mediators = Seq(mediator3),
            staticSynchronizerParameters = paramsOrDefaults(repairSynchronizerName),
          ),
        )
    }

  lazy val P3_S1M1_Config: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 3,
      numSequencers = 1,
      numMediators = 1,
    )

  /**   - 3 participants '''not''' connected to any synchronizer
    *   - 1 synchronizer with 1 sequencer and 1 mediator
    */
  lazy val P3_S1M1: EnvironmentDefinition =
    P3_S1M1_Config.withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S1M1)
    }

  /**   - 3 participants '''not''' connected to any synchronizer
    *   - 1 synchronizer with 1 sequencer and 1 mediator
    *   - no initialized synchronizer
    */
  lazy val P3_S1M1_Manual: EnvironmentDefinition =
    P3_S1M1_Config.withManualStart

  private lazy val P3S2M2_Config: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 3,
      numSequencers = 2,
      numMediators = 2,
    )

  /**   - 3 participants
    *   - 2 sequencers
    *   - 2 mediators
    *   - no initialized synchronizer
    */
  lazy val P3S2M2_Manual: EnvironmentDefinition =
    P3S2M2_Config.withManualStart

  /**   - 3 participants '''not''' connected to any synchronizer
    *   - 1 synchronizer with 2 sequencers and 2 mediators
    */
  lazy val P3_S2M2: EnvironmentDefinition = P3S2M2_Config
    .withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S2M2)
    }

  /**   - 3 participants '''not''' connected to any synchronizer
    *   - 2 synchronizers with 1 sequencer and 1 mediator each
    */
  lazy val P3_S1M1_S1M1_Manual: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 3,
      numSequencers = 2,
      numMediators = 2,
    ).withManualStart

  /**   - 4 participants '''not''' connected to any synchronizer
    *   - 1 sequencer
    *   - 1 mediator
    *   - no initialized synchronizer
    */
  lazy val P4S1M1_Manual: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 4,
      numSequencers = 1,
      numMediators = 1,
    ).withManualStart

  /**   - 4 participants '''not''' connected to any synchronizer
    *   - 1 synchronizer with 1 sequencer and 1 mediator
    */
  lazy val P4_S1M1: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 4,
      numSequencers = 1,
      numMediators = 1,
    ).withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S1M1)
    }

  lazy val P4_S1M1_S1M1: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 4,
      numSequencers = 2,
      numMediators = 2,
    ).withNetworkBootstrap { implicit env =>
      NetworkBootstrapper(S1M1_S1M1)
    }

  /**   - 4 participants '''not''' connected any domain
    *   - 1 domain with 2 sequencers and 2 mediators
    */
  lazy val P4_S2M2: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 4,
      numSequencers = 2,
      numMediators = 2,
    ).withNetworkBootstrap { implicit env =>
      new NetworkBootstrapper(S2M2)
    }

  /**   - 4 participants '''not''' connected to any synchronizer
    *   - 2 synchronizers with 1 sequencer and 1 mediator each
    */
  lazy val P4_S1M1_S1M1_Manual: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 4,
      numSequencers = 2,
      numMediators = 2,
    ).withManualStart

  /**   - 5 participants '''not''' connected to the synchronizer
    *   - 2 synchronizers with 1 sequencer and 1 mediator each
    */
  private lazy val P5_S1M1_S1M1_Config: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 5,
      numSequencers = 2,
      numMediators = 2,
    )

  /**   - 5 participants '''not''' connected to the synchronizer
    *   - 2 synchronizers with 1 sequencer and 1 mediator each
    */
  lazy val P5_S1M1_S1M1: EnvironmentDefinition =
    P5_S1M1_S1M1_Config.withNetworkBootstrap { implicit env =>
      import env.*
      new NetworkBootstrapper(
        NetworkTopologyDescription(
          daName,
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
        ),
        NetworkTopologyDescription(
          acmeName,
          synchronizerOwners = Seq(sequencer2),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer2),
          mediators = Seq(mediator2),
        ),
      )
    }

  lazy val P5_S1M1_S1M1_Manual: EnvironmentDefinition =
    P5_S1M1_S1M1_Config.withManualStart

  /**   - 5 participants
    *   - 4 sequencers
    *   - 4 mediators
    *   - no initialized synchronizer
    */
  lazy val P5S4M4_Manual: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 5,
      numSequencers = 4,
      numMediators = 4,
    ).withManualStart

  /**   - 1 participant
    *   - 4 sequencers
    *   - 1 mediators
    *   - no initialized synchronizer
    */
  lazy val P1_S2M1_Manual: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 1,
      numSequencers = 2,
      numMediators = 1,
    ).withManualStart

  /**   - 1 participant '''not''' connected to any synchronizer
    *   - 2 synchronizers with 2 sequencers and 1 mediator each
    */
  lazy val P1_S2M1_S2M1: EnvironmentDefinition = buildBaseEnvironmentDefinition(
    numParticipants = 1,
    numSequencers = 4,
    numMediators = 2,
  ).withNetworkBootstrap { implicit env =>
    NetworkBootstrapper(S2M1_S2M1)
  }

  /**   - 2 participants '''not''' connected to any synchronizer
    *   - 1 synchronizer with 2 sequencers and 1 mediator
    */
  lazy val P2_S2M1: EnvironmentDefinition = buildBaseEnvironmentDefinition(
    numParticipants = 2,
    numSequencers = 2,
    numMediators = 1,
  )
    .withNetworkBootstrap { implicit env =>
      NetworkBootstrapper(Seq(S2M1))
    }

  /**   - 2 participants '''not''' connected to any synchronizer
    *   - 2 synchronizers with 2 sequencers and 1 mediator each
    */
  lazy val P2_S2M1_S2M1: EnvironmentDefinition = buildBaseEnvironmentDefinition(
    numParticipants = 2,
    numSequencers = 4,
    numMediators = 2,
  )
    .withNetworkBootstrap { implicit env =>
      NetworkBootstrapper(S2M1_S2M1)
    }

  /**   - 3 participants '''not''' connected to any synchronizer
    *   - 2 synchronizers with 1 sequencer and 1 mediator each
    */
  lazy val P3_S1M1_S1M1: EnvironmentDefinition = buildBaseEnvironmentDefinition(
    numParticipants = 3,
    numSequencers = 2,
    numMediators = 2,
  )
    .withNetworkBootstrap { implicit env =>
      NetworkBootstrapper(S1M1_S1M1)
    }

  /**   - 3 participants '''not''' connected to any synchronizer
    *   - 2 synchronizers with 2 sequencers and 1 mediator each
    */
  lazy val P3_S2M1_S2M1: EnvironmentDefinition = buildBaseEnvironmentDefinition(
    numParticipants = 3,
    numSequencers = 4,
    numMediators = 2,
  )
    .withNetworkBootstrap { implicit env =>
      NetworkBootstrapper(S2M1_S2M1)
    }

  def fromResource(path: String): EnvironmentDefinition =
    EnvironmentDefinition(baseConfig = loadConfigFromResource(path))

  private def loadConfigFromResource(path: String): CantonConfig = {
    val rawConfig = ConfigFactory.parseString(Resource.getAsString(path))
    CantonConfig
      .loadAndValidate(rawConfig, EnterpriseCantonEdition)
      .valueOr { err =>
        // print a useful error message such that the developer can figure out which file failed
        logger.error(s"Failed to load file $path: $err", new Exception("location"))
        sys.exit(1)
      }
  }

  def fromFiles(files: File*): EnvironmentDefinition = {
    val config = CantonConfig.parseAndLoadOrExit(files.map(_.toJava), EnterpriseCantonEdition)
    EnvironmentDefinition(baseConfig = config)
  }

}
