// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import cats.syntax.option.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel
import com.digitalasset.canton.config.StorageConfig.Memory
import com.digitalasset.canton.config.{
  NonNegativeFiniteDuration as _,
  PositiveFiniteDuration as _,
  *,
}
import com.digitalasset.canton.console.FeatureFlag
import com.digitalasset.canton.participant.config.{
  LocalParticipantConfig,
  RemoteParticipantConfig,
  TestingTimeServiceConfig,
  UnsafeOnlinePartyReplicationConfig,
}
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.{
  BftSequencer,
  SequencerHighAvailabilityConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.{BlockSequencerConfig, SequencerConfig}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveFiniteDuration}
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, UniquePortGenerator, config}
import com.typesafe.config.ConfigValueFactory
import monocle.macros.syntax.lens.*
import monocle.macros.{GenLens, GenPrism}

import scala.concurrent.duration.*
import scala.util.Random

/** Utilities for transforming instances of [[CantonConfig]]. A transform itself is merely a
  * function of the signature {{{CantonEnterpriseConfig => CantonEnterpriseConfig}}}, abbreviated as
  * [[ConfigTransform]]; this object provides helpers for transforming particular synchronizer or
  * participant instances, or all of them. It also provides a set of transforms that will be run for
  * every integration test using [[BaseIntegrationTest]] unless
  * [[EnterpriseEnvironmentDefinition.clearConfigTransforms]] is called.
  */
object ConfigTransforms {

  sealed trait ConfigNodeType extends Product with Serializable
  object ConfigNodeType {
    case object Sequencer extends ConfigNodeType
    case object Mediator extends ConfigNodeType
    case object Participant extends ConfigNodeType
  }

  def applyMultiple(transforms: Seq[ConfigTransform])(
      config: CantonConfig
  ): CantonConfig =
    transforms.foldLeft(config) { case (cfg, transform) => transform(cfg) }

  /** A shared unique port instance to allow every test in a test run to assign unique ports. */
  val globallyUniquePorts: ConfigTransform =
    uniquePorts(UniquePortGenerator.next)

  def setProtocolVersion(pv: ProtocolVersion): Seq[ConfigTransform] = {
    def configTransformsWhen(predicate: Boolean)(transforms: => Seq[ConfigTransform]) =
      if (predicate) transforms else Seq()

    val enableAlpha = configTransformsWhen(pv.isAlpha)(enableAlphaVersionSupport)
    val enableBeta = configTransformsWhen(pv.isBeta)(setBetaSupport(true))

    val deprecatedPVWarning = if (pv.isDeprecated) dontWarnOnDeprecatedPV else Seq()

    val updateParticipants = Seq(
      updateAllParticipantConfigs_(
        _.focus(_.parameters.minimumProtocolVersion)
          .replace(Some(ParticipantProtocolVersion(pv)))
      )
    )

    val updateInitialProtocolVersion = updateAllInitialProtocolVersion(pv)

    updateParticipants ++ enableAlpha ++ enableBeta ++ deprecatedPVWarning :+ updateInitialProtocolVersion
  }

  val optSetProtocolVersion: Seq[ConfigTransform] = setProtocolVersion(
    BaseTest.testedProtocolVersion
  )

  val generousRateLimiting: ConfigTransform =
    updateAllParticipantConfigs_(
      _.focus(_.ledgerApi.rateLimit).replace(Some(RateLimitingConfig.Default))
    )

  /** Config transforms to apply to heavy-weight tests using an [[EnterpriseEnvironmentDefinition]].
    * For example, these transforms should be applied to toxiproxy tests.
    */
  val heavyTestDefaults: Seq[ConfigTransform] = optSetProtocolVersion ++
    setBetaSupport(BaseTest.testedProtocolVersion.isBeta) ++
    Seq(
      ConfigTransforms.uniqueH2DatabaseNames,
      ConfigTransforms.globallyUniquePorts,
      ConfigTransforms.generousRateLimiting,
      ConfigTransforms.enableAdvancedCommands(FeatureFlag.Preview),
      ConfigTransforms.enableAdvancedCommands(FeatureFlag.Testing),
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.parameters.adminWorkflow.bongTestMaxLevel)
          .replace(NonNegativeInt.tryCreate(20))
          .focus(_.parameters.ledgerApiServer.contractIdSeeding)
          .replace(Seeding.Weak)
      ),
      _.focus(_.parameters.enableAdditionalConsistencyChecks)
        .replace(true)
        .focus(_.monitoring.logging.logSlowFutures)
        .replace(true),
      enableSlowQueryLogging(),
      // turn off the warning message. can't set to None as None will simply be omitted
      // when writing the configuration for the external node tests
      _.focus(_.monitoring.logging.api.warnBeyondLoad).replace(Some(10000)),
      // disable exit on fatal error in tests
      ConfigTransforms.setExitOnFatalFailures(false),
    )

  lazy val dontWarnOnDeprecatedPV: Seq[ConfigTransform] = Seq(
    updateAllSequencerConfigs_(
      _.focus(_.parameters.dontWarnOnDeprecatedPV).replace(true)
    ),
    updateAllMediatorConfigs_(
      _.focus(_.parameters.dontWarnOnDeprecatedPV).replace(true)
    ),
    updateAllParticipantConfigs_(
      _.focus(_.parameters.dontWarnOnDeprecatedPV).replace(true)
    ),
  )

  /** Allow all preview and experimental features without compatibility guarantees
    */
  lazy val enableNonStandardConfig: ConfigTransform = setNonStandardConfig(true)

  /** Enable/disable all preview and experimental features without compatibility guarantees
    *
    * @param enable
    * @return
    */
  def setNonStandardConfig(enable: Boolean): ConfigTransform =
    _.focus(_.parameters.nonStandardConfig).replace(enable)

  def setGlobalAlphaVersionSupport(enable: Boolean): ConfigTransform =
    _.focus(_.parameters.alphaVersionSupport).replace(enable)

  def setGlobalBetaVersionSupport(enable: Boolean): ConfigTransform =
    _.focus(_.parameters.betaVersionSupport).replace(enable)

  def setExitOnFatalFailures(enable: Boolean): ConfigTransform =
    _.focus(_.parameters.exitOnFatalFailures).replace(enable)

  def setAlphaVersionSupport(enable: Boolean): Seq[ConfigTransform] = Seq(
    setNonStandardConfig(enable),
    setGlobalAlphaVersionSupport(enable),
    updateAllParticipantConfigs_(
      _.focus(_.parameters.alphaVersionSupport)
        .replace(enable)
    ),
  )

  def setBetaSupport(enable: Boolean): Seq[ConfigTransform] =
    Seq(
      setGlobalBetaVersionSupport(enable),
      updateAllParticipantConfigs_(
        _.focus(_.parameters.betaVersionSupport)
          .replace(enable)
      ),
    )

  def setStartupMemoryReportLevel(level: ReportingLevel): ConfigTransform =
    _.focus(_.parameters.startupMemoryCheckConfig).replace(StartupMemoryCheckConfig(level))

  lazy val enableAlphaVersionSupport: Seq[ConfigTransform] = setAlphaVersionSupport(true)

  val updateLedgerApiSlowProcessWarning: Seq[ConfigTransform] =
    Seq(
      ConfigTransforms.updateAllParticipantConfigs_(
        // extending the timeout for the warning about uninitialised in-memory package metadata view
        // the default for this value is set to 1 second which is not enough for the canton integration tests
        _.focus(_.ledgerApi.indexService.preparePackageMetadataTimeOutWarning)
          .replace(config.NonNegativeFiniteDuration.ofSeconds(10L))
      )
    )

  /** Default transforms to apply to tests using a [[EnterpriseEnvironmentDefinition]]. Covers the
    * primary ways that distinct concurrent environments may unintentionally collide.
    */
  val defaults: Seq[ConfigTransform] =
    heavyTestDefaults ++ updateLedgerApiSlowProcessWarning ++
      setBetaSupport(BaseTest.testedProtocolVersion.isBeta) ++ Seq(
        // make unbounded duration bounded for our test
        _.focus(_.parameters.timeouts.console.unbounded)
          .replace(config.NonNegativeDuration.tryFromDuration(3.minutes))
          .focus(_.parameters.timeouts.processing.unbounded)
          .replace(config.NonNegativeDuration.tryFromDuration(3.minutes))
          .focus(_.parameters.timeouts.processing.shutdownProcessing)
          .replace(config.NonNegativeDuration.tryFromDuration(10.seconds)),
        ConfigTransforms.modifyAllStorageConfigs { (_, _, storageConfig) =>
          // Increasing here the default timeout for providing more room for DB Servers under test load to provide a new connection
          val newConnectionTimeout = config.NonNegativeFiniteDuration.ofSeconds(30)
          storageConfig match {
            case memConfig: StorageConfig.Memory =>
              // NB: later storage plugins may use this value in the actual postgres / h2 configuration
              memConfig.focus(_.parameters.connectionTimeout).replace(newConnectionTimeout)
            case pgConfig: DbConfig.Postgres =>
              pgConfig.focus(_.parameters.connectionTimeout).replace(newConnectionTimeout)
            case h2Config: DbConfig.H2 =>
              h2Config.focus(_.parameters.connectionTimeout).replace(newConnectionTimeout)
          }
        },
      )

  def updateAllInitialProtocolVersion(pv: ProtocolVersion): ConfigTransform =
    updateAllParticipantConfigs_(
      _.focus(_.parameters.initialProtocolVersion).replace(ParticipantProtocolVersion(pv))
    )

  lazy val clearMinimumProtocolVersion: Seq[ConfigTransform] =
    Seq(
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.parameters.minimumProtocolVersion).replace(None)
      )
    )

  val allDefaultsButGloballyUniquePorts: Seq[ConfigTransform] =
    defaults.filter(_ != globallyUniquePorts)

  def identity: ConfigTransform = Predef.identity

  /** Update a canton config to assign port numbers from the given `startingPort` to all
    * synchronizers and participants
    */
  def uniquePorts(nextPort: => Port): ConfigTransform = {
    val updatedParticipants = updateAllParticipantConfigs_(
      _.focus(_.ledgerApi.internalPort)
        .replace(nextPort.some)
        .focus(_.adminApi.internalPort)
        .replace(nextPort.some)
        .focus(_.monitoring.grpcHealthServer)
        .modify(_.map(_.copy(internalPort = nextPort.some)))
    )

    val updatedSequencers = updateAllSequencerConfigs_(
      _.focus(_.publicApi.internalPort)
        .replace(nextPort.some)
        .focus(_.adminApi.internalPort)
        .replace(nextPort.some)
        .focus(_.monitoring.grpcHealthServer)
        .modify(_.map(_.copy(internalPort = nextPort.some)))
    )

    val updatedMediators = updateAllMediatorConfigs_(
      _.focus(_.adminApi.internalPort)
        .replace(nextPort.some)
        .focus(_.monitoring.grpcHealthServer)
        .modify(_.map(_.copy(internalPort = nextPort.some)))
    )

    updatedSequencers compose updatedMediators compose updatedParticipants
  }

  def modifyAllStorageConfigs(
      storageConfigModifier: (
          ConfigNodeType,
          String,
          StorageConfig,
      ) => StorageConfig
  ): ConfigTransform =
    updateAllParticipantConfigs { (nodeName, cfg) =>
      cfg.focus(_.storage).modify(storageConfigModifier(ConfigNodeType.Participant, nodeName, _))
    } compose updateAllSequencerConfigs { (nodeName, cfg) =>
      cfg
        .focus(_.storage)
        .modify(storageConfigModifier(ConfigNodeType.Sequencer, nodeName, _))
        .focus(_.sequencer)
        .modify(
          GenPrism[SequencerConfig, BftSequencer].modify(
            _.focus(_.config.storage).modify(
              _.map(
                storageConfigModifier(ConfigNodeType.Sequencer, nodeName, _)
              )
            )
          )
        )
    } compose updateAllMediatorConfigs { (nodeName, cfg) =>
      cfg.focus(_.storage).modify(storageConfigModifier(ConfigNodeType.Mediator, nodeName, _))
    }

  /** Replace all storage settings with in-memory stores */
  def allInMemory: ConfigTransform =
    modifyAllStorageConfigs((_, _, _) => StorageConfig.Memory())

  /** Use the given crypto factory config on all nodes with `nodeFilter(name)`.
    */
  def setCrypto(
      cryptoConfig: CryptoConfig,
      nodeFilter: String => Boolean = _ => true,
  ): ConfigTransform =
    updateAllParticipantConfigs {
      case (name, config) if nodeFilter(name) => config.focus(_.crypto).replace(cryptoConfig)
      case (_, config) => config
    } compose updateAllSequencerConfigs {
      case (name, config) if nodeFilter(name) => config.focus(_.crypto).replace(cryptoConfig)
      case (_, config) => config
    } compose updateAllMediatorConfigs {
      case (name, config) if nodeFilter(name) => config.focus(_.crypto).replace(cryptoConfig)
      case (_, config) => config
    }

  /** Configure the environment with static time */
  def useStaticTime: ConfigTransform = {
    val newDelayLoggingThreshold = config.NonNegativeFiniteDuration.ofDays(1000)
    val newSequencedEventProcessingBound = config.NonNegativeDuration.ofDays(1000)
    val mainUpdates: ConfigTransform =
      _.focus(_.parameters.clock)
        .replace(ClockConfig.SimClock)
        // set to arbitrary large periods as we're not using a real clock
        .focus(_.monitoring.logging.delayLoggingThreshold)
        .replace(newDelayLoggingThreshold)
        .focus(_.parameters.timeouts.processing.sequencedEventProcessingBound)
        .replace(newSequencedEventProcessingBound)

    // Update the payload to event margin so that the sequencer will not warn
    // if the clock jumps forwards between when a payload has been written and batch is sequenced
    def updateSequencerConfig(
        sequencerConfig: SequencerConfig
    ): SequencerConfig =
      sequencerConfig match {
        case dbConfig: SequencerConfig.Database =>
          dbConfig
            .focus(_.writer)
            .modify(_.modify(payloadToEventMargin = config.NonNegativeFiniteDuration.ofDays(1000)))
        case other => other
      }

    val sequencerWriteBound = updateAllSequencerConfigs_(
      _.focus(_.sequencer)
        .modify(updateSequencerConfig)
        // set token expiration interval to large value
        // keeping it too low will mean that when you advance the clock by e.g. one hour
        // the auth token will be removed and some send async requests will bounce randomly.
        // unfortunately, some of the retry logic would depend on time advancing which isn't
        // given in our integration test
        .focus(_.publicApi.maxTokenExpirationInterval)
        .replace(config.NonNegativeFiniteDuration.ofDays(10000))
    )

    mainUpdates compose sequencerWriteBound
  }

  /** Enable the testing time service in the ledger API */
  def useTestingTimeService: LocalParticipantConfig => LocalParticipantConfig =
    _.focus(_.testingTime).replace(Some(TestingTimeServiceConfig.MonotonicTime))

  /** Disable exclusion of infrastructure from transaction metering */
  def meterInfrastructure: ConfigTransform =
    updateAllParticipantConfigs_(
      _.focus(_.parameters.excludeInfrastructureTransactions).replace(false)
    )

  def updateContractIdSeeding(seeding: Seeding): ConfigTransform =
    updateAllParticipantConfigs_(
      _.focus(_.parameters.ledgerApiServer.contractIdSeeding).replace(seeding)
    )

  def generateUniqueH2DatabaseName(nodeName: String): String = {
    val dbPrefix = Random.alphanumeric.take(8).map(_.toLower).mkString
    s"${dbPrefix}_$nodeName"
  }

  def withUniqueDbName(
      nodeName: String,
      storageConfig: StorageConfig,
  ): StorageConfig =
    storageConfig match {
      case h2: DbConfig.H2 =>
        // Make sure that each environment and its database names are unique by generating a random prefix
        val dbName = generateUniqueH2DatabaseName(nodeName)
        h2.copy(config =
          h2.config.withValue(
            "url",
            ConfigValueFactory.fromAnyRef(
              s"jdbc:h2:mem:$dbName;MODE=PostgreSQL;LOCK_TIMEOUT=10000;DB_CLOSE_DELAY=-1"
            ),
          )
        )
      case x => x
    }

  def uniqueH2DatabaseNames: ConfigTransform =
    modifyAllStorageConfigs((_, nodeName, config) => withUniqueDbName(nodeName, config))

  def updateParticipantConfig(participantName: String)(
      update: LocalParticipantConfig => LocalParticipantConfig
  ): ConfigTransform =
    cantonConfig =>
      cantonConfig
        .focus(_.participants)
        .index(InstanceName.tryCreate(participantName))
        .modify(update)

  def updateAllParticipantConfigs_(
      update: LocalParticipantConfig => LocalParticipantConfig
  ): ConfigTransform =
    updateAllParticipantConfigs((_, participantConfig) => update(participantConfig))

  def updateAllParticipantConfigs(
      update: (String, LocalParticipantConfig) => LocalParticipantConfig
  ): ConfigTransform =
    cantonConfig =>
      cantonConfig
        .focus(_.participants)
        .modify(_.map { case (pName, pConfig) => (pName, update(pName.unwrap, pConfig)) })

  def updateRemoteParticipantConfig(remoteParticipantName: String)(
      update: RemoteParticipantConfig => RemoteParticipantConfig
  ): ConfigTransform =
    cantonConfig =>
      cantonConfig
        .focus(_.remoteParticipants)
        .index(InstanceName.tryCreate(remoteParticipantName))
        .modify(update)

  def updateSequencerConfig(name: String)(
      update: SequencerNodeConfig => SequencerNodeConfig
  ): ConfigTransform =
    cantonConfig =>
      cantonConfig.focus(_.sequencers).index(InstanceName.tryCreate(name)).modify(update)

  def updateAllSequencerConfigs(
      update: (String, SequencerNodeConfig) => SequencerNodeConfig
  ): ConfigTransform =
    cantonConfig =>
      cantonConfig
        .focus(_.sequencers)
        .modify(_.map { case (sName, sConfig) => (sName, update(sName.unwrap, sConfig)) })

  def updateAllSequencerConfigs_(
      update: SequencerNodeConfig => SequencerNodeConfig
  ): ConfigTransform =
    updateAllSequencerConfigs((_, config) => update(config))

  def updateMediatorConfig(name: String)(
      update: MediatorNodeConfig => MediatorNodeConfig
  ): ConfigTransform = cantonConfig =>
    cantonConfig.focus(_.mediators).index(InstanceName.tryCreate(name)).modify(update)

  def updateAllMediatorConfigs_(
      update: MediatorNodeConfig => MediatorNodeConfig
  ): ConfigTransform =
    updateAllMediatorConfigs((_, config) => update(config))

  def updateAllMediatorConfigs(
      update: (String, MediatorNodeConfig) => MediatorNodeConfig
  ): ConfigTransform =
    cantonConfig =>
      cantonConfig
        .focus(_.mediators)
        .modify(_.map { case (pName, pConfig) => (pName, update(pName.unwrap, pConfig)) })

  def updateAllSequencerClientConfigs(
      update: (ConfigNodeType, String, SequencerClientConfig) => SequencerClientConfig
  ): ConfigTransform =
    updateAllParticipantConfigs((name, config) =>
      config.focus(_.sequencerClient).modify(update(ConfigNodeType.Participant, name, _))
    ).compose(
      updateAllMediatorConfigs((name, config) =>
        config.focus(_.sequencerClient).modify(update(ConfigNodeType.Mediator, name, _))
      )
    ).compose(
      updateAllSequencerConfigs((name, config) =>
        config.focus(_.sequencerClient).modify(update(ConfigNodeType.Sequencer, name, _))
      )
    )

  def updateAllSequencerClientConfigs_(
      update: SequencerClientConfig => SequencerClientConfig
  ): ConfigTransform =
    updateAllSequencerClientConfigs((_, _, config) => update(config))

  def enableAdvancedCommands(
      featureFlag: FeatureFlag
  ): ConfigTransform =
    cfg =>
      featureFlag match {
        case FeatureFlag.Preview => cfg.focus(_.features.enablePreviewCommands).replace(true)
        case FeatureFlag.Testing => cfg.focus(_.features.enableTestingCommands).replace(true)
        case FeatureFlag.Repair => cfg.focus(_.features.enableRepairCommands).replace(true)
        case FeatureFlag.Stable =>
          throw new IllegalArgumentException("Stable commands are already enabled")
      }

  /** Configures auto-init option for all nodes. Sequencer nodes are imperatively set with auto-init
    * \== false and this cannot be modified.
    *
    * @param identity
    *   controls how the node identity (prefix of the unique identifier) is determined. If defined
    *   the node will be set to auto-init.
    * @param listNodeNamesO
    *   the list of nodes to apply the new configuration to. If None it applies the transformation
    *   to all nodes.
    */
  private def setAutoInit(
      identity: Option[InitConfigBase.Identity],
      listNodeNamesO: Option[Set[String]],
  ) =
    updateAllSequencerConfigs { case (name, config) =>
      listNodeNamesO match {
        case Some(listNodeNames) if listNodeNames.contains(name) =>
          config.focus(_.init.identity).replace(identity)
        case _ => config
      }
    }
      .compose(updateAllMediatorConfigs { case (name, config) =>
        listNodeNamesO match {
          case Some(listNodeNames) if listNodeNames.contains(name) =>
            config.focus(_.init.identity).replace(identity)
          case _ => config
        }
      })
      .compose(updateAllParticipantConfigs { case (name, config) =>
        listNodeNamesO match {
          case Some(listNodeNames) if listNodeNames.contains(name) =>
            config.focus(_.init.identity).replace(identity)
          case _ => config
        }
      })

  def disableAutoInit(listNodeNames: Set[String]): ConfigTransform =
    setAutoInit(None, Some(listNodeNames))

  /** For performance tests...
    */
  def disableAdditionalConsistencyChecks: ConfigTransform =
    _.focus(_.parameters.enableAdditionalConsistencyChecks).replace(false)

  def setParticipantMaxConnections(maxConnections: PositiveInt): ConfigTransform =
    modifyAllStorageConfigs { (nodeType, _, config) =>
      nodeType match {
        case ConfigNodeType.Participant =>
          config match {
            case dbConfig: ModifiableDbConfig[?] =>
              dbConfig.modify(parameters =
                dbConfig.parameters.focus(_.maxConnections).replace(Some(maxConnections))
              )
            case _: Memory => sys.error(s"Cannot set max connections for in-memory storage")
          }
        case _ =>
          config
      }
    }

  def setStorageQueueSize(queueSize: Int): ConfigTransform =
    modifyAllStorageConfigs { (_, _, config) =>
      config match {
        case dbConfig: ModifiableDbConfig[_] =>
          dbConfig.modify(config =
            config.config.withValue("queueSize", ConfigValueFactory.fromAnyRef(queueSize))
          )
        case _: Memory => sys.error(s"Cannot set storage queue size for in-memory storage")
      }
    }

  def updatePruningBatchSize(batchSize: PositiveInt): ConfigTransform =
    updateAllParticipantConfigs_(
      _.focus(_.parameters.batching.maxPruningBatchSize)
        .replace(batchSize)
        .focus(_.parameters.batching.ledgerApiPruningBatchSize)
        .replace(batchSize)
    )

  def updateMediatorPruningBatchSize(batchSize: PositiveInt): ConfigTransform =
    updateAllMediatorConfigs_(
      _.focus(_.mediator.pruning.maxPruningBatchSize).replace(batchSize)
    )

  def updateAllDatabaseSequencerConfigs(
      updateDbSequencerConfig: SequencerConfig.Database => SequencerConfig.Database
  ): ConfigTransform = {
    def update: SequencerConfig => SequencerConfig = {
      case x: SequencerConfig.Database => updateDbSequencerConfig(x)
      case x => x
    }
    updateAllSequencerConfigs_(
      _.focus(_.sequencer).modify(update)
    )
  }

  def updateAllBlockSequencerConfigs(
      updateBlockSequencerConfig: BlockSequencerConfig => BlockSequencerConfig
  ): ConfigTransform = {
    def update: SequencerConfig => SequencerConfig = {
      case x: SequencerConfig.External =>
        x.focus(_.block).modify(updateBlockSequencerConfig)
      case x: SequencerConfig.BftSequencer =>
        x.focus(_.block).modify(updateBlockSequencerConfig)
      case x => x
    }
    updateAllSequencerConfigs_(_.focus(_.sequencer).modify(update))
  }

  def updateDatabaseSequencerPruningBatchSize(batchSize: PositiveInt): ConfigTransform =
    updateAllDatabaseSequencerConfigs(
      _.focus(_.pruning.maxPruningBatchSize).replace(batchSize)
    )

  def updateSequencerCheckpointInterval(interval: NonNegativeFiniteDuration): ConfigTransform =
    updateAllDatabaseSequencerConfigs(
      _.focus(_.reader.checkpointInterval).replace(interval.toConfig)
    )

  def updateSequencerExclusiveStorageFailoverInterval(
      interval: PositiveFiniteDuration
  ): ConfigTransform =
    updateAllDatabaseSequencerConfigs(
      _.focus(_.highAvailability).some
        .andThen(GenLens[SequencerHighAvailabilityConfig](_.exclusiveStorage.connectionPool))
        .modify(_.copy(healthCheckPeriod = interval.toConfig, activeTimeout = interval.toConfig))
    )

  def updateSynchronizerTimeTrackerConfigs_(
      update: SynchronizerTimeTrackerConfig => SynchronizerTimeTrackerConfig
  ): ConfigTransform =
    updateAllSequencerConfigs_(
      _.focus(_.timeTracker).modify(update)
    ) compose updateAllMediatorConfigs_(
      _.focus(_.timeTracker).modify(update)
    )

  def updateSynchronizerTimeTrackerInterval(interval: NonNegativeFiniteDuration): ConfigTransform =
    updateSynchronizerTimeTrackerConfigs_(
      _.focus(_.minObservationDuration).replace(interval.toConfig)
    )

  def updateSequencerClientAcknowledgementInterval(
      interval: NonNegativeFiniteDuration
  ): ConfigTransform =
    updateAllSequencerClientConfigs_(_.focus(_.acknowledgementInterval).replace(interval.toConfig))

  def updateMaxDeduplicationDurations(
      maxDeduplicationDuration: java.time.Duration
  ): ConfigTransform =
    updateAllParticipantConfigs_(
      _.focus(_.init.ledgerApi.maxDeduplicationDuration)
        .replace(config.NonNegativeFiniteDuration(maxDeduplicationDuration))
    )

  /** Flag the provided participants as being replicated. Keep in mind to actually work they need to
    * be configured to share the same database (see
    * [[com.digitalasset.canton.integration.plugins.UseSharedStorage]]).
    *
    * Enables replication for all participants if no participants are given
    */
  def enableReplicatedParticipants(
      replicatedParticipantNames: String*
  ): ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs { (name, config) =>
      if (replicatedParticipantNames.isEmpty || replicatedParticipantNames.contains(name))
        config
          .focus(_.replication)
          .modify(
            _.map(_.focus(_.enabled).replace(Some(true)))
              .orElse(Some(ReplicationConfig(enabled = Some(true))))
          )
      else config
    }

  def enableReplicatedMediators(
      replicatedMediatorNames: String*
  ): ConfigTransform =
    ConfigTransforms.updateAllMediatorConfigs { (name, config) =>
      if (replicatedMediatorNames.isEmpty || replicatedMediatorNames.contains(name))
        config
          .focus(_.replication)
          .modify(
            _.map(_.focus(_.enabled).replace(Some(true)))
              .orElse(Some(ReplicationConfig(enabled = Some(true))))
          )
      else config
    }

  def setPassiveCheckPeriodMediators(
      passiveCheckPeriod: config.PositiveFiniteDuration,
      mediatorNames: String*
  ): ConfigTransform =
    ConfigTransforms.updateAllMediatorConfigs { (name, config) =>
      if (mediatorNames.isEmpty || mediatorNames.contains(name)) {
        config
          .focus(_.replication)
          .some
          .andThen(GenLens[ReplicationConfig](_.connectionPool.connection.passiveCheckPeriod))
          .replace(passiveCheckPeriod)
      } else config
    }

  def addMonitoringEndpointForSequencers: Seq[ConfigTransform] =
    Seq(
      ConfigTransforms.updateAllSequencerConfigs_ {
        _.focus(_.monitoring.grpcHealthServer)
          .replace(
            Some(
              GrpcHealthServerConfig(internalPort = UniquePortGenerator.next.some)
            )
          )
      }
    )

  def addMonitoringEndpointAllNodes: Seq[ConfigTransform] = {
    def adjustMonitoring(config: NodeMonitoringConfig): NodeMonitoringConfig =
      config
        .focus(_.grpcHealthServer)
        .replace(
          Some(
            GrpcHealthServerConfig(internalPort = UniquePortGenerator.next.some)
          )
        )
        .focus(_.httpHealthServer)
        .replace(
          Some(
            HttpHealthServerConfig(port = UniquePortGenerator.next)
          )
        )
    Seq(
      ConfigTransforms.updateAllParticipantConfigs_ {
        _.focus(_.monitoring).modify(adjustMonitoring)
      },
      ConfigTransforms.updateAllMediatorConfigs_ {
        _.focus(_.monitoring).modify(adjustMonitoring)
      },
      ConfigTransforms.updateAllSequencerConfigs_ {
        _.focus(_.monitoring).modify(adjustMonitoring)
      },
    )
  }

  def enableReplicatedAllNodes: Seq[ConfigTransform] =
    Seq(
      enableReplicatedParticipants(),
      enableReplicatedMediators(),
    )

  def enableSlowQueryLogging(
      slowQuery: PositiveFiniteDuration = PositiveFiniteDuration.tryOfSeconds(30)
  ): ConfigTransform = cantonConfig => {
    def replace(params: DbParametersConfig): DbParametersConfig =
      params.focus(_.warnOnSlowQuery).replace(Some(slowQuery.toConfig))
    val withSlowQueryLogging = ConfigTransforms
      .modifyAllStorageConfigs {
        case (_, _, storage: DbConfig.H2) =>
          storage.focus(_.parameters).modify(replace)
        case (_, _, storage: DbConfig.Postgres) =>
          storage.focus(_.parameters).modify(replace)
        case (_, _, storage: StorageConfig.Memory) => storage
      }(cantonConfig)

    // turn on query cost logging so we get decent location info
    withSlowQueryLogging
      .focus(_.monitoring.logging.queryCost)
      .modify(
        _.orElse(
          Some(QueryCostMonitoringConfig(every = config.NonNegativeFiniteDuration.ofSeconds(30)))
        )
      )
  }

  /** Enables remote mediators
    *
    * Alternatively use [[EnterpriseEnvironmentDefinition.buildBaseEnterpriseEnvironmentDefinition]]
    * with `withRemote = true`
    */
  def enableRemoteMediators(source: String, target: String): ConfigTransform = cantonConfig => {
    val remote = cantonConfig.mediators
      .getOrElse(
        InstanceName.tryCreate(source),
        throw new IllegalArgumentException(s"no such mediator $source"),
      )
      .toRemoteConfig
    cantonConfig
      .focus(_.remoteMediators)
      .modify(_ + (InstanceName.tryCreate(target) -> remote))
  }

  def defaultsForNodes: Seq[ConfigTransform] =
    setProtocolVersion(ProtocolVersion.v33) :+
      ConfigTransforms.updateAllInitialProtocolVersion(ProtocolVersion.v33)

  def setTopologyTransactionRegistrationTimeout(
      timeout: config.NonNegativeDuration
  ): Seq[ConfigTransform] = Seq(
    updateAllParticipantConfigs_(
      _.focus(_.topology.topologyTransactionRegistrationTimeout).replace(timeout)
    ),
    updateAllMediatorConfigs_(
      _.focus(_.topology.topologyTransactionRegistrationTimeout).replace(timeout)
    ),
    updateAllSequencerConfigs_(
      _.focus(_.topology.topologyTransactionRegistrationTimeout).replace(timeout)
    ),
  )

  def unsafeEnableOnlinePartyReplication: Seq[ConfigTransform] = Seq(
    updateAllParticipantConfigs_(
      _.focus(_.parameters.unsafeOnlinePartyReplication)
        .replace(Some(UnsafeOnlinePartyReplicationConfig()))
    ),
    updateAllSequencerConfigs_(
      _.focus(_.parameters.unsafeEnableOnlinePartyReplication).replace(true)
    ),
  )
}
