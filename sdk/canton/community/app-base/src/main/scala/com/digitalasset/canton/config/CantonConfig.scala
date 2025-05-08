// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

////////////////////////////////////////////////////////
// DO NOT USE INTELLIJ OPTIMIZE-IMPORT AS IT WILL REMOVE
// SOME OF THE IMPLICIT IMPORTS NECESSARY TO COMPILE
////////////////////////////////////////////////////////

import cats.data.Validated
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.jwt.JwtTimestampLeeway
import com.daml.metrics.api.MetricQualification
import com.daml.metrics.{HistogramDefinition, MetricsFilterConfig}
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.auth.{AccessLevel, AuthorizedUser}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.*
import com.digitalasset.canton.config.ConfigErrors.{
  CannotParseFilesError,
  CannotReadFilesError,
  CantonConfigError,
  GenericConfigError,
  NoConfigFiles,
  SubstitutionError,
}
import com.digitalasset.canton.config.InitConfigBase.NodeIdentifierConfig
import com.digitalasset.canton.config.PackageMetadataViewConfig
import com.digitalasset.canton.config.RequireTypes.*
import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.console.{AmmoniteConsoleConfig, FeatureFlag}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.crypto.kms.driver.v1.DriverKms
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.http.{HttpServerConfig, JsonApiConfig, WebsocketConfig}
import com.digitalasset.canton.ledger.runner.common.PureConfigReaderWriter.Secure.{
  commandConfigurationConvert,
  dbConfigPostgresDataSourceConfigConvert,
  identityProviderManagementConfigConvert,
  indexServiceConfigConvert,
  indexerConfigConvert,
  partyManagementServiceConfigConvert,
  userManagementServiceConfigConvert,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.AdminWorkflowConfig
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.config.ParticipantInitConfig.ParticipantLedgerApiInitConfig
import com.digitalasset.canton.participant.sync.CommandProgressTrackerConfig
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.{
  EngineLoggingConfig,
  RateLimitingConfig,
}
import com.digitalasset.canton.platform.config.{
  InteractiveSubmissionServiceConfig,
  TopologyAwarePackageSelectionConfig,
}
import com.digitalasset.canton.pureconfigutils.SharedConfigReaders.catchConvertError
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.block.{SequencerDriver, SequencerDriverFactory}
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.mediator.{
  MediatorConfig,
  MediatorNodeConfig,
  MediatorNodeParameterConfig,
  MediatorNodeParameters,
  MediatorPruningConfig,
  RemoteMediatorConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.{
  DatabaseSequencerExclusiveStorageConfig,
  SequencerHighAvailabilityConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.DriverBlockSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.{
  BftBlockOrdererConfig,
  BftSequencerFactory,
}
import com.digitalasset.canton.synchronizer.sequencer.config.{
  RemoteSequencerConfig,
  SequencerNodeConfig,
  SequencerNodeParameterConfig,
  SequencerNodeParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.BytesUnit
import com.typesafe.config.ConfigException.UnresolvedSubstitution
import com.typesafe.config.{
  Config,
  ConfigException,
  ConfigFactory,
  ConfigList,
  ConfigMemorySize,
  ConfigObject,
  ConfigRenderOptions,
  ConfigValue,
  ConfigValueFactory,
}
import com.typesafe.scalalogging.LazyLogging
import monocle.macros.syntax.lens.*
import org.apache.pekko.stream.ThrottleMode
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.*
import pureconfig.ConfigReader.Result
import pureconfig.error.{CannotConvert, ConfigReaderFailures, ConvertFailure}
import pureconfig.generic.{FieldCoproductHint, ProductHint}

import java.io.File
import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.util.Try

/** Deadlock detection configuration
  *
  * A simple deadlock detection method. Using a background scheduler, we schedule a trivial future
  * on the EC. If the Future is not executed until we check again, we alert.
  *
  * @param enabled
  *   if true, we'll monitor the EC for deadlocks (or slow processings)
  * @param interval
  *   how often we check the EC
  * @param warnInterval
  *   how often we report a deadlock as still being active
  */
final case class DeadlockDetectionConfig(
    enabled: Boolean = true,
    interval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(3),
    warnInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(10),
) extends UniformCantonConfigValidation

object DeadlockDetectionConfig {
  implicit val deadlockDetectionConfigCantonConfigValidator
      : CantonConfigValidator[DeadlockDetectionConfig] =
    CantonConfigValidatorDerivation[DeadlockDetectionConfig]
}

/** Configuration for metrics and tracing
  *
  * @param deadlockDetection
  *   Should we regularly check our environment EC for deadlocks?
  * @param metrics
  *   Optional Metrics Reporter used to expose internally captured metrics
  * @param tracing
  *   Tracing configuration
  *
  * @param dumpNumRollingLogFiles
  *   How many of the rolling log files shold be included in the remote dump. Default is 0.
  */
final case class MonitoringConfig(
    deadlockDetection: DeadlockDetectionConfig = DeadlockDetectionConfig(),
    metrics: MetricsConfig = MetricsConfig(),
    tracing: TracingConfig = TracingConfig(),
    logging: LoggingConfig = LoggingConfig(),
    dumpNumRollingLogFiles: NonNegativeInt = MonitoringConfig.defaultDumpNumRollingLogFiles,
) extends LazyLogging
    with UniformCantonConfigValidation

object MonitoringConfig {
  implicit val monitoringConfigCantonConfigValidator: CantonConfigValidator[MonitoringConfig] = {
    import CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[MonitoringConfig]
  }

  private val defaultDumpNumRollingLogFiles = NonNegativeInt.tryCreate(0)
}

/** Configuration for console command timeouts
  *
  * @param bounded
  *   timeout on how long "bounded" operations, i.e. operations which normally are supposed to
  *   conclude in a fixed timeframe can run before the console considers them as failed.
  * @param unbounded
  *   timeout on how long "unbounded" operations can run, potentially infinite.
  * @param ledgerCommand
  *   default timeout used for ledger commands
  * @param ping
  *   default ping timeout
  * @param testingBong
  *   default bong timeout
  */
final case class ConsoleCommandTimeout(
    bounded: NonNegativeDuration = ConsoleCommandTimeout.defaultBoundedTimeout,
    unbounded: NonNegativeDuration = ConsoleCommandTimeout.defaultUnboundedTimeout,
    ledgerCommand: NonNegativeDuration = ConsoleCommandTimeout.defaultLedgerCommandsTimeout,
    ping: NonNegativeDuration = ConsoleCommandTimeout.defaultPingTimeout,
    testingBong: NonNegativeDuration = ConsoleCommandTimeout.defaultTestingBongTimeout,
) extends UniformCantonConfigValidation

object ConsoleCommandTimeout {
  implicit val consoleCommandTimeoutCantonConfigValidator
      : CantonConfigValidator[ConsoleCommandTimeout] =
    CantonConfigValidatorDerivation[ConsoleCommandTimeout]

  val defaultBoundedTimeout: NonNegativeDuration = NonNegativeDuration.tryFromDuration(1.minute)
  val defaultUnboundedTimeout: NonNegativeDuration =
    NonNegativeDuration.tryFromDuration(Duration.Inf)
  val defaultLedgerCommandsTimeout: NonNegativeDuration =
    NonNegativeDuration.tryFromDuration(1.minute)
  val defaultPingTimeout: NonNegativeDuration = NonNegativeDuration.tryFromDuration(20.seconds)
  val defaultTestingBongTimeout: NonNegativeDuration = NonNegativeDuration.tryFromDuration(1.minute)
}

/** Timeout settings configuration */
final case class TimeoutSettings(
    console: ConsoleCommandTimeout = ConsoleCommandTimeout(),
    processing: ProcessingTimeout = ProcessingTimeout(),
) extends UniformCantonConfigValidation

object TimeoutSettings {
  implicit val timeoutSettingsCantonConfigValidator: CantonConfigValidator[TimeoutSettings] =
    CantonConfigValidatorDerivation[TimeoutSettings]
}

sealed trait ClockConfig extends Product with Serializable
object ClockConfig {
  implicit val clockConfigCantonConfigValidator: CantonConfigValidator[ClockConfig] =
    CantonConfigValidatorDerivation[ClockConfig]

  /** Configure Canton to use a simclock
    *
    * A SimClock's time only progresses when [[com.digitalasset.canton.time.SimClock.advance]] is
    * explicitly called.
    */
  case object SimClock extends ClockConfig with UniformCantonConfigValidation

  /** Configure Canton to use the wall clock (default)
    *
    * @param skews
    *   map of clock skews, indexed by node name (used for testing, default empty) Any node whose
    *   name is contained in the map will use a WallClock, but the time of the clock will by shifted
    *   by the associated duration, which can be positive or negative. The clock shift will be
    *   constant during the life of the node.
    */
  final case class WallClock(skews: Map[String, FiniteDuration] = Map.empty)
      extends ClockConfig
      with UniformCantonConfigValidation

  /** Configure Canton to use a remote clock
    *
    * In crash recovery testing scenarios, we want several processes to use the same time. In most
    * cases, we can rely on NTP and the host clock. However, in cases where we test static time, we
    * need the spawn processes to access the main processes clock. For such cases we can use a
    * remote clock. However, no user should ever require this.
    * @param remoteApi
    *   admin-port of the node to read the time from
    */
  final case class RemoteClock(remoteApi: FullClientConfig)
      extends ClockConfig
      with UniformCantonConfigValidation

}

/** Default retention periods used by pruning commands where no values are explicitly specified.
  * Although by default the commands will retain enough data to remain operational, however
  * operators may like to retain more than this to facilitate possible disaster recovery scenarios
  * or retain evidence of completed transactions.
  */
final case class RetentionPeriodDefaults(
    sequencer: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(7),
    mediator: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(7),
) extends UniformCantonConfigValidation

object RetentionPeriodDefaults {
  implicit val retentionPeriodDefaultsCantonConfigValidator
      : CantonConfigValidator[RetentionPeriodDefaults] =
    CantonConfigValidatorDerivation[RetentionPeriodDefaults]
}

/** Parameters for testing Canton. Use default values in a production environment.
  *
  * @param enableAdditionalConsistencyChecks
  *   if true, run additional consistency checks. This will degrade performance.
  * @param manualStart
  *   If set to true, the nodes have to be manually started via console (default false)
  * @param startupParallelism
  *   Start up to N nodes in parallel (default is num-threads)
  * @param nonStandardConfig
  *   don't fail config validation on non-standard configuration settings
  * @param sessionSigningKeys
  *   Configure the use of session signing keys in the protocol
  * @param alphaVersionSupport
  *   If true, allow synchronizer nodes to use alpha protocol versions and participant nodes to
  *   connect to such synchronizers
  * @param betaVersionSupport
  *   If true, allow synchronizer nodes to use beta protocol versions and participant nodes to
  *   connect to such synchronizers
  * @param timeouts
  *   Sets the timeouts used for processing and console
  * @param portsFile
  *   A ports file name, where the ports of all participants will be written to after startup
  * @param exitOnFatalFailures
  *   If true the node will exit/stop the process in case of fatal failures
  * @param enableAlphaStateViaConfig
  *   If true, we will start the declarative api functionality
  * @param stateRefreshInterval
  *   If configured, the config file will be reread in the given interval to allow dynamic
  *   properties to be picked up immediately
  */
final case class CantonParameters(
    clock: ClockConfig = ClockConfig.WallClock(),
    enableAdditionalConsistencyChecks: Boolean = false,
    manualStart: Boolean = false,
    startupParallelism: Option[PositiveInt] = None,
    nonStandardConfig: Boolean = false,
    sessionSigningKeys: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
    alphaVersionSupport: Boolean = false,
    betaVersionSupport: Boolean = false,
    portsFile: Option[String] = None,
    timeouts: TimeoutSettings = TimeoutSettings(),
    retentionPeriodDefaults: RetentionPeriodDefaults = RetentionPeriodDefaults(),
    console: AmmoniteConsoleConfig = AmmoniteConsoleConfig(),
    exitOnFatalFailures: Boolean = true,
    startupMemoryCheckConfig: StartupMemoryCheckConfig = StartupMemoryCheckConfig(
      ReportingLevel.Warn
    ),
    enableAlphaStateViaConfig: Boolean = false,
    stateRefreshInterval: Option[config.NonNegativeFiniteDuration] = None,
) extends UniformCantonConfigValidation {
  def getStartupParallelism(numThreads: PositiveInt): PositiveInt =
    startupParallelism.getOrElse(numThreads)
}
object CantonParameters {
  implicit val cantonParametersCantonConfigValidator: CantonConfigValidator[CantonParameters] = {
    import CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[CantonParameters]
  }
}

/** Control which features are turned on / off in Canton
  *
  * @param enablePreviewCommands
  *   Feature flag to enable the set of commands that use functionality which we don't deem stable.
  * @param enableTestingCommands
  *   Feature flag to enable the set of commands used by Canton developers for testing purposes.
  * @param enableRepairCommands
  *   Feature flag to enable the set of commands used by Canton operators for manual repair
  *   purposes.
  */
final case class CantonFeatures(
    enablePreviewCommands: Boolean = false,
    enableTestingCommands: Boolean = false,
    enableRepairCommands: Boolean = false,
) extends UniformCantonConfigValidation {
  def featureFlags: Set[FeatureFlag] =
    (Seq(FeatureFlag.Stable)
      ++ (if (enableTestingCommands) Seq(FeatureFlag.Testing) else Seq())
      ++ (if (enablePreviewCommands) Seq(FeatureFlag.Preview) else Seq())
      ++ (if (enableRepairCommands) Seq(FeatureFlag.Repair) else Seq())).toSet
}
object CantonFeatures {
  implicit val cantonFeaturesCantonConfigValidator: CantonConfigValidator[CantonFeatures] =
    CantonConfigValidatorDerivation[CantonFeatures]
}

/** Root configuration parameters for a single Canton process.
  *
  * @param participants
  *   All locally running participants that this Canton process can connect and operate on.
  * @param remoteParticipants
  *   All remotely running participants to which the console can connect and operate on.
  * @param sequencers
  *   All locally running sequencers that this Canton process can connect and operate on.
  * @param remoteSequencers
  *   All remotely running sequencers that this Canton process can connect and operate on.
  * @param mediators
  *   All locally running mediators that this Canton process can connect and operate on.
  * @param remoteMediators
  *   All remotely running mediators that this Canton process can connect and operate on.
  * @param monitoring
  *   determines how this Canton process can be monitored
  * @param parameters
  *   per-environment parameters to control enabled features and set testing parameters
  * @param features
  *   control which features are enabled
  */
final case class CantonConfig(
    sequencers: Map[InstanceName, SequencerNodeConfig] = Map.empty,
    mediators: Map[InstanceName, MediatorNodeConfig] = Map.empty,
    participants: Map[InstanceName, ParticipantNodeConfig] = Map.empty,
    remoteSequencers: Map[InstanceName, RemoteSequencerConfig] = Map.empty,
    remoteMediators: Map[InstanceName, RemoteMediatorConfig] = Map.empty,
    remoteParticipants: Map[InstanceName, RemoteParticipantConfig] = Map.empty,
    monitoring: MonitoringConfig = MonitoringConfig(),
    parameters: CantonParameters = CantonParameters(),
    features: CantonFeatures = CantonFeatures(),
) extends UniformCantonConfigValidation
    with ConfigDefaults[DefaultPorts, CantonConfig] {

  def allNodes: Map[InstanceName, LocalNodeConfig] =
    (participants: Map[InstanceName, LocalNodeConfig]) ++ sequencers ++ mediators

  /** Use `participants` instead!
    */
  def participantsByString: Map[String, ParticipantNodeConfig] = participants.map { case (n, c) =>
    n.unwrap -> c
  }

  /** Use `sequencers` instead!
    */
  def sequencersByString: Map[String, SequencerNodeConfig] = sequencers.map { case (n, c) =>
    n.unwrap -> c
  }

  /** Use `remoteSequencers` instead!
    */
  def remoteSequencersByString: Map[String, RemoteSequencerConfig] = remoteSequencers.map {
    case (n, c) =>
      n.unwrap -> c
  }

  /** Use `mediators` instead!
    */
  def mediatorsByString: Map[String, MediatorNodeConfig] = mediators.map { case (n, c) =>
    n.unwrap -> c
  }

  /** Use `remoteMediators` instead!
    */
  def remoteMediatorsByString: Map[String, RemoteMediatorConfig] = remoteMediators.map {
    case (n, c) =>
      n.unwrap -> c
  }

  /** Use `remoteParticipants` instead!
    */
  def remoteParticipantsByString: Map[String, RemoteParticipantConfig] = remoteParticipants.map {
    case (n, c) =>
      n.unwrap -> c
  }

  /** dump config to string (without sensitive data) */
  /** renders the config as a string (used for dumping config for diagnostic purposes) */
  def dumpString: String = CantonConfig.makeConfidentialString(this)

  /** run a validation on the current config and return possible warning messages */
  def validate(edition: CantonEdition): Validated[NonEmpty[Seq[String]], Unit] = {
    val validator = edition match {
      case CommunityCantonEdition =>
        CommunityConfigValidations
      case EnterpriseCantonEdition =>
        EnterpriseConfigValidations
    }
    validator.validate(this, edition)
  }

  private lazy val participantNodeParameters_ : Map[InstanceName, ParticipantNodeParameters] =
    participants.fmap { participantConfig =>
      val participantParameters = participantConfig.parameters
      ParticipantNodeParameters(
        general = CantonNodeParameterConverter.general(this, participantConfig),
        adminWorkflow = participantParameters.adminWorkflow,
        maxUnzippedDarSize = participantParameters.maxUnzippedDarSize,
        stores = participantParameters.stores,
        reassignmentTimeProofFreshnessProportion =
          participantParameters.reassignmentTimeProofFreshnessProportion,
        protocolConfig = ParticipantProtocolConfig(
          minimumProtocolVersion = participantParameters.minimumProtocolVersion.map(_.unwrap),
          sessionSigningKeys = participantParameters.sessionSigningKeys,
          alphaVersionSupport = participantParameters.alphaVersionSupport,
          betaVersionSupport = participantParameters.betaVersionSupport,
          dontWarnOnDeprecatedPV = participantParameters.dontWarnOnDeprecatedPV,
        ),
        ledgerApiServerParameters = participantParameters.ledgerApiServer,
        engine = participantParameters.engine,
        journalGarbageCollectionDelay =
          participantParameters.journalGarbageCollectionDelay.toInternal,
        disableUpgradeValidation = participantParameters.disableUpgradeValidation,
        commandProgressTracking = participantParameters.commandProgressTracker,
        unsafeOnlinePartyReplication = participantParameters.unsafeOnlinePartyReplication,
      )
    }

  private[canton] def participantNodeParameters(
      participant: InstanceName
  ): ParticipantNodeParameters =
    nodeParametersFor(participantNodeParameters_, "participant", participant)

  /** Use `participantNodeParameters` instead!
    */
  private[canton] def participantNodeParametersByString(name: String) = participantNodeParameters(
    InstanceName.tryCreate(name)
  )

  private lazy val sequencerNodeParameters_ : Map[InstanceName, SequencerNodeParameters] =
    sequencers.fmap { sequencerNodeConfig =>
      SequencerNodeParameters(
        general = CantonNodeParameterConverter.general(this, sequencerNodeConfig),
        protocol = CantonNodeParameterConverter.protocol(this, sequencerNodeConfig.parameters),
        maxConfirmationRequestsBurstFactor =
          sequencerNodeConfig.parameters.maxConfirmationRequestsBurstFactor,
        unsafeEnableOnlinePartyReplication =
          sequencerNodeConfig.parameters.unsafeEnableOnlinePartyReplication,
      )
    }

  private[canton] def sequencerNodeParameters(name: InstanceName): SequencerNodeParameters =
    nodeParametersFor(sequencerNodeParameters_, "sequencer", name)

  private[canton] def sequencerNodeParametersByString(name: String): SequencerNodeParameters =
    sequencerNodeParameters(InstanceName.tryCreate(name))

  private lazy val mediatorNodeParameters_ : Map[InstanceName, MediatorNodeParameters] =
    mediators.fmap { mediatorNodeConfig =>
      MediatorNodeParameters(
        general = CantonNodeParameterConverter.general(this, mediatorNodeConfig),
        protocol = CantonNodeParameterConverter.protocol(this, mediatorNodeConfig.parameters),
      )
    }

  private[canton] def mediatorNodeParameters(name: InstanceName): MediatorNodeParameters =
    nodeParametersFor(mediatorNodeParameters_, "mediator", name)

  private[canton] def mediatorNodeParametersByString(name: String): MediatorNodeParameters =
    mediatorNodeParameters(InstanceName.tryCreate(name))

  protected def nodeParametersFor[A](
      cachedNodeParameters: Map[InstanceName, A],
      kind: String,
      name: InstanceName,
  ): A =
    cachedNodeParameters.getOrElse(
      name,
      throw new IllegalArgumentException(
        s"Unknown $kind $name. Known ${kind}s: ${cachedNodeParameters.keys.mkString(", ")}"
      ),
    )

  /** Produces a message in the structure
    * "da:admin-api=1,public-api=2;participant1:admin-api=3,ledger-api=4". Helpful for diagnosing
    * port already bound issues during tests. Allows any config value to be be null (can happen with
    * invalid configs or config stubbed in tests)
    */
  lazy val portDescription: String = mkPortDescription

  private def mkPortDescription: String = {
    def participant(config: ParticipantNodeConfig): Seq[String] =
      portDescriptionFromConfig(config)(Seq(("admin-api", _.adminApi), ("ledger-api", _.ledgerApi)))

    def sequencer(config: SequencerNodeConfig): Seq[String] =
      portDescriptionFromConfig(config)(Seq(("admin-api", _.adminApi), ("public-api", _.publicApi)))

    def mediator(config: MediatorNodeConfig): Seq[String] =
      portDescriptionFromConfig(config)(Seq(("admin-api", _.adminApi)))

    Seq(
      participants.fmap(participant),
      sequencers.fmap(sequencer),
      mediators.fmap(mediator),
    )
      .flatMap(_.map { case (name, ports) =>
        nodePortsDescription(name, ports)
      })
      .mkString(";")
  }

  protected def nodePortsDescription(
      nodeName: InstanceName,
      portDescriptions: Seq[String],
  ): String =
    s"$nodeName:${portDescriptions.mkString(",")}"

  protected def portDescriptionFromConfig[C](
      config: C
  )(apiNamesAndExtractors: Seq[(String, C => ServerConfig)]): Seq[String] = {
    def server(name: String, config: ServerConfig): Option[String] =
      Option(config).map(c => s"$name=${c.port}")
    Option(config)
      .map(config =>
        apiNamesAndExtractors.map { case (name, extractor) =>
          server(name, extractor(config))
        }
      )
      .getOrElse(Seq.empty)
      .flatMap(_.toList)
  }

  /** reduces the configuration into a single node configuration (used for external testing) */
  def asSingleNode(instanceName: InstanceName): CantonConfig =
    // based on the assumption that clashing instance names would clash in the console
    // environment, we can just filter.
    copy(
      participants = participants.filter(_._1 == instanceName),
      sequencers = sequencers.filter(_._1 == instanceName),
      mediators = mediators.filter(_._1 == instanceName),
    )

  override def withDefaults(
      defaults: DefaultPorts,
      edition: CantonEdition,
  ): CantonConfig = {
    def mapWithDefaults[K, V](m: Map[K, V with ConfigDefaults[DefaultPorts, V]]): Map[K, V] =
      m.fmap(_.withDefaults(defaults, edition))

    this
      .focus(_.participants)
      .modify(mapWithDefaults)
      .focus(_.sequencers)
      .modify(mapWithDefaults)
      .focus(_.mediators)
      .modify(mapWithDefaults)
  }

  def mergeDynamicChanges(newConfig: CantonConfig): CantonConfig =
    copy(participants =
      participants
        .map { case (name, config) =>
          (name, config, newConfig.participants.get(name))
        }
        .map {
          case (name, old, Some(newConfig)) =>
            (name, old.copy(alphaDynamic = newConfig.alphaDynamic))
          case (name, old, None) => (name, old)
        }
        .toMap
    )

}

private[canton] object CantonNodeParameterConverter {

  def general(parent: CantonConfig, node: LocalNodeConfig): CantonNodeParameters.General =
    CantonNodeParameters.General.Impl(
      tracing = parent.monitoring.tracing,
      delayLoggingThreshold = parent.monitoring.logging.delayLoggingThreshold.toInternal,
      loggingConfig = parent.monitoring.logging,
      enableAdditionalConsistencyChecks = parent.parameters.enableAdditionalConsistencyChecks,
      enablePreviewFeatures = parent.features.enablePreviewCommands,
      processingTimeouts = parent.parameters.timeouts.processing,
      sequencerClient = node.sequencerClient,
      cachingConfigs = node.parameters.caching,
      batchingConfig = node.parameters.batching,
      nonStandardConfig = parent.parameters.nonStandardConfig,
      dbMigrateAndStart = node.storage.parameters.migrateAndStart,
      exitOnFatalFailures = parent.parameters.exitOnFatalFailures,
      watchdog = node.parameters.watchdog,
      startupMemoryCheckConfig = parent.parameters.startupMemoryCheckConfig,
    )

  def protocol(parent: CantonConfig, config: ProtocolConfig): CantonNodeParameters.Protocol =
    CantonNodeParameters.Protocol.Impl(
      sessionSigningKeys = config.sessionSigningKeys,
      alphaVersionSupport = parent.parameters.alphaVersionSupport || config.alphaVersionSupport,
      betaVersionSupport = parent.parameters.betaVersionSupport || config.betaVersionSupport,
      dontWarnOnDeprecatedPV = config.dontWarnOnDeprecatedPV,
    )

}

object CantonConfig {
  implicit val cantonConfigCantonConfigValidator: CantonConfigValidator[CantonConfig] =
    CantonConfigValidatorDerivation[CantonConfig]

  // the great ux of pureconfig expects you to provide this ProductHint such that the created derivedReader fails on
  // unknown keys
  implicit def preventAllUnknownKeys[T]: ProductHint[T] = ProductHint[T](allowUnknownKeys = false)

  import com.daml.nonempty.NonEmptyUtil.instances.*
  import pureconfig.ConfigReader
  import pureconfig.generic.semiauto.*
  import pureconfig.module.cats.*

  implicit val storageConfigTypeHint: FieldCoproductHint[StorageConfig] =
    CantonConfigUtil.lowerCaseStorageConfigType[StorageConfig]

  /** In the external config we use `port` for an optionally set port, while internally we store it
    * as `internalPort`
    */
  implicit def serverConfigProductHint[SC <: ServerConfig]: ProductHint[SC] = ProductHint[SC](
    fieldMapping = ConfigFieldMapping(CamelCase, KebabCase).withOverrides("internalPort" -> "port"),
    allowUnknownKeys = false,
  )

  object ConfigReaders {
    import CantonConfigUtil.*
    import BaseCantonConfig.Readers.*

    implicit val nonNegativeDurationReader: ConfigReader[NonNegativeDuration] =
      ConfigReader.fromString[NonNegativeDuration] { str =>
        def err(message: String) =
          CannotConvert(str, NonNegativeDuration.getClass.getName, message)

        Either
          .catchOnly[NumberFormatException](Duration.apply(str))
          .leftMap(error => err(error.getMessage))
          .flatMap(duration => NonNegativeDuration.fromDuration(duration).leftMap(err))
      }

    implicit val positiveDurationSecondsReader: ConfigReader[PositiveDurationSeconds] =
      ConfigReader.fromString[PositiveDurationSeconds] { str =>
        def err(message: String) =
          CannotConvert(str, PositiveDurationSeconds.getClass.getName, message)

        Either
          .catchOnly[NumberFormatException](Duration.apply(str))
          .leftMap(error => err(error.getMessage))
          .flatMap(duration => PositiveDurationSeconds.fromDuration(duration).leftMap(err))
      }

    object Crypto {

      lazy implicit final val cryptoSigningAlgorithmSpecReader: ConfigReader[SigningAlgorithmSpec] =
        deriveEnumerationReader[SigningAlgorithmSpec]
      lazy implicit final val signingKeySpecReader: ConfigReader[SigningKeySpec] =
        deriveEnumerationReader[SigningKeySpec]
      lazy implicit final val cryptoEncryptionAlgorithmSpecReader
          : ConfigReader[EncryptionAlgorithmSpec] =
        deriveEnumerationReader[EncryptionAlgorithmSpec]
      lazy implicit final val encryptionKeySpecReader: ConfigReader[EncryptionKeySpec] =
        deriveEnumerationReader[EncryptionKeySpec]
      lazy implicit final val cryptoSymmetricKeySchemeReader: ConfigReader[SymmetricKeyScheme] =
        deriveEnumerationReader[SymmetricKeyScheme]
      lazy implicit final val cryptoHashAlgorithmReader: ConfigReader[HashAlgorithm] =
        deriveEnumerationReader[HashAlgorithm]
      lazy implicit final val cryptoPbkdfSchemeReader: ConfigReader[PbkdfScheme] =
        deriveEnumerationReader[PbkdfScheme]
      lazy implicit final val cryptoKeyFormatReader: ConfigReader[CryptoKeyFormat] =
        deriveEnumerationReader[CryptoKeyFormat]: @nowarn(
          "msg=Der in object CryptoKeyFormat is deprecated"
        )
      lazy implicit final val signingSchemeConfigReader: ConfigReader[SigningSchemeConfig] =
        deriveReader[SigningSchemeConfig]
      lazy implicit final val encryptionSchemeConfigReader: ConfigReader[EncryptionSchemeConfig] =
        deriveReader[EncryptionSchemeConfig]

      implicit def cryptoSchemeConfig[S: ConfigReader]: ConfigReader[CryptoSchemeConfig[S]] =
        deriveReader[CryptoSchemeConfig[S]]

      lazy implicit final val cryptoProviderReader: ConfigReader[CryptoProvider] =
        deriveEnumerationReader[CryptoProvider]

      implicit val kmsBackoffConfigReader: ConfigReader[KmsConfig.ExponentialBackoffConfig] =
        deriveReader[KmsConfig.ExponentialBackoffConfig]
      implicit val kmsRetryConfigReader: ConfigReader[KmsConfig.RetryConfig] =
        deriveReader[KmsConfig.RetryConfig]

      implicit val kmsReader: ConfigReader[EncryptedPrivateStoreConfig.Kms] =
        deriveReader[EncryptedPrivateStoreConfig.Kms]
      implicit val encryptedPrivateStoreConfigReader: ConfigReader[EncryptedPrivateStoreConfig] =
        deriveReader[EncryptedPrivateStoreConfig]
      implicit val privateKeyStoreConfigReader: ConfigReader[PrivateKeyStoreConfig] =
        deriveReader[PrivateKeyStoreConfig]

      implicit val gcpKmsConfigReader: ConfigReader[KmsConfig.Gcp] =
        deriveReader[KmsConfig.Gcp]
      implicit val awsKmsConfigReader: ConfigReader[KmsConfig.Aws] =
        deriveReader[KmsConfig.Aws]
      implicit val driverKmsConfigReader: ConfigReader[KmsConfig.Driver] =
        deriveReader[KmsConfig.Driver]
      implicit val kmsConfigReader: ConfigReader[KmsConfig] =
        deriveReader[KmsConfig]
      implicit val cryptoReader: ConfigReader[CryptoConfig] =
        deriveReader[CryptoConfig]
    }

    lazy implicit final val sequencerTestingInterceptorReader
        : ConfigReader[DatabaseSequencerConfig.TestingInterceptor] =
      (_: ConfigCursor) =>
        sys.error("Sequencer testing interceptor cannot be created from pureconfig")

    lazy implicit final val keepAliveClientConfigReader: ConfigReader[KeepAliveClientConfig] =
      deriveReader[KeepAliveClientConfig]
    lazy implicit final val keepAliveServerConfigReader: ConfigReader[BasicKeepAliveServerConfig] =
      deriveReader[BasicKeepAliveServerConfig]
    lazy implicit final val tlsClientCertificateReader: ConfigReader[TlsClientCertificate] =
      deriveReader[TlsClientCertificate]
    lazy implicit final val tlsServerConfigReader: ConfigReader[TlsServerConfig] = {
      implicit val serverAuthRequirementConfigNoneReader
          : ConfigReader[ServerAuthRequirementConfig.None.type] =
        deriveReader[ServerAuthRequirementConfig.None.type]
      implicit val serverAuthRequirementConfigOptionalReader
          : ConfigReader[ServerAuthRequirementConfig.Optional.type] =
        deriveReader[ServerAuthRequirementConfig.Optional.type]
      implicit val serverAuthRequirementConfigRequireReader
          : ConfigReader[ServerAuthRequirementConfig.Require] =
        deriveReader[ServerAuthRequirementConfig.Require]
      implicit val serverAuthRequirementConfigReader: ConfigReader[ServerAuthRequirementConfig] =
        deriveReader[ServerAuthRequirementConfig]
      deriveReader[TlsServerConfig]
    }

    implicit val identityConfigReaderAuto: ConfigReader[IdentityConfig.Auto] = {
      implicit val nodeNameReader: ConfigReader[NodeIdentifierConfig] = {
        implicit val nodeNameConfigReader: ConfigReader[NodeIdentifierConfig.Config.type] =
          deriveReader[NodeIdentifierConfig.Config.type]
        implicit val nodeNameRandomReader: ConfigReader[NodeIdentifierConfig.Random.type] =
          deriveReader[NodeIdentifierConfig.Random.type]
        implicit val nodeNameExplicitReader: ConfigReader[NodeIdentifierConfig.Explicit] =
          deriveReader[NodeIdentifierConfig.Explicit]
        deriveReader[NodeIdentifierConfig]
      }
      deriveReader[IdentityConfig.Auto]
    }
    implicit val identityConfigReaderManual: ConfigReader[IdentityConfig.Manual.type] =
      deriveReader[IdentityConfig.Manual.type]
    implicit val identityConfigReaderExternal: ConfigReader[IdentityConfig.External] =
      deriveReader[IdentityConfig.External]
    implicit val initBaseIdentityConfigReader: ConfigReader[IdentityConfig] =
      deriveReader[IdentityConfig]
    lazy implicit final val initConfigReader: ConfigReader[InitConfig] =
      deriveReader[InitConfig]
    lazy implicit final val participantInitConfigReader: ConfigReader[ParticipantInitConfig] = {
      implicit val ledgerApiParticipantInitConfigReader
          : ConfigReader[ParticipantLedgerApiInitConfig] =
        deriveReader[ParticipantLedgerApiInitConfig]
      deriveReader[ParticipantInitConfig]
    }

    lazy implicit final val pemFileReader: ConfigReader[PemFile] = new ConfigReader[PemFile] {
      override def from(cur: ConfigCursor): Result[PemFile] = cur.asString.flatMap { s =>
        ExistingFile.create(new File(s)) match {
          case Right(file) => Right(PemFile(file))
          case Left(error) =>
            val failureReason = CannotConvert(s, PemFile.getClass.getName, error.message)
            Left(ConfigReaderFailures(ConvertFailure(failureReason, cur)))
        }
      }
    }

    lazy implicit final val pemFileOrStringReader: ConfigReader[PemFileOrString] =
      new ConfigReader[PemFileOrString] {
        override def from(cur: ConfigCursor): Result[PemFileOrString] = cur.asString.flatMap { s =>
          if (s.contains("-----BEGIN")) {
            // assume it's a PEM string
            Right(PemString(s))
          } else {
            // assume it's a file path
            pemFileReader.from(cur)
          }
        }
      }

    implicit val tlsClientConfigReader: ConfigReader[TlsClientConfig] =
      deriveReader[TlsClientConfig]
    lazy implicit final val fullClientConfigReader: ConfigReader[FullClientConfig] =
      deriveReader[FullClientConfig]

    lazy implicit final val remoteParticipantConfigReader: ConfigReader[RemoteParticipantConfig] =
      deriveReader[RemoteParticipantConfig]
    lazy implicit final val sequencerApiclientConfigReader
        : ConfigReader[SequencerApiClientConfig] = {
      implicit val tlsClientConfigOnlyTrustFileReader: ConfigReader[TlsClientConfigOnlyTrustFile] =
        deriveReader[TlsClientConfigOnlyTrustFile]
      deriveReader[SequencerApiClientConfig]
    }

    lazy implicit final val nodeMonitoringConfigReader: ConfigReader[NodeMonitoringConfig] = {
      implicit val httpHealthServerConfigReader: ConfigReader[HttpHealthServerConfig] =
        deriveReader[HttpHealthServerConfig]
      implicit val grpcHealthServerConfigReader: ConfigReader[GrpcHealthServerConfig] =
        deriveReader[GrpcHealthServerConfig]
      deriveReader[NodeMonitoringConfig]
    }

    lazy implicit final val testingTimeServiceConfigReader
        : ConfigReader[TestingTimeServiceConfig] = {
      implicit val monotonicTimeReader: ConfigReader[TestingTimeServiceConfig.MonotonicTime.type] =
        deriveReader[TestingTimeServiceConfig.MonotonicTime.type]
      deriveReader[TestingTimeServiceConfig]
    }

    lazy implicit final val adminServerReader: ConfigReader[AdminServerConfig] =
      deriveReader[AdminServerConfig]
    lazy implicit final val tlsBaseServerConfigReader: ConfigReader[TlsBaseServerConfig] =
      deriveReader[TlsBaseServerConfig]

    lazy implicit final val clockConfigReader: ConfigReader[ClockConfig] = {
      implicit val clockConfigRemoteClockReader: ConfigReader[ClockConfig.RemoteClock] =
        deriveReader[ClockConfig.RemoteClock]
      implicit val clockConfigWallClockReader: ConfigReader[ClockConfig.WallClock] =
        deriveReader[ClockConfig.WallClock]
      implicit val clockConfigSimClockReader: ConfigReader[ClockConfig.SimClock.type] =
        deriveReader[ClockConfig.SimClock.type]
      deriveReader[ClockConfig]
    }
    lazy implicit final val jwtTimestampLeewayConfigReader: ConfigReader[JwtTimestampLeeway] =
      deriveReader[JwtTimestampLeeway]

    lazy implicit final val authServiceConfigReader: ConfigReader[AuthServiceConfig] = {
      implicit val authorizedUserReader: ConfigReader[AuthorizedUser] =
        deriveReader[AuthorizedUser]
      implicit val authServiceAccessLevelReader: ConfigReader[AccessLevel] =
        deriveEnumerationReader[AccessLevel]
      implicit val authServiceConfigUnsafeJwtHmac256Reader
          : ConfigReader[AuthServiceConfig.UnsafeJwtHmac256] =
        deriveReader[AuthServiceConfig.UnsafeJwtHmac256]
      implicit val authServiceConfigJwtEs256CrtReader: ConfigReader[AuthServiceConfig.JwtEs256Crt] =
        deriveReader[AuthServiceConfig.JwtEs256Crt]
      implicit val authServiceConfigJwtEs512CrtReader: ConfigReader[AuthServiceConfig.JwtEs512Crt] =
        deriveReader[AuthServiceConfig.JwtEs512Crt]
      implicit val authServiceConfigJwtRs256CrtReader: ConfigReader[AuthServiceConfig.JwtRs256Crt] =
        deriveReader[AuthServiceConfig.JwtRs256Crt]
      implicit val authServiceConfigJwtJwksReader: ConfigReader[AuthServiceConfig.JwtJwks] =
        deriveReader[AuthServiceConfig.JwtJwks]
      implicit val authServiceConfigWildcardReader: ConfigReader[AuthServiceConfig.Wildcard.type] =
        deriveReader[AuthServiceConfig.Wildcard.type]
      deriveReader[AuthServiceConfig]
    }
    lazy implicit final val rateLimitConfigReader: ConfigReader[RateLimitingConfig] =
      deriveReader[RateLimitingConfig]

    lazy implicit final val ledgerApiServerConfigReader: ConfigReader[LedgerApiServerConfig] = {
      implicit val lapiKeepAliveServerConfigReader: ConfigReader[LedgerApiKeepAliveServerConfig] =
        deriveReader[LedgerApiKeepAliveServerConfig]
      implicit val ledgerApiInteractiveSubmissionServiceConfigReader
          : ConfigReader[InteractiveSubmissionServiceConfig] =
        deriveReader[InteractiveSubmissionServiceConfig]

      implicit val ledgerApiTopologyAwarePackageSelectionConfigReader
          : ConfigReader[TopologyAwarePackageSelectionConfig] =
        deriveReader[TopologyAwarePackageSelectionConfig]

      deriveReader[LedgerApiServerConfig]
    }

    lazy implicit final val httpApiServerConfigReader: ConfigReader[JsonApiConfig] = {
      implicit val throttleModeCfgReader: ConfigReader[ThrottleMode] =
        ConfigReader.fromString[ThrottleMode](catchConvertError { s =>
          s.toLowerCase() match {
            case "enforcing" => Right(ThrottleMode.Enforcing)
            case "shaping" => Right(ThrottleMode.Shaping)
            case _ => Left("not one of 'shaping' or 'enforcing'")
          }
        })
      implicit val wsConfigReader: ConfigReader[WebsocketConfig] =
        deriveReader[WebsocketConfig]
      implicit val httpServerConfigReader: ConfigReader[HttpServerConfig] =
        deriveReader[HttpServerConfig]
      deriveReader[JsonApiConfig]
    }

    lazy implicit final val topologyConfigReader: ConfigReader[TopologyConfig] =
      deriveReader[TopologyConfig]

    lazy implicit val databaseSequencerExclusiveStorageConfigReader
        : ConfigReader[DatabaseSequencerExclusiveStorageConfig] =
      deriveReader[DatabaseSequencerExclusiveStorageConfig]
    lazy implicit val sequencerHighAvailabilityConfigReader
        : ConfigReader[SequencerHighAvailabilityConfig] =
      deriveReader[SequencerHighAvailabilityConfig]
    lazy implicit final val sequencerConfigDatabaseReader: ConfigReader[SequencerConfig.Database] =
      deriveReader[SequencerConfig.Database]
    lazy implicit final val blockSequencerConfigReader: ConfigReader[BlockSequencerConfig] =
      deriveReader[BlockSequencerConfig]
    lazy implicit final val sequencerWriterCommitModeConfigReader: ConfigReader[CommitMode] =
      deriveEnumerationReader[CommitMode]
    lazy implicit final val sequencerWriterConfigReader: ConfigReader[SequencerWriterConfig] =
      deriveReader[SequencerWriterConfig]
    lazy implicit final val bytesUnitReader: ConfigReader[BytesUnit] =
      BasicReaders.configMemorySizeReader.map(cms => BytesUnit(cms.toBytes))
    lazy implicit final val sequencerWriterConfigHighThroughputReader
        : ConfigReader[SequencerWriterConfig.HighThroughput] =
      deriveReader[SequencerWriterConfig.HighThroughput]
    lazy implicit final val sequencerWriterConfigLowLatencyReader
        : ConfigReader[SequencerWriterConfig.LowLatency] =
      deriveReader[SequencerWriterConfig.LowLatency]

    lazy implicit val memoryReader: ConfigReader[StorageConfig.Memory] =
      deriveReader[StorageConfig.Memory]
    lazy implicit val h2Reader: ConfigReader[DbConfig.H2] =
      deriveReader[DbConfig.H2]
    lazy implicit val postgresReader: ConfigReader[DbConfig.Postgres] =
      deriveReader[DbConfig.Postgres]
    lazy implicit val dbConfigReader: ConfigReader[DbConfig] =
      deriveReader[DbConfig]
    lazy implicit val storageConfigReader: ConfigReader[StorageConfig] =
      deriveReader[StorageConfig]
    lazy implicit val dbLockConfigReader: ConfigReader[DbLockConfig] = deriveReader[DbLockConfig]
    lazy implicit val lockedConnectionConfigReader: ConfigReader[DbLockedConnectionConfig] =
      deriveReader[DbLockedConnectionConfig]
    lazy implicit val connectionPoolConfigReader: ConfigReader[DbLockedConnectionPoolConfig] =
      deriveReader[DbLockedConnectionPoolConfig]

    lazy implicit val bftBlockOrdererP2PServerConfigReader
        : ConfigReader[BftBlockOrdererConfig.P2PServerConfig] =
      deriveReader[BftBlockOrdererConfig.P2PServerConfig]
    lazy implicit val bftBlockOrdererP2PEndpointConfigReader
        : ConfigReader[BftBlockOrdererConfig.P2PEndpointConfig] =
      deriveReader[BftBlockOrdererConfig.P2PEndpointConfig]
    lazy implicit val bftBlockOrdererP2PNetworkAuthenticationConfigReader
        : ConfigReader[BftBlockOrdererConfig.P2PNetworkAuthenticationConfig] =
      deriveReader[BftBlockOrdererConfig.P2PNetworkAuthenticationConfig]
    lazy implicit val bftBlockOrdererP2PNetworkConfigReader
        : ConfigReader[BftBlockOrdererConfig.P2PNetworkConfig] =
      deriveReader[BftBlockOrdererConfig.P2PNetworkConfig]
    lazy implicit val bftBlockOrdererPruningConfigReader
        : ConfigReader[BftBlockOrdererConfig.PruningConfig] =
      deriveReader[BftBlockOrdererConfig.PruningConfig]
    lazy implicit val bftBlockOrdererConfigReader: ConfigReader[BftBlockOrdererConfig] =
      deriveReader[BftBlockOrdererConfig]
    lazy implicit val sequencerConfigBftSequencerReader
        : ConfigReader[SequencerConfig.BftSequencer] =
      deriveReader[SequencerConfig.BftSequencer]

    lazy implicit final val sequencerPruningConfig
        : ConfigReader[DatabaseSequencerConfig.SequencerPruningConfig] =
      deriveReader[DatabaseSequencerConfig.SequencerPruningConfig]

    lazy implicit val sequencerReaderConfigReader: ConfigReader[SequencerReaderConfig] =
      deriveReader[SequencerReaderConfig]

    lazy implicit final val sequencerConfigReader: ConfigReader[SequencerConfig] =
      ConfigReader.fromCursor[SequencerConfig] { cur =>
        for {
          objCur <- cur.asObjectCursor
          sequencerType <- objCur.atKey("type").flatMap(_.asString)
          config <- (sequencerType match {
            case "database" =>
              sequencerConfigDatabaseReader.from(objCur.withoutKey("type"))
            case BftSequencerFactory.ShortName =>
              sequencerConfigBftSequencerReader.from(objCur.withoutKey("type"))
            case other =>
              for {
                config <- objCur.atKey("config")
                // since the `database` subsection is optional, we try to get the
                // config value, and if it doesn't exist parse the default
                // Database sequencer config with an empty object
                database = objCur
                  .atKeyOrUndefined("block")
                  .valueOpt
                  .getOrElse(ConfigValueFactory.fromMap(new java.util.HashMap()))
                blockSequencerConfig <- blockSequencerConfigReader.from(database)
              } yield SequencerConfig.External(other, blockSequencerConfig, config)
          }): ConfigReader.Result[SequencerConfig]
        } yield config
      }

    lazy implicit final val sequencerNodeParametersConfigReader
        : ConfigReader[SequencerNodeParameterConfig] =
      deriveReader[SequencerNodeParameterConfig]
    lazy implicit final val SequencerHealthConfigReader: ConfigReader[SequencerHealthConfig] =
      deriveReader[SequencerHealthConfig]

    lazy implicit final val remoteSequencerConfigReader: ConfigReader[RemoteSequencerConfig] =
      deriveReader[RemoteSequencerConfig]
    lazy implicit final val mediatorNodeParameterConfigReader
        : ConfigReader[MediatorNodeParameterConfig] =
      deriveReader[MediatorNodeParameterConfig]

    lazy implicit final val mediatorConfigReader: ConfigReader[MediatorConfig] = {
      implicit val mediatorPruningConfigReader: ConfigReader[MediatorPruningConfig] =
        deriveReader[MediatorPruningConfig]
      deriveReader[MediatorConfig]
    }
    lazy implicit final val remoteMediatorConfigReader: ConfigReader[RemoteMediatorConfig] =
      deriveReader[RemoteMediatorConfig]

    lazy implicit final val sequencerTrafficConfigReader: ConfigReader[SequencerTrafficConfig] =
      deriveReader[SequencerTrafficConfig]

    lazy implicit final val monitoringConfigReader: ConfigReader[MonitoringConfig] = {
      implicit val tracingConfigDisabledSpanExporterReader
          : ConfigReader[TracingConfig.Exporter.Disabled.type] =
        deriveReader[TracingConfig.Exporter.Disabled.type]
      implicit val tracingConfigZipkinSpanExporterReader
          : ConfigReader[TracingConfig.Exporter.Zipkin] =
        deriveReader[TracingConfig.Exporter.Zipkin]
      implicit val tracingConfigOtlpSpanExporterReader: ConfigReader[TracingConfig.Exporter.Otlp] =
        deriveReader[TracingConfig.Exporter.Otlp]
      implicit val tracingConfigSpanExporterReader: ConfigReader[TracingConfig.Exporter] =
        deriveReader[TracingConfig.Exporter]
      implicit val tracingConfigAlwaysOnSamplerReader
          : ConfigReader[TracingConfig.Sampler.AlwaysOn] =
        deriveReader[TracingConfig.Sampler.AlwaysOn]
      implicit val tracingConfigAlwaysOffSamplerReader
          : ConfigReader[TracingConfig.Sampler.AlwaysOff] =
        deriveReader[TracingConfig.Sampler.AlwaysOff]
      implicit val tracingConfigTraceIdRatioSamplerReader
          : ConfigReader[TracingConfig.Sampler.TraceIdRatio] =
        deriveReader[TracingConfig.Sampler.TraceIdRatio]
      implicit val tracingConfigSamplerReader: ConfigReader[TracingConfig.Sampler] =
        deriveReader[TracingConfig.Sampler]
      implicit val tracingConfigBatchSpanProcessorReader
          : ConfigReader[TracingConfig.BatchSpanProcessor] =
        deriveReader[TracingConfig.BatchSpanProcessor]
      implicit val tracingConfigTracerReader: ConfigReader[TracingConfig.Tracer] =
        deriveReader[TracingConfig.Tracer]
      // treat TracingConfig.Propagation as an enum as we currently only have case object types in the sealed family
      implicit val tracingConfigPropagationReader: ConfigReader[TracingConfig.Propagation] =
        deriveEnumerationReader[TracingConfig.Propagation]
      implicit val tracingConfigReader: ConfigReader[TracingConfig] =
        deriveReader[TracingConfig]
      implicit val deadlockDetectionConfigReader: ConfigReader[DeadlockDetectionConfig] =
        deriveReader[DeadlockDetectionConfig]
      implicit val metricsFilterConfigReader: ConfigReader[MetricsFilterConfig] =
        deriveReader[MetricsFilterConfig]
      implicit val metricsConfigPrometheusReader: ConfigReader[MetricsReporterConfig.Prometheus] =
        deriveReader[MetricsReporterConfig.Prometheus]
      implicit val metricsConfigCsvReader: ConfigReader[MetricsReporterConfig.Csv] =
        deriveReader[MetricsReporterConfig.Csv]
      implicit val metricsConfigLoggingReader: ConfigReader[MetricsReporterConfig.Logging] =
        deriveReader[MetricsReporterConfig.Logging]
      implicit val metricsConfigJvmConfigReader: ConfigReader[MetricsConfig.JvmMetrics] =
        deriveReader[MetricsConfig.JvmMetrics]
      implicit val metricsReporterConfigReader: ConfigReader[MetricsReporterConfig] =
        deriveReader[MetricsReporterConfig]
      implicit val histogramExponentialConfigReader: ConfigReader[HistogramDefinition.Exponential] =
        deriveReader[HistogramDefinition.Exponential]
      implicit val histogramBucketConfigReader: ConfigReader[HistogramDefinition.Buckets] =
        deriveReader[HistogramDefinition.Buckets]
      implicit val histogramAggregationTypeConfigReader
          : ConfigReader[HistogramDefinition.AggregationType] =
        deriveReader[HistogramDefinition.AggregationType]
      implicit val histogramDefinitionConfigReader: ConfigReader[HistogramDefinition] =
        deriveReader[HistogramDefinition]
      implicit val metricQualificationConfigReader: ConfigReader[MetricQualification] =
        ConfigReader.fromString[MetricQualification](catchConvertError { s =>
          s.toLowerCase() match {
            case "debug" => Right(MetricQualification.Debug)
            case "errors" => Right(MetricQualification.Errors)
            case "saturation" => Right(MetricQualification.Saturation)
            case "traffic" => Right(MetricQualification.Traffic)
            case "latency" => Right(MetricQualification.Latency)
            case _ => Left("not one of 'errors', 'saturation', 'traffic', 'latency', 'debug'")
          }
        })
      implicit val metricsConfigReader: ConfigReader[MetricsConfig] =
        deriveReader[MetricsConfig]

      implicit val loggingConfigReader: ConfigReader[LoggingConfig] = {
        implicit val apiLoggingConfigReader: ConfigReader[ApiLoggingConfig] =
          deriveReader[ApiLoggingConfig]
        implicit val gcLoggingConfigReader: ConfigReader[GCLoggingConfig] =
          deriveReader[GCLoggingConfig]
        deriveReader[LoggingConfig]
      }
      deriveReader[MonitoringConfig]
    }

    import Crypto.*
    lazy implicit final val sessionSigningKeysConfigReader: ConfigReader[SessionSigningKeysConfig] =
      deriveReader[SessionSigningKeysConfig]

    lazy implicit final val cachingConfigsReader: ConfigReader[CachingConfigs] = {
      implicit val cacheConfigWithTimeoutReader: ConfigReader[CacheConfigWithTimeout] =
        deriveReader[CacheConfigWithTimeout]
      implicit val cacheConfigWithMemoryBoundsReader: ConfigReader[CacheConfigWithMemoryBounds] =
        deriveReader[CacheConfigWithMemoryBounds]

      implicit val sessionEncryptionKeyCacheConfigReader
          : ConfigReader[SessionEncryptionKeyCacheConfig] =
        deriveReader[SessionEncryptionKeyCacheConfig]
      implicit val cacheConfigReader: ConfigReader[CacheConfig] =
        deriveReader[CacheConfig]
      deriveReader[CachingConfigs]
    }

    lazy implicit final val ledgerApiServerParametersConfigReader
        : ConfigReader[LedgerApiServerParametersConfig] = {
      implicit val ledgerApiContractLoaderConfigReader: ConfigReader[ContractLoaderConfig] =
        deriveReader[ContractLoaderConfig]
      implicit val contractIdSeedingReader: ConfigReader[Seeding] =
        // Not using deriveEnumerationReader[Seeding] as we prefer "testing-static" over static (that appears
        // in Seeding.name, but not in the case object name). This makes it clear that static is not to
        // be used in production and avoids naming the configuration option contractIdSeedingOverrideOnlyForTesting or so.
        ConfigReader.fromString[Seeding] {
          case Seeding.Strong.name => Right(Seeding.Strong)
          case Seeding.Weak.name =>
            Right(
              Seeding.Weak
            ) // Pending upstream discussions, weak may turn out to be viable too for production
          case Seeding.Static.name => Right(Seeding.Static)
          case unknownSeeding =>
            Left(
              CannotConvert(
                unknownSeeding,
                Seeding.getClass.getName,
                s"Seeding is neither ${Seeding.Strong.name}, ${Seeding.Weak.name}, nor ${Seeding.Static.name}: $unknownSeeding",
              )
            )
        }

      deriveReader[LedgerApiServerParametersConfig]
    }

    lazy implicit final val participantNodeParameterConfigReader
        : ConfigReader[ParticipantNodeParameterConfig] = {
      implicit val cantonEngineConfigReader: ConfigReader[CantonEngineConfig] = {
        implicit val engineLoggingConfigReader: ConfigReader[EngineLoggingConfig] =
          deriveReader[EngineLoggingConfig]
        deriveReader[CantonEngineConfig]
      }
      implicit val participantStoreConfigReader: ConfigReader[ParticipantStoreConfig] = {
        implicit val journalPruningConfigReader: ConfigReader[JournalPruningConfig] =
          deriveReader[JournalPruningConfig]
        deriveReader[ParticipantStoreConfig]
      }
      implicit val adminWorkflowConfigReader: ConfigReader[AdminWorkflowConfig] =
        deriveReader[AdminWorkflowConfig]
      implicit val commandProgressTrackerConfigReader: ConfigReader[CommandProgressTrackerConfig] =
        deriveReader[CommandProgressTrackerConfig]
      implicit val packageMetadataViewConfigReader: ConfigReader[PackageMetadataViewConfig] =
        deriveReader[PackageMetadataViewConfig]
      implicit val unsafeOnlinePartyReplicationConfig
          : ConfigReader[UnsafeOnlinePartyReplicationConfig] =
        deriveReader[UnsafeOnlinePartyReplicationConfig]
      deriveReader[ParticipantNodeParameterConfig]
    }
    lazy implicit final val timeTrackerConfigReader: ConfigReader[SynchronizerTimeTrackerConfig] = {
      implicit val timeRequestConfigReader: ConfigReader[TimeProofRequestConfig] =
        deriveReader[TimeProofRequestConfig]
      deriveReader[SynchronizerTimeTrackerConfig]
    }

    lazy implicit val authTokenManagerConfigReader: ConfigReader[AuthenticationTokenManagerConfig] =
      deriveReader[AuthenticationTokenManagerConfig]
    lazy implicit final val sequencerClientConfigReader: ConfigReader[SequencerClientConfig] =
      deriveReader[SequencerClientConfig]

    lazy implicit final val cantonParametersReader: ConfigReader[CantonParameters] = {
      implicit val ammoniteConfigReader: ConfigReader[AmmoniteConsoleConfig] =
        deriveReader[AmmoniteConsoleConfig]
      implicit val retentionPeriodDefaultsConfigReader: ConfigReader[RetentionPeriodDefaults] =
        deriveReader[RetentionPeriodDefaults]
      implicit val timeoutSettingsReader: ConfigReader[TimeoutSettings] = {
        implicit val consoleCommandTimeoutReader: ConfigReader[ConsoleCommandTimeout] =
          deriveReader[ConsoleCommandTimeout]
        implicit val processingTimeoutReader: ConfigReader[ProcessingTimeout] =
          deriveReader[ProcessingTimeout]
        deriveReader[TimeoutSettings]
      }
      deriveReader[CantonParameters]
    }
    lazy implicit final val cantonFeaturesReader: ConfigReader[CantonFeatures] =
      deriveReader[CantonFeatures]
    lazy implicit final val cantonWatchdogConfigReader: ConfigReader[WatchdogConfig] =
      deriveReader[WatchdogConfig]

    lazy implicit final val reportingLevelReader
        : ConfigReader[StartupMemoryCheckConfig.ReportingLevel] =
      deriveEnumerationReader[StartupMemoryCheckConfig.ReportingLevel]

    lazy implicit final val startupMemoryCheckConfigReader: ConfigReader[StartupMemoryCheckConfig] =
      deriveReader[StartupMemoryCheckConfig]

    implicit val participantReplicationConfigReader: ConfigReader[ReplicationConfig] =
      deriveReader[ReplicationConfig]
    implicit val participantEnterpriseFeaturesConfigReader
        : ConfigReader[EnterpriseParticipantFeaturesConfig] =
      deriveReader[EnterpriseParticipantFeaturesConfig]

    implicit val localParticipantConfigReader: ConfigReader[ParticipantNodeConfig] = {
      import DeclarativeParticipantConfig.Readers.*
      deriveReader[ParticipantNodeConfig]
    }
    implicit val mediatorNodeConfigReader: ConfigReader[MediatorNodeConfig] =
      deriveReader[MediatorNodeConfig]

    implicit val publicServerConfigReader: ConfigReader[PublicServerConfig] =
      deriveReader[PublicServerConfig]
    implicit val sequencerNodeConfigReader: ConfigReader[SequencerNodeConfig] =
      deriveReader[SequencerNodeConfig]
  }

  private implicit lazy val cantonConfigReader: ConfigReader[CantonConfig] = {
    // memoize it so we get the same instance every time
    import ConfigReaders.*

    deriveReader[CantonConfig]
  }

  /** writers
    * @param confidential
    *   if set to true, confidential data which should not be shared for support purposes is blinded
    */
  class ConfigWriters(confidential: Boolean) {
    import BaseCantonConfig.Writers.*
    val confidentialWriter = new ConfidentialConfigWriter(confidential)

    implicit val nonNegativeDurationWriter: ConfigWriter[NonNegativeDuration] =
      ConfigWriter.toString { x =>
        x.unwrap match {
          case Duration.Inf => "Inf"
          case y => y.toString
        }
      }
    implicit val positiveDurationSecondsWriter: ConfigWriter[PositiveDurationSeconds] =
      ConfigWriter.toString(_.underlying.toString)

    implicit val lengthLimitedStringWriter: ConfigWriter[LengthLimitedString] =
      ConfigWriter.toString(_.unwrap)
    implicit val nonNegativeIntWriter: ConfigWriter[NonNegativeInt] =
      ConfigWriter.toString(x => x.unwrap.toString)

    implicit val existingFileWriter: ConfigWriter[ExistingFile] =
      ConfigWriter.toString(x => x.unwrap.toString)
    implicit val nonEmptyStringWriter: ConfigWriter[NonEmptyString] =
      ConfigWriter.toString(x => x.unwrap)

    object Crypto {
      lazy implicit final val cryptoSigningAlgorithmSpecWriter: ConfigWriter[SigningAlgorithmSpec] =
        deriveEnumerationWriter[SigningAlgorithmSpec]
      lazy implicit final val signingKeySpecWriter: ConfigWriter[SigningKeySpec] =
        deriveEnumerationWriter[SigningKeySpec]
      lazy implicit final val cryptoEncryptionAlgorithmSpecWriter
          : ConfigWriter[EncryptionAlgorithmSpec] =
        deriveEnumerationWriter[EncryptionAlgorithmSpec]
      lazy implicit final val encryptionKeySpecWriter: ConfigWriter[EncryptionKeySpec] =
        deriveEnumerationWriter[EncryptionKeySpec]
      lazy implicit final val cryptoSymmetricKeySchemeWriter: ConfigWriter[SymmetricKeyScheme] =
        deriveEnumerationWriter[SymmetricKeyScheme]
      lazy implicit final val cryptoHashAlgorithmWriter: ConfigWriter[HashAlgorithm] =
        deriveEnumerationWriter[HashAlgorithm]
      lazy implicit final val cryptoPbkdfSchemeWriter: ConfigWriter[PbkdfScheme] =
        deriveEnumerationWriter[PbkdfScheme]
      lazy implicit final val cryptoKeyFormatWriter: ConfigWriter[CryptoKeyFormat] =
        deriveEnumerationWriter[CryptoKeyFormat]: @nowarn(
          "msg=Der in object CryptoKeyFormat is deprecated"
        )
      lazy implicit final val signingSchemeConfigWriter: ConfigWriter[SigningSchemeConfig] =
        deriveWriter[SigningSchemeConfig]
      lazy implicit final val encryptionSchemeConfigWriter: ConfigWriter[EncryptionSchemeConfig] =
        deriveWriter[EncryptionSchemeConfig]

      implicit def cryptoSchemeConfigWriter[S: ConfigWriter]: ConfigWriter[CryptoSchemeConfig[S]] =
        deriveWriter[CryptoSchemeConfig[S]]

      implicit val cryptoProviderWriter: ConfigWriter[CryptoProvider] =
        deriveEnumerationWriter[CryptoProvider]

      implicit val kmsBackoffConfigWriter: ConfigWriter[KmsConfig.ExponentialBackoffConfig] =
        deriveWriter[KmsConfig.ExponentialBackoffConfig]
      implicit val kmsRetryConfigWriter: ConfigWriter[KmsConfig.RetryConfig] =
        deriveWriter[KmsConfig.RetryConfig]

      def driverConfigWriter(driverConfig: KmsConfig.Driver): ConfigWriter[ConfigValue] =
        ConfigWriter.fromFunction { config =>
          val kmsDriverFactory = DriverKms
            .factory(driverConfig.name)
            .valueOr { err =>
              sys.error(
                s"Failed to instantiate KMS Driver Factory for ${driverConfig.name}: $err"
              )
            }

          val parsedConfig = kmsDriverFactory.configReader
            .from(config)
            .valueOr(err => sys.error(s"Failed to read KMS Driver config: $err"))

          kmsDriverFactory.configWriter(confidential).to(parsedConfig)
        }
      implicit val kmsKeyIdWriter: ConfigWriter[KmsKeyId] =
        ConfigWriter.toString(_.unwrap)
      implicit val kmsWriter: ConfigWriter[EncryptedPrivateStoreConfig.Kms] =
        deriveWriter[EncryptedPrivateStoreConfig.Kms]
      implicit val encryptedPrivateStorageConfigWriter: ConfigWriter[EncryptedPrivateStoreConfig] =
        deriveWriter[EncryptedPrivateStoreConfig]
      implicit val privateKeyStoreConfigWriter: ConfigWriter[PrivateKeyStoreConfig] =
        deriveWriter[PrivateKeyStoreConfig]

      implicit val driverKmsConfigWriter: ConfigWriter[KmsConfig.Driver] =
        ConfigWriter.fromFunction { driverConfig =>
          implicit val driverConfigWriter: ConfigWriter[ConfigValue] =
            Crypto.driverConfigWriter(driverConfig)
          deriveWriter[KmsConfig.Driver].to(driverConfig)
        }
      implicit val awsKmsConfigWriter: ConfigWriter[KmsConfig.Aws] =
        deriveWriter[KmsConfig.Aws]
      implicit val gcpKmsConfigWriter: ConfigWriter[KmsConfig.Gcp] =
        deriveWriter[KmsConfig.Gcp]
      implicit val kmsConfigWriter: ConfigWriter[KmsConfig] =
        deriveWriter[KmsConfig]
      implicit val cryptoWriter: ConfigWriter[CryptoConfig] =
        deriveWriter[CryptoConfig]
    }

    implicit val sequencerTestingInterceptorWriter
        : ConfigWriter[DatabaseSequencerConfig.TestingInterceptor] =
      ConfigWriter.toString(_ => "None")

    lazy implicit final val pemFileOrStringWriter: ConfigWriter[PemFileOrString] =
      new ConfigWriter[PemFileOrString] {
        override def to(value: PemFileOrString): ConfigValue = value match {
          case PemFile(file) => ConfigValueFactory.fromAnyRef(file.unwrap.toString)
          case pemString: PemString => ConfigValueFactory.fromAnyRef(pemString.pemString)
        }
      }

    lazy implicit final val pemFileWriter: ConfigWriter[PemFile] = new ConfigWriter[PemFile] {
      override def to(value: PemFile): ConfigValue =
        ConfigValueFactory.fromAnyRef(value.pemFile.unwrap.toString)
    }

    lazy implicit final val tlsClientCertificateWriter: ConfigWriter[TlsClientCertificate] =
      deriveWriter[TlsClientCertificate]

    lazy implicit final val keepAliveClientConfigWriter: ConfigWriter[KeepAliveClientConfig] =
      deriveWriter[KeepAliveClientConfig]
    lazy implicit final val keepAliveServerConfigWriter: ConfigWriter[BasicKeepAliveServerConfig] =
      deriveWriter[BasicKeepAliveServerConfig]

    lazy implicit final val tlsServerConfigWriter: ConfigWriter[TlsServerConfig] = {
      implicit val serverAuthRequirementConfigNoneWriter
          : ConfigWriter[ServerAuthRequirementConfig.None.type] =
        deriveWriter[ServerAuthRequirementConfig.None.type]
      implicit val serverAuthRequirementConfigOptionalWriter
          : ConfigWriter[ServerAuthRequirementConfig.Optional.type] =
        deriveWriter[ServerAuthRequirementConfig.Optional.type]
      implicit val serverAuthRequirementConfigRequireWriter
          : ConfigWriter[ServerAuthRequirementConfig.Require] =
        deriveWriter[ServerAuthRequirementConfig.Require]
      implicit val serverAuthRequirementConfigWriter: ConfigWriter[ServerAuthRequirementConfig] =
        deriveWriter[ServerAuthRequirementConfig]
      deriveWriter[TlsServerConfig]
    }

    lazy implicit final val identityConfigWriter: ConfigWriter[IdentityConfig] = {
      implicit val nodeNameWriter: ConfigWriter[NodeIdentifierConfig] = {
        implicit val nodeNameConfigWriter: ConfigWriter[NodeIdentifierConfig.Config.type] =
          deriveWriter[NodeIdentifierConfig.Config.type]
        implicit val nodeNameRandomWriter: ConfigWriter[NodeIdentifierConfig.Random.type] =
          deriveWriter[NodeIdentifierConfig.Random.type]
        implicit val nodeNameExplicitWriter: ConfigWriter[NodeIdentifierConfig.Explicit] =
          deriveWriter[NodeIdentifierConfig.Explicit]
        deriveWriter[NodeIdentifierConfig]
      }
      implicit val identityConfigWriterAuto: ConfigWriter[IdentityConfig.Auto] =
        deriveWriter[IdentityConfig.Auto]
      implicit val identityConfigWriterManual: ConfigWriter[IdentityConfig.Manual.type] =
        deriveWriter[IdentityConfig.Manual.type]
      implicit val identityConfigWriterExternal: ConfigWriter[IdentityConfig.External] =
        deriveWriter[IdentityConfig.External]
      deriveWriter[IdentityConfig]
    }
    lazy implicit final val initConfigWriter: ConfigWriter[InitConfig] = deriveWriter[InitConfig]

    lazy implicit final val participantInitConfigWriter: ConfigWriter[ParticipantInitConfig] = {
      implicit val ledgerApiParticipantInitConfigWriter
          : ConfigWriter[ParticipantLedgerApiInitConfig] =
        deriveWriter[ParticipantLedgerApiInitConfig]
      deriveWriter[ParticipantInitConfig]
    }

    implicit val tlsClientConfigWriter: ConfigWriter[TlsClientConfig] =
      deriveWriter[TlsClientConfig]
    lazy implicit final val fullClientConfigWriter: ConfigWriter[FullClientConfig] =
      deriveWriter[FullClientConfig]
    lazy implicit final val sequencerApiClientConfigWriter
        : ConfigWriter[SequencerApiClientConfig] = {
      implicit val tlsClientConfigOnlyTrustFileWriter: ConfigWriter[TlsClientConfigOnlyTrustFile] =
        deriveWriter[TlsClientConfigOnlyTrustFile]
      deriveWriter[SequencerApiClientConfig]
    }
    lazy implicit final val remoteParticipantConfigWriter: ConfigWriter[RemoteParticipantConfig] =
      deriveWriter[RemoteParticipantConfig]
    lazy implicit final val nodeMonitoringConfigWriter: ConfigWriter[NodeMonitoringConfig] = {
      implicit val httpHealthServerConfigWriter: ConfigWriter[HttpHealthServerConfig] =
        deriveWriter[HttpHealthServerConfig]
      implicit val grpcHealthServerConfigWriter: ConfigWriter[GrpcHealthServerConfig] =
        deriveWriter[GrpcHealthServerConfig]
      deriveWriter[NodeMonitoringConfig]
    }

    lazy implicit final val testingTimeServiceConfigWriter
        : ConfigWriter[TestingTimeServiceConfig] = {
      implicit val monotonicTimeWriter: ConfigWriter[TestingTimeServiceConfig.MonotonicTime.type] =
        deriveWriter[TestingTimeServiceConfig.MonotonicTime.type]
      deriveWriter[TestingTimeServiceConfig]
    }

    lazy implicit final val adminServerConfigWriter: ConfigWriter[AdminServerConfig] =
      deriveWriter[AdminServerConfig]
    lazy implicit final val tlsBaseServerConfigWriter: ConfigWriter[TlsBaseServerConfig] =
      deriveWriter[TlsBaseServerConfig]

    lazy implicit final val clockConfigWriter: ConfigWriter[ClockConfig] = {
      implicit val clockConfigRemoteClockWriter: ConfigWriter[ClockConfig.RemoteClock] =
        deriveWriter[ClockConfig.RemoteClock]
      implicit val clockConfigWallClockWriter: ConfigWriter[ClockConfig.WallClock] =
        deriveWriter[ClockConfig.WallClock]
      implicit val clockConfigSimClockWriter: ConfigWriter[ClockConfig.SimClock.type] =
        deriveWriter[ClockConfig.SimClock.type]
      deriveWriter[ClockConfig]
    }

    lazy implicit final val jwtTimestampLeewayConfigWriter: ConfigWriter[JwtTimestampLeeway] =
      deriveWriter[JwtTimestampLeeway]

    lazy implicit final val authServiceConfigWriter: ConfigWriter[AuthServiceConfig] = {
      implicit val authorizedUserWriter: ConfigWriter[AuthorizedUser] =
        confidentialWriter[AuthorizedUser](
          _.copy(userId = "****")
        )
      implicit val authServiceAccessLevelWriter: ConfigWriter[AccessLevel] =
        deriveEnumerationWriter[AccessLevel]
      implicit val authServiceConfigJwtEs256CrtWriter: ConfigWriter[AuthServiceConfig.JwtEs256Crt] =
        deriveWriter[AuthServiceConfig.JwtEs256Crt]
      implicit val authServiceConfigJwtEs512CrtWriter: ConfigWriter[AuthServiceConfig.JwtEs512Crt] =
        deriveWriter[AuthServiceConfig.JwtEs512Crt]
      implicit val authServiceConfigJwtRs256CrtWriter: ConfigWriter[AuthServiceConfig.JwtRs256Crt] =
        deriveWriter[AuthServiceConfig.JwtRs256Crt]
      implicit val authServiceConfigJwtJwksWriter: ConfigWriter[AuthServiceConfig.JwtJwks] =
        deriveWriter[AuthServiceConfig.JwtJwks]
      implicit val authServiceConfigUnsafeJwtHmac256Writer
          : ConfigWriter[AuthServiceConfig.UnsafeJwtHmac256] =
        confidentialWriter[AuthServiceConfig.UnsafeJwtHmac256](
          _.copy(secret = NonEmptyString.tryCreate("****"))
        )
      implicit val authServiceConfigWildcardWriter: ConfigWriter[AuthServiceConfig.Wildcard.type] =
        deriveWriter[AuthServiceConfig.Wildcard.type]
      deriveWriter[AuthServiceConfig]
    }
    lazy implicit final val rateLimitConfigWriter: ConfigWriter[RateLimitingConfig] =
      deriveWriter[RateLimitingConfig]
    lazy implicit final val ledgerApiServerConfigWriter: ConfigWriter[LedgerApiServerConfig] = {
      implicit val lapiKeepAliveServerConfigWriter: ConfigWriter[LedgerApiKeepAliveServerConfig] =
        deriveWriter[LedgerApiKeepAliveServerConfig]
      implicit val ledgerApiInteractiveSubmissionServiceConfigWriter
          : ConfigWriter[InteractiveSubmissionServiceConfig] =
        deriveWriter[InteractiveSubmissionServiceConfig]

      implicit val ledgerApiTopologyAwarePackageSelectionConfigWriter
          : ConfigWriter[TopologyAwarePackageSelectionConfig] =
        deriveWriter[TopologyAwarePackageSelectionConfig]

      deriveWriter[LedgerApiServerConfig]
    }
    lazy implicit final val sequencerTrafficConfigWriter: ConfigWriter[SequencerTrafficConfig] =
      deriveWriter[SequencerTrafficConfig]

    lazy implicit final val httpApiServerConfigWriter: ConfigWriter[JsonApiConfig] = {
      implicit val throttleModeCfgWriter: ConfigWriter[ThrottleMode] =
        ConfigWriter.toString[ThrottleMode] {
          case ThrottleMode.Shaping => "shaping"
          case ThrottleMode.Enforcing => "enforcing"
        }
      implicit val wsConfigWriter: ConfigWriter[WebsocketConfig] =
        deriveWriter[WebsocketConfig]
      implicit val httpServerConfigWriter: ConfigWriter[HttpServerConfig] =
        deriveWriter[HttpServerConfig]
      deriveWriter[JsonApiConfig]
    }

    lazy implicit final val topologyConfigWriter: ConfigWriter[TopologyConfig] =
      deriveWriter[TopologyConfig]

    lazy implicit val databaseSequencerExclusiveStorageConfigWriter
        : ConfigWriter[DatabaseSequencerExclusiveStorageConfig] =
      deriveWriter[DatabaseSequencerExclusiveStorageConfig]
    lazy implicit val sequencerHighAvailabilityConfigWriter
        : ConfigWriter[SequencerHighAvailabilityConfig] =
      deriveWriter[SequencerHighAvailabilityConfig]
    lazy implicit final val sequencerConfigDatabaseWriter: ConfigWriter[SequencerConfig.Database] =
      deriveWriter[SequencerConfig.Database]
    lazy implicit final val blockSequencerConfigWriter: ConfigWriter[BlockSequencerConfig] =
      deriveWriter[BlockSequencerConfig]
    lazy implicit final val sequencerReaderConfigWriter: ConfigWriter[SequencerReaderConfig] =
      deriveWriter[SequencerReaderConfig]
    lazy implicit final val sequencerWriterCommitModeConfigWriter: ConfigWriter[CommitMode] =
      deriveEnumerationWriter[CommitMode]
    lazy implicit final val sequencerWriterConfigWriter: ConfigWriter[SequencerWriterConfig] =
      deriveWriter[SequencerWriterConfig]
    lazy implicit final val bytesUnitWriter: ConfigWriter[BytesUnit] =
      BasicWriters.configMemorySizeWriter.contramap[BytesUnit](b =>
        ConfigMemorySize.ofBytes(b.bytes)
      )
    lazy implicit final val sequencerWriterConfigHighThroughputWriter
        : ConfigWriter[SequencerWriterConfig.HighThroughput] =
      deriveWriter[SequencerWriterConfig.HighThroughput]
    lazy implicit final val sequencerWriterConfigLowLatencyWriter
        : ConfigWriter[SequencerWriterConfig.LowLatency] =
      deriveWriter[SequencerWriterConfig.LowLatency]

    lazy implicit val publicServerConfigWriter: ConfigWriter[PublicServerConfig] =
      deriveWriter[PublicServerConfig]

    lazy implicit val memoryWriter: ConfigWriter[StorageConfig.Memory] =
      deriveWriter[StorageConfig.Memory]
    lazy implicit val h2Writer: ConfigWriter[DbConfig.H2] =
      confidentialWriter[DbConfig.H2](x => x.copy(config = DbConfig.hideConfidential(x.config)))
    lazy implicit val postgresWriter: ConfigWriter[DbConfig.Postgres] =
      confidentialWriter[DbConfig.Postgres](x =>
        x.copy(config = DbConfig.hideConfidential(x.config))
      )
    lazy implicit val storageConfigWriter: ConfigWriter[StorageConfig] =
      deriveWriter[StorageConfig]
    lazy implicit val dbLockConfigWriter: ConfigWriter[DbLockConfig] = deriveWriter[DbLockConfig]
    lazy implicit val lockedConnectionConfigWriter: ConfigWriter[DbLockedConnectionConfig] =
      deriveWriter[DbLockedConnectionConfig]
    lazy implicit val connectionPoolConfigWriter: ConfigWriter[DbLockedConnectionPoolConfig] =
      deriveWriter[DbLockedConnectionPoolConfig]

    lazy implicit final val bftBlockOrdererBftP2PServerConfigWriter
        : ConfigWriter[BftBlockOrdererConfig.P2PServerConfig] =
      deriveWriter[BftBlockOrdererConfig.P2PServerConfig]
    lazy implicit val bftBlockOrdererBftP2PEndpointConfigWriter
        : ConfigWriter[BftBlockOrdererConfig.P2PEndpointConfig] =
      deriveWriter[BftBlockOrdererConfig.P2PEndpointConfig]
    lazy implicit val bftBlockOrdererBftP2PNetworkAuthenticationConfigWriter
        : ConfigWriter[BftBlockOrdererConfig.P2PNetworkAuthenticationConfig] =
      deriveWriter[BftBlockOrdererConfig.P2PNetworkAuthenticationConfig]
    lazy implicit val bftBlockOrdererBftP2PNetworkConfigWriter
        : ConfigWriter[BftBlockOrdererConfig.P2PNetworkConfig] =
      deriveWriter[BftBlockOrdererConfig.P2PNetworkConfig]
    lazy implicit val bftBlockOrdererPruningConfigWriter
        : ConfigWriter[BftBlockOrdererConfig.PruningConfig] =
      deriveWriter[BftBlockOrdererConfig.PruningConfig]
    lazy implicit val bftBlockOrdererConfigWriter: ConfigWriter[BftBlockOrdererConfig] =
      deriveWriter[BftBlockOrdererConfig]

    lazy implicit val sequencerConfigBftSequencerWriter
        : ConfigWriter[SequencerConfig.BftSequencer] =
      deriveWriter[SequencerConfig.BftSequencer]
    lazy implicit val sequencerPruningConfigWriter
        : ConfigWriter[DatabaseSequencerConfig.SequencerPruningConfig] =
      deriveWriter[DatabaseSequencerConfig.SequencerPruningConfig]

    implicit def sequencerConfigWriter[C]: ConfigWriter[SequencerConfig] = {
      case dbSequencerConfig: SequencerConfig.Database =>
        import pureconfig.syntax.*
        Map("type" -> "database").toConfig
          .withFallback(sequencerConfigDatabaseWriter.to(dbSequencerConfig))

      case bftSequencerConfig: SequencerConfig.BftSequencer =>
        import pureconfig.syntax.*
        Map("type" -> BftSequencerFactory.ShortName).toConfig
          .withFallback(sequencerConfigBftSequencerWriter.to(bftSequencerConfig))

      case otherSequencerConfig: SequencerConfig.External =>
        import scala.jdk.CollectionConverters.*
        val sequencerType = otherSequencerConfig.sequencerType

        val factory: SequencerDriverFactory {
          type ConfigType = C
        } = DriverBlockSequencerFactory
          .getSequencerDriverFactory(sequencerType, SequencerDriver.DriverApiVersion)

        val blockConfigValue = blockSequencerConfigWriter.to(otherSequencerConfig.block)

        val configValue = factory.configParser
          .from(otherSequencerConfig.config)
          // in order to make use of the confidential flag, we must first parse the raw sequencer driver config
          // and then write it again now making sure to use hide fields if confidentiality is turned on
          .map(parsedConfig => factory.configWriter(confidential).to(parsedConfig))
          .valueOr(error =>
            sys.error(s"Failed to read $sequencerType sequencer's config. Error: $error")
          )
        ConfigValueFactory.fromMap(
          Map[String, Object](
            "type" -> sequencerType,
            "block" -> blockConfigValue,
            "config" -> configValue,
          ).asJava
        )
    }

    lazy implicit final val sequencerNodeParameterConfigWriter
        : ConfigWriter[SequencerNodeParameterConfig] =
      deriveWriter[SequencerNodeParameterConfig]
    lazy implicit final val SequencerHealthConfigWriter: ConfigWriter[SequencerHealthConfig] =
      deriveWriter[SequencerHealthConfig]
    lazy implicit final val remoteSequencerConfigWriter: ConfigWriter[RemoteSequencerConfig] =
      deriveWriter[RemoteSequencerConfig]

    lazy implicit final val mediatorConfigWriter: ConfigWriter[MediatorConfig] = {
      implicit val mediatorPruningConfigWriter: ConfigWriter[MediatorPruningConfig] =
        deriveWriter[MediatorPruningConfig]
      deriveWriter[MediatorConfig]
    }
    lazy implicit final val mediatorNodeParameterConfigWriter
        : ConfigWriter[MediatorNodeParameterConfig] =
      deriveWriter[MediatorNodeParameterConfig]
    lazy implicit final val remoteMediatorConfigWriter: ConfigWriter[RemoteMediatorConfig] =
      deriveWriter[RemoteMediatorConfig]

    lazy implicit final val monitoringConfigWriter: ConfigWriter[MonitoringConfig] = {
      implicit val tracingConfigDisabledSpanExporterWriter
          : ConfigWriter[TracingConfig.Exporter.Disabled.type] =
        deriveWriter[TracingConfig.Exporter.Disabled.type]
      implicit val tracingConfigZipkinSpanExporterWriter
          : ConfigWriter[TracingConfig.Exporter.Zipkin] =
        deriveWriter[TracingConfig.Exporter.Zipkin]
      implicit val tracingConfigOtlpSpanExporterWriter: ConfigWriter[TracingConfig.Exporter.Otlp] =
        deriveWriter[TracingConfig.Exporter.Otlp]
      implicit val tracingConfigSpanExporterWriter: ConfigWriter[TracingConfig.Exporter] =
        deriveWriter[TracingConfig.Exporter]
      implicit val tracingConfigAlwaysOnSamplerWriter
          : ConfigWriter[TracingConfig.Sampler.AlwaysOn] =
        deriveWriter[TracingConfig.Sampler.AlwaysOn]
      implicit val tracingConfigAlwaysOffSamplerWriter
          : ConfigWriter[TracingConfig.Sampler.AlwaysOff] =
        deriveWriter[TracingConfig.Sampler.AlwaysOff]
      implicit val tracingConfigTraceIdRatioSamplerWriter
          : ConfigWriter[TracingConfig.Sampler.TraceIdRatio] =
        deriveWriter[TracingConfig.Sampler.TraceIdRatio]
      implicit val tracingConfigSamplerWriter: ConfigWriter[TracingConfig.Sampler] =
        deriveWriter[TracingConfig.Sampler]
      implicit val tracingConfigBatchSpanProcessorWriter
          : ConfigWriter[TracingConfig.BatchSpanProcessor] =
        deriveWriter[TracingConfig.BatchSpanProcessor]
      implicit val tracingConfigTracerWriter: ConfigWriter[TracingConfig.Tracer] =
        deriveWriter[TracingConfig.Tracer]
      // treat TracingConfig.Propagation as an enum as we currently only have case object types in the sealed family
      implicit val tracingConfigPropagationWriter: ConfigWriter[TracingConfig.Propagation] =
        deriveEnumerationWriter[TracingConfig.Propagation]
      implicit val tracingConfigWriter: ConfigWriter[TracingConfig] =
        deriveWriter[TracingConfig]
      implicit val deadlockDetectionConfigWriter: ConfigWriter[DeadlockDetectionConfig] =
        deriveWriter[DeadlockDetectionConfig]
      implicit val metricsFilterConfigWriter: ConfigWriter[MetricsFilterConfig] =
        deriveWriter[MetricsFilterConfig]
      implicit val metricsConfigPrometheusWriter: ConfigWriter[MetricsReporterConfig.Prometheus] =
        deriveWriter[MetricsReporterConfig.Prometheus]
      implicit val metricsConfigCsvWriter: ConfigWriter[MetricsReporterConfig.Csv] =
        deriveWriter[MetricsReporterConfig.Csv]
      implicit val metricsConfigLoggingWriter: ConfigWriter[MetricsReporterConfig.Logging] =
        deriveWriter[MetricsReporterConfig.Logging]
      implicit val metricsConfigJvmMetricsWriter: ConfigWriter[MetricsConfig.JvmMetrics] =
        deriveWriter[MetricsConfig.JvmMetrics]
      implicit val metricsReporterConfigWriter: ConfigWriter[MetricsReporterConfig] =
        deriveWriter[MetricsReporterConfig]
      implicit val histogramBucketConfigWriter: ConfigWriter[HistogramDefinition.Buckets] =
        deriveWriter[HistogramDefinition.Buckets]
      implicit val histogramExponentialConfigWriter: ConfigWriter[HistogramDefinition.Exponential] =
        deriveWriter[HistogramDefinition.Exponential]
      implicit val histogramAggregationTypeConfigWriter
          : ConfigWriter[HistogramDefinition.AggregationType] =
        deriveWriter[HistogramDefinition.AggregationType]
      implicit val histogramDefinitionConfigWriter: ConfigWriter[HistogramDefinition] =
        deriveWriter[HistogramDefinition]
      implicit val metricQualificationConfigWriter: ConfigWriter[MetricQualification] =
        ConfigWriter.toString[MetricQualification] {
          case MetricQualification.Debug => "debug"
          case MetricQualification.Errors => "errors"
          case MetricQualification.Saturation => "saturation"
          case MetricQualification.Traffic => "traffic"
          case MetricQualification.Latency => "latency"
        }
      implicit val metricsConfigWriter: ConfigWriter[MetricsConfig] =
        deriveWriter[MetricsConfig]

      implicit val apiLoggingConfigWriter: ConfigWriter[ApiLoggingConfig] =
        deriveWriter[ApiLoggingConfig]
      lazy implicit val loggingConfigWriter: ConfigWriter[LoggingConfig] = {
        implicit val gcLoggingConfigWriter: ConfigWriter[GCLoggingConfig] =
          deriveWriter[GCLoggingConfig]
        deriveWriter[LoggingConfig]
      }
      deriveWriter[MonitoringConfig]
    }

    import Crypto.*
    lazy implicit final val sessionSigningKeysConfigWriter: ConfigWriter[SessionSigningKeysConfig] =
      deriveWriter[SessionSigningKeysConfig]

    lazy implicit final val cachingConfigsWriter: ConfigWriter[CachingConfigs] = {
      implicit val cacheConfigWriter: ConfigWriter[CacheConfig] =
        deriveWriter[CacheConfig]
      implicit val cacheConfigWithTimeoutWriter: ConfigWriter[CacheConfigWithTimeout] =
        deriveWriter[CacheConfigWithTimeout]
      implicit val cacheConfigWithMemoryBoundsWriter: ConfigWriter[CacheConfigWithMemoryBounds] =
        deriveWriter[CacheConfigWithMemoryBounds]
      implicit val sessionEncryptionKeyCacheConfigWriter
          : ConfigWriter[SessionEncryptionKeyCacheConfig] =
        deriveWriter[SessionEncryptionKeyCacheConfig]
      deriveWriter[CachingConfigs]
    }

    lazy implicit final val ledgerApiServerParametersConfigWriter
        : ConfigWriter[LedgerApiServerParametersConfig] = {
      implicit val contractIdSeedingWriter: ConfigWriter[Seeding] = ConfigWriter.toString(_.name)
      implicit val ledgerApiContractLoaderConfigWriter: ConfigWriter[ContractLoaderConfig] =
        deriveWriter[ContractLoaderConfig]
      deriveWriter[LedgerApiServerParametersConfig]
    }

    lazy implicit final val participantNodeParameterConfigWriter
        : ConfigWriter[ParticipantNodeParameterConfig] = {
      implicit val cantonEngineConfigWriter: ConfigWriter[CantonEngineConfig] = {
        implicit val engineLoggingConfigWriter: ConfigWriter[EngineLoggingConfig] =
          deriveWriter[EngineLoggingConfig]
        deriveWriter[CantonEngineConfig]
      }
      implicit val participantStoreConfigWriter: ConfigWriter[ParticipantStoreConfig] = {
        implicit val journalPruningConfigWriter: ConfigWriter[JournalPruningConfig] =
          deriveWriter[JournalPruningConfig]
        deriveWriter[ParticipantStoreConfig]
      }
      implicit val adminWorkflowConfigWriter: ConfigWriter[AdminWorkflowConfig] =
        deriveWriter[AdminWorkflowConfig]
      implicit val commandProgressTrackerConfigWriter: ConfigWriter[CommandProgressTrackerConfig] =
        deriveWriter[CommandProgressTrackerConfig]

      implicit val packageMetadataViewConfigWriter: ConfigWriter[PackageMetadataViewConfig] =
        deriveWriter[PackageMetadataViewConfig]
      implicit val unsafeOnlinePartyReplicationConfigWriter
          : ConfigWriter[UnsafeOnlinePartyReplicationConfig] =
        deriveWriter[UnsafeOnlinePartyReplicationConfig]
      deriveWriter[ParticipantNodeParameterConfig]
    }
    lazy implicit final val timeTrackerConfigWriter: ConfigWriter[SynchronizerTimeTrackerConfig] = {
      implicit val timeRequestConfigWriter: ConfigWriter[TimeProofRequestConfig] =
        deriveWriter[TimeProofRequestConfig]
      deriveWriter[SynchronizerTimeTrackerConfig]
    }

    lazy implicit val authTokenManagerConfigWriter: ConfigWriter[AuthenticationTokenManagerConfig] =
      deriveWriter[AuthenticationTokenManagerConfig]
    lazy implicit final val sequencerClientConfigWriter: ConfigWriter[SequencerClientConfig] =
      deriveWriter[SequencerClientConfig]

    lazy implicit final val cantonParametersWriter: ConfigWriter[CantonParameters] = {
      implicit val ammoniteConfigWriter: ConfigWriter[AmmoniteConsoleConfig] =
        deriveWriter[AmmoniteConsoleConfig]
      implicit val retentionPeriodDefaultsConfigWriter: ConfigWriter[RetentionPeriodDefaults] =
        deriveWriter[RetentionPeriodDefaults]
      implicit val timeoutSettingsWriter: ConfigWriter[TimeoutSettings] = {
        implicit val consoleCommandTimeoutWriter: ConfigWriter[ConsoleCommandTimeout] =
          deriveWriter[ConsoleCommandTimeout]
        implicit val processingTimeoutWriter: ConfigWriter[ProcessingTimeout] =
          deriveWriter[ProcessingTimeout]
        deriveWriter[TimeoutSettings]
      }
      deriveWriter[CantonParameters]
    }
    lazy implicit final val cantonFeaturesWriter: ConfigWriter[CantonFeatures] =
      deriveWriter[CantonFeatures]
    lazy implicit final val cantonWatchdogConfigWriter: ConfigWriter[WatchdogConfig] =
      deriveWriter[WatchdogConfig]

    lazy implicit final val reportingLevelWriter
        : ConfigWriter[StartupMemoryCheckConfig.ReportingLevel] =
      deriveEnumerationWriter[StartupMemoryCheckConfig.ReportingLevel]

    lazy implicit final val startupMemoryCheckConfigWriter: ConfigWriter[StartupMemoryCheckConfig] =
      deriveWriter[StartupMemoryCheckConfig]

    implicit val participantReplicationConfigWriter: ConfigWriter[ReplicationConfig] =
      deriveWriter[ReplicationConfig]
    implicit val participantEnterpriseFeaturesConfigWriter
        : ConfigWriter[EnterpriseParticipantFeaturesConfig] =
      deriveWriter[EnterpriseParticipantFeaturesConfig]

    implicit val localParticipantConfigWriter: ConfigWriter[ParticipantNodeConfig] = {
      val writers = new DeclarativeParticipantConfig.ConfigWriters(confidential)
      import writers.*
      deriveWriter[ParticipantNodeConfig]
    }
    implicit val mediatorNodeConfigWriter: ConfigWriter[MediatorNodeConfig] =
      deriveWriter[MediatorNodeConfig]
    implicit val sequencerNodeConfigWriter: ConfigWriter[SequencerNodeConfig] =
      deriveWriter[SequencerNodeConfig]
  }

  private def makeWriter(confidential: Boolean): ConfigWriter[CantonConfig] = {
    val writers = new CantonConfig.ConfigWriters(confidential)
    import writers.*

    deriveWriter[CantonConfig]
  }
  private lazy val nonConfidentialWriter: ConfigWriter[CantonConfig] =
    makeWriter(confidential = false)
  private lazy val confidentialConfigWriter: ConfigWriter[CantonConfig] =
    makeWriter(confidential = true)

  def makeConfidentialString(config: CantonConfig): String =
    "canton " + confidentialConfigWriter
      .to(config)
      .render(CantonConfig.defaultConfigRenderer)

  /** Parses and merges the provided configuration files into a single
    * [[com.typesafe.config.Config]]. Also loads and merges the default config (as defined by the
    * Lightbend config library) with the provided configuration files. Unless you know that you
    * explicitly only want to use the provided configuration files, use this method. Any errors will
    * be returned, but not logged.
    *
    * @param files
    *   config files to read, parse and merge
    * @return
    *   [[scala.Right]] [[com.typesafe.config.Config]] if parsing was successful.
    */
  private def parseAndMergeConfigs(
      files: NonEmpty[Seq[File]]
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, Config] = {
    val baseConfig = ConfigFactory.load()
    for {
      verifiedFiles <- verifyThatFilesCanBeRead(files)
      parsedFiles <- parseConfigs(verifiedFiles)
      combinedConfig = mergeConfigs(parsedFiles).withFallback(baseConfig)
    } yield combinedConfig
  }

  /** Parses and merges the provided configuration files into a single
    * [[com.typesafe.config.Config]]. Does not load and merge the default config (as defined by the
    * Lightbend config library) with the provided configuration files. Only use this if you
    * explicitly know that you don't want to load and merge the default config.
    *
    * @param files
    *   config files to read, parse and merge
    * @return
    *   [[scala.Right]] [[com.typesafe.config.Config]] if parsing was successful.
    */
  def parseAndMergeJustCLIConfigs(
      files: NonEmpty[Seq[File]]
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, Config] =
    for {
      verifiedFiles <- verifyThatFilesCanBeRead(files)
      parsedFiles <- parseConfigs(verifiedFiles)
      combinedConfig = mergeConfigs(parsedFiles)
    } yield combinedConfig

  /** Renders a configuration file such that we can write it to the log-file on startup */
  def renderForLoggingOnStartup(config: Config): String = {
    import scala.jdk.CollectionConverters.*
    val replace =
      Set("secret", "pw", "password", "ledger-api-jdbc-url", "jdbc", "token", "admin-token")
    val blinded = ConfigValueFactory.fromAnyRef("****")
    def goVal(key: String, c: ConfigValue): ConfigValue =
      c match {
        case lst: ConfigList => goLst(lst)
        case obj: ConfigObject =>
          goObj(obj)
        case other =>
          if (replace.contains(key))
            blinded
          else other
      }
    def goObj(c: ConfigObject): ConfigObject = {
      val resolved = Try(c.isEmpty).isSuccess
      if (resolved) {
        c.entrySet().asScala.map(x => (x.getKey, x.getValue)).foldLeft(c) {
          case (acc, (key, value)) =>
            acc.withValue(key, goVal(key, value))
        }
      } else c
    }
    def goLst(c: ConfigList): ConfigList = {
      val mapped = (0 until c.size()) map { idx =>
        goVal("idx", c.get(idx))
      }
      ConfigValueFactory.fromIterable(mapped.asJava)
    }
    def go(c: Config): Config =
      c
        .root()
        .entrySet()
        .asScala
        .map(x => (x.getKey, x.getValue))
        .foldLeft(c) { case (subConfig, (key, obj)) =>
          subConfig.withValue(key, goVal(key, obj))
        }
    go(config.resolve()) // Resolve the config _before_ redacting confidential information
      .root()
      .get("canton")
      .render(CantonConfig.defaultConfigRenderer)
  }

  private def verifyThatFilesCanBeRead(
      files: NonEmpty[Seq[File]]
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, NonEmpty[Seq[File]]] = {
    val filesThatCannotBeRead = files.filterNot(_.canRead)
    Either.cond(
      filesThatCannotBeRead.isEmpty,
      files,
      CannotReadFilesError.Error(filesThatCannotBeRead),
    )
  }

  private def parseConfigs(
      files: NonEmpty[Seq[File]]
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, NonEmpty[Seq[Config]]] = {
    import cats.implicits.*
    files.toNEF
      .traverse(f => Either.catchOnly[ConfigException](ConfigFactory.parseFile(f)).toValidatedNec)
      .toEither
      .leftMap(errs => CannotParseFilesError.Error(errs.toList))
      .leftWiden[CantonConfigError]
  }

  private def configOrExit[ConfClass](
      result: Either[CantonConfigError, ConfClass]
  ): ConfClass =
    result.valueOr { _ =>
      sys.exit(1)
    }

  /** Merge a number of [[com.typesafe.config.Config]] instances into a single
    * [[com.typesafe.config.Config]]. If the same key is included in multiple configurations, then
    * the last definition has highest precedence.
    */
  def mergeConfigs(firstConfig: Config, otherConfigs: Seq[Config]): Config =
    otherConfigs.foldLeft(firstConfig)((combined, config) => config.withFallback(combined))

  /** Merge a number of [[com.typesafe.config.Config]] instances into a single
    * [[com.typesafe.config.Config]]. If the same key is included in multiple configurations, then
    * the last definition has highest precedence.
    */
  def mergeConfigs(configs: NonEmpty[Seq[Config]]): Config =
    mergeConfigs(configs.head1, configs.tail1)

  /** Parses the provided files to generate a [[com.typesafe.config.Config]], then attempts to load
    * the [[com.typesafe.config.Config]] based on the given ClassTag. Will return an error (but not
    * log anything) if any steps fails.
    *
    * @param files
    *   config files to read, parse and merge
    * @return
    *   [[scala.Right]] of type `ConfClass` (e.g. [[CantonConfig]])) if parsing was successful.
    */
  def parseAndLoad(
      files: Seq[File],
      edition: CantonEdition,
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, CantonConfig] =
    for {
      nonEmpty <- NonEmpty.from(files).toRight(NoConfigFiles.Error())
      parsedAndMerged <- parseAndMergeConfigs(nonEmpty)
      loaded <- loadAndValidate(parsedAndMerged, edition)
    } yield loaded

  /** Parses the provided files to generate a [[com.typesafe.config.Config]], then attempts to load
    * the [[com.typesafe.config.Config]] based on the given ClassTag. Will log the error and exit
    * with code 1, if any error is encountered. *
    *
    * @param files
    *   config files to read - must be a non-empty Seq
    * @throws java.lang.IllegalArgumentException
    *   if `files` is empty
    * @return
    *   [[scala.Right]] of type `ClassTag` (e.g. [[CantonConfig]])) if parsing was successful.
    */
  def parseAndLoadOrExit(files: Seq[File], edition: CantonEdition)(implicit
      elc: ErrorLoggingContext = elc
  ): CantonConfig = {
    val result = parseAndLoad(files, edition)
    configOrExit(result)
  }

  /** Will load a case class configuration (defined by template args) from the configuration object.
    * Any configuration errors encountered will be returned (but not logged).
    *
    * @return
    *   [[scala.Right]] of type [[CantonConfig]] if parsing was successful.
    */
  def loadAndValidate(
      config: Config,
      edition: CantonEdition,
  )(implicit elc: ErrorLoggingContext = elc): Either[CantonConfigError, CantonConfig] = {
    // config.resolve forces any substitutions to be resolved (typically referenced environment variables or system properties).
    // this normally would happen by default during ConfigFactory.load(),
    // however we have to manually as we've merged in individual files.
    val result = Either.catchOnly[UnresolvedSubstitution](config.resolve())
    result match {
      case Right(resolvedConfig) =>
        loadRawConfig(resolvedConfig)
          .flatMap { conf =>
            val confWithDefaults = conf.withDefaults(new DefaultPorts(), edition)
            confWithDefaults
              .validate(edition)
              .toEither
              .map(_ => confWithDefaults)
              .leftMap(causes => ConfigErrors.ValidationError.Error(causes.toList))
          }
      case Left(substitutionError) => Left(SubstitutionError.Error(Seq(substitutionError)))
    }
  }

  private val logger: Logger = LoggerFactory.getLogger(classOf[CantonConfig])
  private val elc = ErrorLoggingContext(
    TracedLogger(logger),
    NamedLoggerFactory.root.properties,
    TraceContext.empty,
  )

  /** Will load a case class configuration (defined by template args) from the configuration object.
    * If any configuration errors are encountered, they will be logged and the JVM will exit with
    * code 1.
    */
  def loadOrExit(
      config: Config,
      edition: CantonEdition,
  )(implicit elc: ErrorLoggingContext = elc): CantonConfig =
    loadAndValidate(config, edition).valueOr(_ => sys.exit(1))

  private[config] def loadRawConfig(
      rawConfig: Config
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, CantonConfig] =
    pureconfig.ConfigSource
      .fromConfig(rawConfig)
      .at("canton")
      .load[CantonConfig]
      .leftMap(failures =>
        GenericConfigError.Error(ConfigErrors.getMessage[CantonConfig](failures))
      )

  lazy val defaultConfigRenderer: ConfigRenderOptions =
    ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false)

  def save(
      config: CantonConfig,
      filename: String,
      removeConfigPaths: Set[(String, Option[(String, Any)])] = Set.empty,
  ): Unit = {
    import better.files.File as BFile
    val unfiltered = nonConfidentialWriter.to(config)
    val value = unfiltered match {
      case o: ConfigObject =>
        removeConfigPaths
          .foldLeft(o.toConfig) {
            case (v, (p, None)) => v.withoutPath(p)
            case (v, (p, Some((replacementPath, value)))) =>
              if (v.hasPath(p))
                v.withoutPath(p).withValue(replacementPath, ConfigValueFactory.fromAnyRef(value))
              else v.withoutPath(p)
          }
          .root()
      case _ => unfiltered
    }
    val _ =
      BFile(filename).write(value.atKey("canton").root().render(CantonConfig.defaultConfigRenderer))
  }

}
