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
import com.digitalasset.canton.auth.AccessLevel
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
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.networking.Endpoint
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
import com.digitalasset.canton.platform.config.InteractiveSubmissionServiceConfig
import com.digitalasset.canton.pureconfigutils.SharedConfigReaders.catchConvertError
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.block.{SequencerDriver, SequencerDriverFactory}
import com.digitalasset.canton.synchronizer.mediator.{
  MediatorConfig,
  MediatorNodeConfigCommon,
  MediatorNodeParameterConfig,
  MediatorNodeParameters,
  MediatorPruningConfig,
  RemoteMediatorConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.block.DriverBlockSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.{
  BftBlockOrderer,
  BftSequencerFactory,
}
import com.digitalasset.canton.synchronizer.sequencer.config.{
  RemoteSequencerConfig,
  SequencerNodeConfigCommon,
  SequencerNodeInitConfig,
  SequencerNodeParameterConfig,
  SequencerNodeParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.tracing.TracingConfig
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
import org.apache.pekko.stream.ThrottleMode
import pureconfig.*
import pureconfig.error.CannotConvert
import pureconfig.generic.{FieldCoproductHint, ProductHint}

import java.io.File
import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.reflect.ClassTag
import scala.util.Try

/** Deadlock detection configuration
  *
  * A simple deadlock detection method. Using a background scheduler, we schedule a trivial future on the EC.
  * If the Future is not executed until we check again, we alert.
  *
  * @param enabled if true, we'll monitor the EC for deadlocks (or slow processings)
  * @param interval how often we check the EC
  * @param warnInterval how often we report a deadlock as still being active
  */
final case class DeadlockDetectionConfig(
    enabled: Boolean = true,
    interval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(3),
    warnInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(10),
)

/** Configuration for metrics and tracing
  *
  * @param deadlockDetection Should we regularly check our environment EC for deadlocks?
  * @param metrics Optional Metrics Reporter used to expose internally captured metrics
  * @param tracing       Tracing configuration
  *
  * @param dumpNumRollingLogFiles How many of the rolling log files shold be included in the remote dump. Default is 0.
  */
final case class MonitoringConfig(
    deadlockDetection: DeadlockDetectionConfig = DeadlockDetectionConfig(),
    metrics: MetricsConfig = MetricsConfig(),
    tracing: TracingConfig = TracingConfig(),
    logging: LoggingConfig = LoggingConfig(),
    dumpNumRollingLogFiles: NonNegativeInt = MonitoringConfig.defaultDumpNumRollingLogFiles,
) extends LazyLogging

object MonitoringConfig {
  private val defaultDumpNumRollingLogFiles = NonNegativeInt.tryCreate(0)
}

/** Configuration for console command timeouts
  *
  * @param bounded timeout on how long "bounded" operations, i.e. operations which normally are supposed to conclude
  *                in a fixed timeframe can run before the console considers them as failed.
  * @param unbounded timeout on how long "unbounded" operations can run, potentially infinite.
  * @param ledgerCommand default timeout used for ledger commands
  * @param ping default ping timeout
  * @param testingBong default bong timeout
  */
final case class ConsoleCommandTimeout(
    bounded: NonNegativeDuration = ConsoleCommandTimeout.defaultBoundedTimeout,
    unbounded: NonNegativeDuration = ConsoleCommandTimeout.defaultUnboundedTimeout,
    ledgerCommand: NonNegativeDuration = ConsoleCommandTimeout.defaultLedgerCommandsTimeout,
    ping: NonNegativeDuration = ConsoleCommandTimeout.defaultPingTimeout,
    testingBong: NonNegativeDuration = ConsoleCommandTimeout.defaultTestingBongTimeout,
)

object ConsoleCommandTimeout {
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
)

sealed trait ClockConfig extends Product with Serializable
object ClockConfig {

  /** Configure Canton to use a simclock
    *
    * A SimClock's time only progresses when [[com.digitalasset.canton.time.SimClock.advance]] is explicitly called.
    */
  case object SimClock extends ClockConfig

  /** Configure Canton to use the wall clock (default)
    *
    * @param skews   map of clock skews, indexed by node name (used for testing, default empty)
    *                Any node whose name is contained in the map will use a WallClock, but
    *                the time of the clock will by shifted by the associated duration, which can be positive
    *                or negative. The clock shift will be constant during the life of the node.
    */
  final case class WallClock(skews: Map[String, FiniteDuration] = Map.empty) extends ClockConfig

  /** Configure Canton to use a remote clock
    *
    * In crash recovery testing scenarios, we want several processes to use the same time.
    * In most cases, we can rely on NTP and the host clock. However, in cases
    * where we test static time, we need the spawn processes to access the main processes
    * clock.
    * For such cases we can use a remote clock. However, no user should ever require this.
    * @param remoteApi admin-port of the node to read the time from
    */
  final case class RemoteClock(remoteApi: ClientConfig) extends ClockConfig

}

/** Default retention periods used by pruning commands where no values are explicitly specified.
  * Although by default the commands will retain enough data to remain operational,
  * however operators may like to retain more than this to facilitate possible disaster recovery scenarios or
  * retain evidence of completed transactions.
  */
final case class RetentionPeriodDefaults(
    sequencer: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(7),
    mediator: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(7),
)

/** Parameters for testing Canton. Use default values in a production environment.
  *
  * @param enableAdditionalConsistencyChecks if true, run additional consistency checks. This will degrade performance.
  * @param manualStart  If set to true, the nodes have to be manually started via console (default false)
  * @param startupParallelism Start up to N nodes in parallel (default is num-threads)
  * @param nonStandardConfig don't fail config validation on non-standard configuration settings
  * @param sessionSigningKeys Configure the use of session signing keys in the protocol
  * @param alphaVersionSupport If true, allow synchronizer nodes to use alpha protocol versions and participant nodes to connect to such synchronizers
  * @param betaVersionSupport If true, allow synchronizer nodes to use beta protocol versions and participant nodes to connect to such synchronizers
  * @param timeouts Sets the timeouts used for processing and console
  * @param portsFile A ports file name, where the ports of all participants will be written to after startup
  * @param exitOnFatalFailures If true the node will exit/stop the process in case of fatal failures
  */
final case class CantonParameters(
    clock: ClockConfig = ClockConfig.WallClock(),
    enableAdditionalConsistencyChecks: Boolean = false,
    manualStart: Boolean = false,
    startupParallelism: Option[PositiveInt] = None,
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    nonStandardConfig: Boolean = true,
    sessionSigningKeys: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    alphaVersionSupport: Boolean = true,
    betaVersionSupport: Boolean = false,
    portsFile: Option[String] = None,
    timeouts: TimeoutSettings = TimeoutSettings(),
    retentionPeriodDefaults: RetentionPeriodDefaults = RetentionPeriodDefaults(),
    console: AmmoniteConsoleConfig = AmmoniteConsoleConfig(),
    exitOnFatalFailures: Boolean = true,
    startupMemoryCheckConfig: StartupMemoryCheckConfig = StartupMemoryCheckConfig(
      ReportingLevel.Warn
    ),
) {
  def getStartupParallelism(numThreads: Int): Int =
    startupParallelism.fold(numThreads)(_.value)
}

/** Control which features are turned on / off in Canton
  *
  * @param enablePreviewCommands         Feature flag to enable the set of commands that use functionality which we don't deem stable.
  * @param enableTestingCommands         Feature flag to enable the set of commands used by Canton developers for testing purposes.
  * @param enableRepairCommands          Feature flag to enable the set of commands used by Canton operators for manual repair purposes.
  */
final case class CantonFeatures(
    enablePreviewCommands: Boolean = false,
    enableTestingCommands: Boolean = false,
    enableRepairCommands: Boolean = false,
) {
  def featureFlags: Set[FeatureFlag] =
    (Seq(FeatureFlag.Stable)
      ++ (if (enableTestingCommands) Seq(FeatureFlag.Testing) else Seq())
      ++ (if (enablePreviewCommands) Seq(FeatureFlag.Preview) else Seq())
      ++ (if (enableRepairCommands) Seq(FeatureFlag.Repair) else Seq())).toSet
}

/** Root configuration parameters for a single Canton process. */
trait CantonConfig {

  type ParticipantConfigType <: LocalParticipantConfig & ConfigDefaults[
    DefaultPorts,
    ParticipantConfigType,
  ]
  type MediatorNodeConfigType <: MediatorNodeConfigCommon
  type SequencerNodeConfigType <: SequencerNodeConfigCommon

  def allNodes: Map[InstanceName, LocalNodeConfig] =
    (participants: Map[InstanceName, LocalNodeConfig]) ++ sequencers ++ mediators

  /** all participants that this Canton process can operate or connect to
    *
    * participants are grouped by their local name
    */
  def participants: Map[InstanceName, ParticipantConfigType]

  /** Use `participants` instead!
    */
  def participantsByString: Map[String, ParticipantConfigType] = participants.map { case (n, c) =>
    n.unwrap -> c
  }

  def sequencers: Map[InstanceName, SequencerNodeConfigType]

  /** Use `sequencers` instead!
    */
  def sequencersByString: Map[String, SequencerNodeConfigType] = sequencers.map { case (n, c) =>
    n.unwrap -> c
  }

  def remoteSequencers: Map[InstanceName, RemoteSequencerConfig]

  /** Use `remoteSequencers` instead!
    */
  def remoteSequencersByString: Map[String, RemoteSequencerConfig] = remoteSequencers.map {
    case (n, c) =>
      n.unwrap -> c
  }

  def mediators: Map[InstanceName, MediatorNodeConfigType]

  /** Use `mediators` instead!
    */
  def mediatorsByString: Map[String, MediatorNodeConfigType] = mediators.map { case (n, c) =>
    n.unwrap -> c
  }

  def remoteMediators: Map[InstanceName, RemoteMediatorConfig]

  /** Use `remoteMediators` instead!
    */
  def remoteMediatorsByString: Map[String, RemoteMediatorConfig] = remoteMediators.map {
    case (n, c) =>
      n.unwrap -> c
  }

  /** all remotely running participants to which the console can connect and operate on */
  def remoteParticipants: Map[InstanceName, RemoteParticipantConfig]

  /** Use `remoteParticipants` instead!
    */
  def remoteParticipantsByString: Map[String, RemoteParticipantConfig] = remoteParticipants.map {
    case (n, c) =>
      n.unwrap -> c
  }

  /** determines how this Canton process can be monitored */
  def monitoring: MonitoringConfig

  /** per-environment parameters to control enabled features and set testing parameters */
  def parameters: CantonParameters

  /** control which features are enabled */
  def features: CantonFeatures

  /** dump config to string (without sensitive data) */
  def dumpString: String

  /** run a validation on the current config and return possible warning messages */
  def validate: Validated[NonEmpty[Seq[String]], Unit]

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
        excludeInfrastructureTransactions = participantParameters.excludeInfrastructureTransactions,
        engine = participantParameters.engine,
        journalGarbageCollectionDelay =
          participantParameters.journalGarbageCollectionDelay.toInternal,
        disableUpgradeValidation = participantParameters.disableUpgradeValidation,
        commandProgressTracking = participantParameters.commandProgressTracker,
        unsafeOnlinePartyReplication = participantParameters.unsafeOnlinePartyReplication,
        // TODO(i21341) Remove the flag before going to production
        experimentalEnableTopologyEvents = participantParameters.experimentalEnableTopologyEvents,
        enableExternalAuthorization = participantParameters.enableExternalAuthorization,
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

  /** Produces a message in the structure "da:admin-api=1,public-api=2;participant1:admin-api=3,ledger-api=4".
    * Helpful for diagnosing port already bound issues during tests.
    * Allows any config value to be be null (can happen with invalid configs or config stubbed in tests)
    */
  lazy val portDescription: String = mkPortDescription

  protected def mkPortDescription: String = {
    def participant(config: LocalParticipantConfig): Seq[String] =
      portDescriptionFromConfig(config)(Seq(("admin-api", _.adminApi), ("ledger-api", _.ledgerApi)))

    def sequencer(config: SequencerNodeConfigCommon): Seq[String] =
      portDescriptionFromConfig(config)(Seq(("admin-api", _.adminApi), ("public-api", _.publicApi)))

    def mediator(config: MediatorNodeConfigCommon): Seq[String] =
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

  // the great ux of pureconfig expects you to provide this ProductHint such that the created derivedReader fails on
  // unknown keys
  implicit def preventAllUnknownKeys[T]: ProductHint[T] = ProductHint[T](allowUnknownKeys = false)

  import com.daml.nonempty.NonEmptyUtil.instances.*
  import pureconfig.ConfigReader
  import pureconfig.generic.semiauto.*
  import pureconfig.module.cats.*

  implicit val communityStorageConfigTypeHint: FieldCoproductHint[StorageConfig] =
    CantonConfigUtil.lowerCaseStorageConfigType[StorageConfig]

  /** In the external config we use `port` for an optionally set port, while internally we store it as `internalPort` */
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

    lazy implicit final val initBaseIdentityConfigReader: ConfigReader[InitConfigBase.Identity] =
      deriveReader[InitConfigBase.Identity]
    lazy implicit final val stateConfigReader: ConfigReader[StateConfig] = deriveReader[StateConfig]
    lazy implicit final val initConfigReader: ConfigReader[InitConfig] =
      deriveReader[InitConfig]
        .enableNestedOpt("auto-init", _.copy(identity = None))

    lazy implicit final val nodeNameReader: ConfigReader[NodeIdentifierConfig] = {
      implicit val nodeNameConfigReader: ConfigReader[NodeIdentifierConfig.Config.type] =
        deriveReader[NodeIdentifierConfig.Config.type]
      implicit val nodeNameRandomReader: ConfigReader[NodeIdentifierConfig.Random.type] =
        deriveReader[NodeIdentifierConfig.Random.type]
      implicit val nodeNameExplicitReader: ConfigReader[NodeIdentifierConfig.Explicit] =
        deriveReader[NodeIdentifierConfig.Explicit]
      deriveReader[NodeIdentifierConfig]
    }
    lazy implicit final val participantInitConfigReader: ConfigReader[ParticipantInitConfig] = {
      implicit val ledgerApiParticipantInitConfigReader
          : ConfigReader[ParticipantLedgerApiInitConfig] =
        deriveReader[ParticipantLedgerApiInitConfig]

      deriveReader[ParticipantInitConfig]
        .enableNestedOpt("auto-init", _.copy(identity = None))
    }

    lazy implicit final val clientConfigReader: ConfigReader[ClientConfig] = {
      implicit val tlsClientConfigReader: ConfigReader[TlsClientConfig] =
        deriveReader[TlsClientConfig]
      deriveReader[ClientConfig]
    }
    lazy implicit final val remoteParticipantConfigReader: ConfigReader[RemoteParticipantConfig] =
      deriveReader[RemoteParticipantConfig]

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
    private lazy implicit final val sequencerConnectionConfigCertificateFileReader
        : ConfigReader[SequencerConnectionConfig.CertificateFile] =
      deriveReader[SequencerConnectionConfig.CertificateFile]
    lazy implicit final val sequencerConnectionConfigGrpcReader
        : ConfigReader[SequencerConnectionConfig.Grpc] =
      deriveReader[SequencerConnectionConfig.Grpc]
    lazy implicit final val sequencerConnectionConfigReader
        : ConfigReader[SequencerConnectionConfig] =
      deriveReader[SequencerConnectionConfig]
        // since the big majority of users will use GRPC, default to it so that they don't need to specify `type = grpc`
        .orElse(ConfigReader[SequencerConnectionConfig.Grpc])

    // NOTE: the below readers should move to community / enterprise
    lazy implicit final val communitySequencerConfigDatabaseReader
        : ConfigReader[CommunitySequencerConfig.Database] =
      deriveReader[CommunitySequencerConfig.Database]
    lazy implicit final val blockSequencerConfigReader: ConfigReader[BlockSequencerConfig] =
      deriveReader[BlockSequencerConfig]
    lazy implicit final val communityDatabaseSequencerReaderConfigReader
        : ConfigReader[CommunitySequencerReaderConfig] =
      deriveReader[CommunitySequencerReaderConfig]
    lazy implicit final val communitySequencerWriterCommitModeConfigReader
        : ConfigReader[CommitMode] =
      deriveEnumerationReader[CommitMode]
    lazy implicit final val communityNewDatabaseSequencerWriterConfigReader
        : ConfigReader[SequencerWriterConfig] =
      deriveReader[SequencerWriterConfig]
    lazy implicit final val bytesUnitReader: ConfigReader[BytesUnit] =
      BasicReaders.configMemorySizeReader.map(cms => BytesUnit(cms.toBytes))
    lazy implicit final val communityNewDatabaseSequencerWriterConfigHighThroughputReader
        : ConfigReader[SequencerWriterConfig.HighThroughput] =
      deriveReader[SequencerWriterConfig.HighThroughput]
    lazy implicit final val communityNewDatabaseSequencerWriterConfigLowLatencyReader
        : ConfigReader[SequencerWriterConfig.LowLatency] =
      deriveReader[SequencerWriterConfig.LowLatency]
    implicit val endpointReader: ConfigReader[Endpoint] =
      deriveReader[Endpoint]
    implicit val bftBlockOrdererBftTopologyReader: ConfigReader[BftBlockOrderer.BftNetwork] =
      deriveReader[BftBlockOrderer.BftNetwork]
    implicit val bftBlockOrdererConfigReader: ConfigReader[BftBlockOrderer.Config] =
      deriveReader[BftBlockOrderer.Config]
    implicit val communitySequencerConfigBftSequencerReader
        : ConfigReader[CommunitySequencerConfig.BftSequencer] =
      deriveReader[CommunitySequencerConfig.BftSequencer]

    lazy implicit final val sequencerPruningConfig
        : ConfigReader[DatabaseSequencerConfig.SequencerPruningConfig] =
      deriveReader[DatabaseSequencerConfig.SequencerPruningConfig]
    lazy implicit final val sequencerNodeInitConfigReader: ConfigReader[SequencerNodeInitConfig] =
      deriveReader[SequencerNodeInitConfig]
        .enableNestedOpt("auto-init", _.copy(identity = None))

    lazy implicit final val communitySequencerConfigReader: ConfigReader[CommunitySequencerConfig] =
      ConfigReader.fromCursor[CommunitySequencerConfig] { cur =>
        for {
          objCur <- cur.asObjectCursor
          sequencerType <- objCur.atKey("type").flatMap(_.asString) match {
            case Right("reference") =>
              // Allow using "reference" as the sequencer type in both community and enterprise
              Right("community-reference")
            case other => other
          }
          config <- (sequencerType match {
            case "database" =>
              communitySequencerConfigDatabaseReader.from(objCur.withoutKey("type"))
            case BftSequencerFactory.ShortName =>
              communitySequencerConfigBftSequencerReader.from(objCur.withoutKey("type"))
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
              } yield CommunitySequencerConfig.External(other, blockSequencerConfig, config)
          }): ConfigReader.Result[CommunitySequencerConfig]
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

    lazy implicit final val sequencerClientConfigReader: ConfigReader[SequencerClientConfig] = {
      implicit val authTokenManagerConfigReader: ConfigReader[AuthenticationTokenManagerConfig] =
        deriveReader[AuthenticationTokenManagerConfig]
      deriveReader[SequencerClientConfig]
    }

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
  }

  /** writers
    * @param confidential if set to true, confidential data which should not be shared for support purposes is blinded
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
    }

    implicit val sequencerTestingInterceptorWriter
        : ConfigWriter[DatabaseSequencerConfig.TestingInterceptor] =
      ConfigWriter.toString(_ => "None")

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

    lazy implicit final val initBaseIdentityConfigWriter: ConfigWriter[InitConfigBase.Identity] =
      deriveWriter[InitConfigBase.Identity]
    lazy implicit final val stateConfigWriter: ConfigWriter[StateConfig] = deriveWriter[StateConfig]
    lazy implicit final val initConfigWriter: ConfigWriter[InitConfig] =
      InitConfigBase.writerForSubtype(deriveWriter[InitConfig])

    lazy implicit final val nodeNameWriter: ConfigWriter[NodeIdentifierConfig] = {
      implicit val nodeNameConfigWriter: ConfigWriter[NodeIdentifierConfig.Config.type] =
        deriveWriter[NodeIdentifierConfig.Config.type]
      implicit val nodeNameRandomWriter: ConfigWriter[NodeIdentifierConfig.Random.type] =
        deriveWriter[NodeIdentifierConfig.Random.type]
      implicit val nodeNameExplicitWriter: ConfigWriter[NodeIdentifierConfig.Explicit] =
        deriveWriter[NodeIdentifierConfig.Explicit]
      deriveWriter[NodeIdentifierConfig]
    }

    lazy implicit final val participantInitConfigWriter: ConfigWriter[ParticipantInitConfig] = {
      implicit val ledgerApiParticipantInitConfigWriter
          : ConfigWriter[ParticipantLedgerApiInitConfig] =
        deriveWriter[ParticipantLedgerApiInitConfig]
      InitConfigBase.writerForSubtype(deriveWriter[ParticipantInitConfig])
    }

    lazy implicit final val clientConfigWriter: ConfigWriter[ClientConfig] = {
      implicit val tlsClientConfigWriter: ConfigWriter[TlsClientConfig] =
        deriveWriter[TlsClientConfig]
      deriveWriter[ClientConfig]
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
    lazy implicit final val sequencerConnectionConfigCertificateFileWriter
        : ConfigWriter[SequencerConnectionConfig.CertificateFile] =
      deriveWriter[SequencerConnectionConfig.CertificateFile]
    lazy implicit final val sequencerConnectionConfigGrpcWriter
        : ConfigWriter[SequencerConnectionConfig.Grpc] =
      deriveWriter[SequencerConnectionConfig.Grpc]
    lazy implicit final val sequencerConnectionConfigWriter
        : ConfigWriter[SequencerConnectionConfig] =
      deriveWriter[SequencerConnectionConfig]

    // NOTE: the below writers should move to community / enterprise
    lazy implicit final val communitySequencerConfigDatabaseWriter
        : ConfigWriter[CommunitySequencerConfig.Database] =
      deriveWriter[CommunitySequencerConfig.Database]
    lazy implicit final val blockSequencerConfigWriter: ConfigWriter[BlockSequencerConfig] =
      deriveWriter[BlockSequencerConfig]
    lazy implicit final val communityDatabaseSequencerReaderConfigWriter
        : ConfigWriter[CommunitySequencerReaderConfig] =
      deriveWriter[CommunitySequencerReaderConfig]
    lazy implicit final val communitySequencerWriterCommitModeConfigWriter
        : ConfigWriter[CommitMode] =
      deriveEnumerationWriter[CommitMode]
    lazy implicit final val communityDatabaseSequencerWriterConfigWriter
        : ConfigWriter[SequencerWriterConfig] =
      deriveWriter[SequencerWriterConfig]
    lazy implicit final val bytesUnitWriter: ConfigWriter[BytesUnit] =
      BasicWriters.configMemorySizeWriter.contramap[BytesUnit](b =>
        ConfigMemorySize.ofBytes(b.bytes)
      )
    lazy implicit final val communityDatabaseSequencerWriterConfigHighThroughputWriter
        : ConfigWriter[SequencerWriterConfig.HighThroughput] =
      deriveWriter[SequencerWriterConfig.HighThroughput]
    lazy implicit final val communityDatabaseSequencerWriterConfigLowLatencyWriter
        : ConfigWriter[SequencerWriterConfig.LowLatency] =
      deriveWriter[SequencerWriterConfig.LowLatency]
    implicit val endpointWriter: ConfigWriter[Endpoint] =
      deriveWriter[Endpoint]
    implicit val bftBlockOrdererBftTopologyWriter: ConfigWriter[BftBlockOrderer.BftNetwork] =
      deriveWriter[BftBlockOrderer.BftNetwork]
    implicit val bftBlockOrdererConfigWriter: ConfigWriter[BftBlockOrderer.Config] =
      deriveWriter[BftBlockOrderer.Config]
    implicit val communitySequencerConfigBftSequencerWriter
        : ConfigWriter[CommunitySequencerConfig.BftSequencer] =
      deriveWriter[CommunitySequencerConfig.BftSequencer]
    implicit val sequencerPruningConfigWriter
        : ConfigWriter[DatabaseSequencerConfig.SequencerPruningConfig] =
      deriveWriter[DatabaseSequencerConfig.SequencerPruningConfig]
    lazy implicit final val sequencerNodeInitConfigWriter: ConfigWriter[SequencerNodeInitConfig] =
      InitConfigBase.writerForSubtype(deriveWriter[SequencerNodeInitConfig])

    implicit def communitySequencerConfigWriter[C]: ConfigWriter[CommunitySequencerConfig] = {
      case dbSequencerConfig: CommunitySequencerConfig.Database =>
        import pureconfig.syntax.*
        Map("type" -> "database").toConfig
          .withFallback(communitySequencerConfigDatabaseWriter.to(dbSequencerConfig))

      case bftSequencerConfig: CommunitySequencerConfig.BftSequencer =>
        import pureconfig.syntax.*
        Map("type" -> BftSequencerFactory.ShortName).toConfig
          .withFallback(communitySequencerConfigBftSequencerWriter.to(bftSequencerConfig))

      case otherSequencerConfig: CommunitySequencerConfig.External =>
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

    lazy implicit final val sequencerClientConfigWriter: ConfigWriter[SequencerClientConfig] = {
      implicit val authTokenManagerConfigWriter: ConfigWriter[AuthenticationTokenManagerConfig] =
        deriveWriter[AuthenticationTokenManagerConfig]
      deriveWriter[SequencerClientConfig]
    }

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
  }

  /** Parses and merges the provided configuration files into a single [[com.typesafe.config.Config]].
    * Also loads and merges the default config (as defined by the Lightbend config library) with the provided
    * configuration files. Unless you know that you explicitly only want to use the provided configuration files,
    * use this method.
    * Any errors will be returned, but not logged.
    *
    * @param files config files to read, parse and merge
    * @return [[scala.Right]] [[com.typesafe.config.Config]] if parsing was successful.
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

  /** Parses and merges the provided configuration files into a single [[com.typesafe.config.Config]].
    * Does not load and merge the default config (as defined by the Lightbend config library) with the provided
    * configuration files. Only use this if you explicitly know that you don't want to load and merge the default config.
    *
    * @param files config files to read, parse and merge
    * @return [[scala.Right]] [[com.typesafe.config.Config]] if parsing was successful.
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
    go(config)
      .resolve()
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

  /** Merge a number of [[com.typesafe.config.Config]] instances into a single [[com.typesafe.config.Config]].
    * If the same key is included in multiple configurations, then the last definition has highest precedence.
    */
  def mergeConfigs(firstConfig: Config, otherConfigs: Seq[Config]): Config =
    otherConfigs.foldLeft(firstConfig)((combined, config) => config.withFallback(combined))

  /** Merge a number of [[com.typesafe.config.Config]] instances into a single [[com.typesafe.config.Config]].
    * If the same key is included in multiple configurations, then the last definition has highest precedence.
    */
  def mergeConfigs(configs: NonEmpty[Seq[Config]]): Config =
    mergeConfigs(configs.head1, configs.tail1)

  /** Parses the provided files to generate a [[com.typesafe.config.Config]], then attempts to load the
    * [[com.typesafe.config.Config]] based on the given ClassTag. Will return an error (but not log anything) if
    *  any steps fails.
    *
    * @param files config files to read, parse and merge
    * @return [[scala.Right]] of type `ConfClass` (e.g. [[CantonCommunityConfig]])) if parsing was successful.
    */
  def parseAndLoad[
      ConfClass <: CantonConfig & ConfigDefaults[DefaultPorts, ConfClass]: ClassTag: ConfigReader
  ](
      files: Seq[File]
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, ConfClass] =
    for {
      nonEmpty <- NonEmpty.from(files).toRight(NoConfigFiles.Error())
      parsedAndMerged <- parseAndMergeConfigs(nonEmpty)
      loaded <- loadAndValidate[ConfClass](parsedAndMerged)
    } yield loaded

  /** Parses the provided files to generate a [[com.typesafe.config.Config]], then attempts to load the
    * [[com.typesafe.config.Config]] based on the given ClassTag. Will log the error and exit with code 1, if any error
    * is encountered.
    *  *
    * @param files config files to read - must be a non-empty Seq
    * @throws java.lang.IllegalArgumentException if `files` is empty
    * @return [[scala.Right]] of type `ClassTag` (e.g. [[CantonCommunityConfig]])) if parsing was successful.
    */
  def parseAndLoadOrExit[
      ConfClass <: CantonConfig & ConfigDefaults[DefaultPorts, ConfClass]: ClassTag: ConfigReader
  ](files: Seq[File])(implicit
      elc: ErrorLoggingContext
  ): ConfClass = {
    val result = parseAndLoad[ConfClass](files)
    configOrExit(result)
  }

  /** Will load a case class configuration (defined by template args) from the configuration object.
    * Any configuration errors encountered will be returned (but not logged).
    *
    * @return [[scala.Right]] of type `CantonConfig` (e.g. [[CantonCommunityConfig]])) if parsing was successful.
    */
  def loadAndValidate[ConfClass <: CantonConfig & ConfigDefaults[
    DefaultPorts,
    ConfClass,
  ]: ClassTag: ConfigReader](
      config: Config
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, ConfClass] = {
    // config.resolve forces any substitutions to be resolved (typically referenced environment variables or system properties).
    // this normally would happen by default during ConfigFactory.load(),
    // however we have to manually as we've merged in individual files.
    val result = Either.catchOnly[UnresolvedSubstitution](config.resolve())
    result match {
      case Right(resolvedConfig) =>
        loadRawConfig[ConfClass](resolvedConfig)
          .flatMap { conf =>
            val confWithDefaults = conf.withDefaults(new DefaultPorts())
            confWithDefaults.validate.toEither
              .map(_ => confWithDefaults)
              .leftMap(causes => ConfigErrors.ValidationError.Error(causes.toList))
          }
      case Left(substitutionError) => Left(SubstitutionError.Error(Seq(substitutionError)))
    }
  }

  /** Will load a case class configuration (defined by template args) from the configuration object.
    * If any configuration errors are encountered, they will be logged and the thread will exit with code 1.
    *
    * @return [[scala.Right]] of type `ClassTag` (e.g. [[CantonCommunityConfig]])) if parsing was successful.
    */
  def loadOrExit[
      ConfClass <: CantonConfig & ConfigDefaults[DefaultPorts, ConfClass]: ClassTag: ConfigReader
  ](
      config: Config
  )(implicit elc: ErrorLoggingContext): ConfClass =
    loadAndValidate[ConfClass](config).valueOr(_ => sys.exit(1))

  private[config] def loadRawConfig[ConfClass <: CantonConfig: ClassTag: ConfigReader](
      rawConfig: Config
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, ConfClass] =
    pureconfig.ConfigSource
      .fromConfig(rawConfig)
      .at("canton")
      .load[ConfClass]
      .leftMap(failures => GenericConfigError.Error(ConfigErrors.getMessage[ConfClass](failures)))

  lazy val defaultConfigRenderer: ConfigRenderOptions =
    ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false)
}
