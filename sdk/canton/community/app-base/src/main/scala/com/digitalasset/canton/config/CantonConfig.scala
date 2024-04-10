// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

////////////////////////////////////////////////////////
// DO NOT USE INTELLIJ OPTIMIZE-IMPORT AS IT WILL REMOVE
// SOME OF THE IMPLICIT IMPORTS NECESSARY TO COMPILE
////////////////////////////////////////////////////////

import cats.Order
import cats.data.Validated
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.jwt.JwtTimestampLeeway
import com.daml.metrics.HistogramDefinition
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.{
  InvalidLengthString,
  defaultMaxLength,
}
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
import com.digitalasset.canton.config.RequireTypes.*
import com.digitalasset.canton.console.{AmmoniteConsoleConfig, FeatureFlag}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.domain.block.{SequencerDriver, SequencerDriverFactory}
import com.digitalasset.canton.domain.config.*
import com.digitalasset.canton.domain.mediator.{
  MediatorNodeConfigCommon,
  MediatorNodeParameterConfig,
  MediatorNodeParameters,
  RemoteMediatorConfig,
}
import com.digitalasset.canton.domain.sequencing.config.{
  RemoteSequencerConfig,
  SequencerNodeConfigCommon,
  SequencerNodeInitXConfig,
  SequencerNodeParameterConfig,
  SequencerNodeParameters,
}
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.DriverBlockSequencerFactory
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.http.{HttpApiConfig, StaticContentConfig, WebsocketConfig}
import com.digitalasset.canton.ledger.runner.common.PureConfigReaderWriter.Secure.{
  commandConfigurationConvert,
  dbConfigPostgresDataSourceConfigConvert,
  identityProviderManagementConfigConvert,
  indexServiceConfigConvert,
  indexerConfigConvert,
  userManagementServiceConfigConvert,
}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.AdminWorkflowConfig
import com.digitalasset.canton.participant.config.ParticipantInitConfig.ParticipantLedgerApiInitConfig
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfig
import com.digitalasset.canton.platform.indexer.PackageMetadataViewConfig
import com.digitalasset.canton.protocol.AcsCommitmentsCatchUpConfig
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.pureconfigutils.HttpServerConfig
import com.digitalasset.canton.pureconfigutils.SharedConfigReaders.catchConvertError
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.EnrichedDurations.RichNonNegativeFiniteDurationConfig
import com.digitalasset.canton.tracing.TracingConfig
import com.typesafe.config.ConfigException.UnresolvedSubstitution
import com.typesafe.config.{
  Config,
  ConfigException,
  ConfigFactory,
  ConfigList,
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
import java.nio.file.{Path, Paths}
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
  * @param delayLoggingThreshold         Logs a warning message once the sequencer client falls behind in processing messages from the sequencer (based on the sequencing timestamp).
  *
  * @param tracing       Tracing configuration
  * @param logMessagePayloads  Determines whether message payloads (as well as metadata) sent through GRPC are logged.
  * @param logQueryCost Determines whether to log the 15 most expensive db queries
  * @param logSlowFutures Whether we should active log slow futures (where instructed)
  * @param dumpNumRollingLogFiles How many of the rolling log files shold be included in the remote dump. Default is 0.
  */
final case class MonitoringConfig(
    deadlockDetection: DeadlockDetectionConfig = DeadlockDetectionConfig(),
    metrics: MetricsConfig = MetricsConfig(),
    // TODO(i9014) move into logging
    delayLoggingThreshold: NonNegativeFiniteDuration =
      MonitoringConfig.defaultDelayLoggingThreshold,
    tracing: TracingConfig = TracingConfig(),
    // TODO(i9014) rename to queries
    logQueryCost: Option[QueryCostMonitoringConfig] = None,
    // TODO(i9014) move into logging
    logSlowFutures: Boolean = false,
    logging: LoggingConfig = LoggingConfig(),
    dumpNumRollingLogFiles: NonNegativeInt = MonitoringConfig.defaultDumpNumRollingLogFiles,
) extends LazyLogging

object MonitoringConfig {
  private val defaultDelayLoggingThreshold = NonNegativeFiniteDuration.ofSeconds(20)
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
    * @param skew    maximum simulated clock skew (0)
    *                If positive, Canton nodes will use a WallClock, but the time of the wall clocks
    *                will be shifted by a random number between `-simulateMaxClockSkewMillis` and
    *                `simulateMaxClockSkewMillis`. The clocks will never move backwards.
    */
  final case class WallClock(
      skew: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(0)
  ) extends ClockConfig

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
    unauthenticatedMembers: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(1),
)

/** Parameters for testing Canton. Use default values in a production environment.
  *
  * @param enableAdditionalConsistencyChecks if true, run additional consistency checks. This will degrade performance.
  * @param manualStart  If set to true, the nodes have to be manually started via console (default false)
  * @param startupParallelism Start up to N nodes in parallel (default is num-threads)
  * @param nonStandardConfig don't fail config validation on non-standard configuration settings
  * @param devVersionSupport If true, allow domain nodes to use unstable protocol versions and participant nodes to connect to such domains
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
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    devVersionSupport: Boolean = true,
    portsFile: Option[String] = None,
    timeouts: TimeoutSettings = TimeoutSettings(),
    retentionPeriodDefaults: RetentionPeriodDefaults = RetentionPeriodDefaults(),
    console: AmmoniteConsoleConfig = AmmoniteConsoleConfig(),
    exitOnFatalFailures: Boolean = true,
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
  def featureFlags: Set[FeatureFlag] = {
    (Seq(FeatureFlag.Stable)
      ++ (if (enableTestingCommands) Seq(FeatureFlag.Testing) else Seq())
      ++ (if (enablePreviewCommands) Seq(FeatureFlag.Preview) else Seq())
      ++ (if (enableRepairCommands) Seq(FeatureFlag.Repair) else Seq())).toSet
  }
}

/** Root configuration parameters for a single Canton process. */
trait CantonConfig {

  type ParticipantConfigType <: LocalParticipantConfig & ConfigDefaults[
    DefaultPorts,
    ParticipantConfigType,
  ]
  type MediatorNodeXConfigType <: MediatorNodeConfigCommon
  type SequencerNodeXConfigType <: SequencerNodeConfigCommon

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

  def sequencers: Map[InstanceName, SequencerNodeXConfigType]

  /** Use `sequencers` instead!
    */
  def sequencersByString: Map[String, SequencerNodeXConfigType] = sequencers.map { case (n, c) =>
    n.unwrap -> c
  }

  def remoteSequencers: Map[InstanceName, RemoteSequencerConfig]

  /** Use `remoteSequencers` instead!
    */
  def remoteSequencersByString: Map[String, RemoteSequencerConfig] = remoteSequencers.map {
    case (n, c) =>
      n.unwrap -> c
  }

  def mediators: Map[InstanceName, MediatorNodeXConfigType]

  /** Use `mediators` instead!
    */
  def mediatorsByString: Map[String, MediatorNodeXConfigType] = mediators.map { case (n, c) =>
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
        partyChangeNotification = participantParameters.partyChangeNotification,
        adminWorkflow = participantParameters.adminWorkflow,
        maxUnzippedDarSize = participantParameters.maxUnzippedDarSize,
        stores = participantParameters.stores,
        transferTimeProofFreshnessProportion =
          participantParameters.transferTimeProofFreshnessProportion,
        protocolConfig = ParticipantProtocolConfig(
          minimumProtocolVersion = participantParameters.minimumProtocolVersion.map(_.unwrap),
          devVersionSupport = participantParameters.devVersionSupport,
          dontWarnOnDeprecatedPV = participantParameters.dontWarnOnDeprecatedPV,
        ),
        ledgerApiServerParameters = participantParameters.ledgerApiServer,
        excludeInfrastructureTransactions = participantParameters.excludeInfrastructureTransactions,
        enableEngineStackTrace = participantParameters.enableEngineStackTraces,
        iterationsBetweenInterruptions = participantParameters.iterationsBetweenInterruptions,
        journalGarbageCollectionDelay =
          participantParameters.journalGarbageCollectionDelay.toInternal,
        disableUpgradeValidation = participantParameters.disableUpgradeValidation,
        allowForUnauthenticatedContractIds =
          participantParameters.allowForUnauthenticatedContractIds,
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

  private lazy val sequencerNodeParametersX_ : Map[InstanceName, SequencerNodeParameters] =
    sequencers.fmap { sequencerNodeXConfig =>
      SequencerNodeParameters(
        general = CantonNodeParameterConverter.general(this, sequencerNodeXConfig),
        protocol = CantonNodeParameterConverter.protocol(this, sequencerNodeXConfig.parameters),
        maxBurstFactor = sequencerNodeXConfig.parameters.maxBurstFactor,
      )
    }

  private[canton] def sequencerNodeParametersX(name: InstanceName): SequencerNodeParameters =
    nodeParametersFor(sequencerNodeParametersX_, "sequencer-x", name)

  private[canton] def sequencerNodeParametersByStringX(name: String): SequencerNodeParameters =
    sequencerNodeParametersX(InstanceName.tryCreate(name))

  private lazy val mediatorNodeParametersX_ : Map[InstanceName, MediatorNodeParameters] =
    mediators.fmap { mediatorNodeConfig =>
      MediatorNodeParameters(
        general = CantonNodeParameterConverter.general(this, mediatorNodeConfig),
        protocol = CantonNodeParameterConverter.protocol(this, mediatorNodeConfig.parameters),
      )
    }

  private[canton] def mediatorNodeParametersX(name: InstanceName): MediatorNodeParameters =
    nodeParametersFor(mediatorNodeParametersX_, "mediator-x", name)

  private[canton] def mediatorNodeParametersByStringX(name: String): MediatorNodeParameters =
    mediatorNodeParametersX(InstanceName.tryCreate(name))

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
  import com.digitalasset.canton.time.EnrichedDurations.*

  def general(parent: CantonConfig, node: LocalNodeConfig): CantonNodeParameters.General = {
    CantonNodeParameters.General.Impl(
      tracing = parent.monitoring.tracing,
      delayLoggingThreshold = parent.monitoring.delayLoggingThreshold.toInternal,
      logQueryCost = parent.monitoring.logQueryCost,
      loggingConfig = parent.monitoring.logging,
      enableAdditionalConsistencyChecks = parent.parameters.enableAdditionalConsistencyChecks,
      enablePreviewFeatures = parent.features.enablePreviewCommands,
      processingTimeouts = parent.parameters.timeouts.processing,
      sequencerClient = node.sequencerClient,
      cachingConfigs = node.parameters.caching,
      batchingConfig = node.parameters.batching,
      nonStandardConfig = parent.parameters.nonStandardConfig,
      dbMigrateAndStart = node.storage.parameters.migrateAndStart,
      useNewTrafficControl = node.parameters.useNewTrafficControl,
      exitOnFatalFailures = parent.parameters.exitOnFatalFailures,
      useUnifiedSequencer = node.parameters.useUnifiedSequencer,
    )
  }

  def protocol(parent: CantonConfig, config: ProtocolConfig): CantonNodeParameters.Protocol =
    CantonNodeParameters.Protocol.Impl(
      devVersionSupport = parent.parameters.devVersionSupport || config.devVersionSupport,
      dontWarnOnDeprecatedPV = config.dontWarnOnDeprecatedPV,
    )

}

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
object CantonConfig {

  implicit def preventAllUnknownKeys[T]: ProductHint[T] = ProductHint[T](allowUnknownKeys = false)

  import com.daml.nonempty.NonEmptyUtil.instances.*
  import pureconfig.ConfigReader
  import pureconfig.generic.semiauto.*
  import pureconfig.module.cats.*

  implicit val communityStorageConfigTypeHint: FieldCoproductHint[CommunityStorageConfig] =
    CantonConfigUtil.lowerCaseStorageConfigType[CommunityStorageConfig]

  /** In the external config we use `port` for an optionally set port, while internally we store it as `internalPort` */
  implicit def serverConfigProductHint[SC <: ServerConfig]: ProductHint[SC] = ProductHint[SC](
    fieldMapping = ConfigFieldMapping(CamelCase, KebabCase).withOverrides("internalPort" -> "port"),
    allowUnknownKeys = false,
  )

  object ConfigReaders {
    import CantonConfigUtil.*

    lazy implicit val lengthLimitedStringReader: ConfigReader[LengthLimitedString] = {
      ConfigReader.fromString[LengthLimitedString] { str =>
        Either.cond(
          str.nonEmpty && str.length <= defaultMaxLength,
          new LengthLimitedStringVar(str, defaultMaxLength)(),
          InvalidLengthString(str),
        )
      }
    }

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

    implicit val maxRequestSizeReader: ConfigReader[MaxRequestSize] =
      NonNegativeNumeric.nonNegativeNumericReader[Int].map(MaxRequestSize)

    implicit val sequencerTestingInterceptorReader
        : ConfigReader[DatabaseSequencerConfig.TestingInterceptor] =
      (_: ConfigCursor) =>
        sys.error("Sequencer testing interceptor cannot be created from pureconfig")

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
    implicit val tracingConfigAlwaysOnSamplerReader: ConfigReader[TracingConfig.Sampler.AlwaysOn] =
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
              s"Seeding is neither ${Seeding.Strong.name}, ${Seeding.Weak.name}, nor ${Seeding.Static.name}: ${unknownSeeding}",
            )
          )
      }

    /** Using semi-automatic derivation over automatic derivation to save compile time
      * NOTE: the order of the declaration matters ... if you see errors such as
      * could not find Lazy implicit value of type pureconfig.generic.DerivedConfigReader[..]
      * then it means that the orders of the reader / writers is wrong.
      *
      * The following python script is very helpful to create the semi-automatic writer and readers
      *
      * import fileinput
      * for line in fileinput.input():
      * t = line.strip()
      * if len(t) > 0:
      * a = (t[0].lower() + t[1:]).replace(".","")
      * print("""
      * implicit val %sReader : ConfigReader[%s] = deriveReader[%s]
      * implicit val %sWriter : ConfigWriter[%s] = deriveWriter[%s]
      * """ % (a, t, t, a, t, t))
      */
    lazy implicit val tlsClientCertificateReader: ConfigReader[TlsClientCertificate] =
      deriveReader[TlsClientCertificate]
    lazy implicit val serverAuthRequirementConfigNoneReader
        : ConfigReader[ServerAuthRequirementConfig.None.type] =
      deriveReader[ServerAuthRequirementConfig.None.type]
    lazy implicit val serverAuthRequirementConfigOptionalReader
        : ConfigReader[ServerAuthRequirementConfig.Optional.type] =
      deriveReader[ServerAuthRequirementConfig.Optional.type]
    lazy implicit val serverAuthRequirementConfigRequireReader
        : ConfigReader[ServerAuthRequirementConfig.Require] =
      deriveReader[ServerAuthRequirementConfig.Require]
    lazy implicit val serverAuthRequirementConfigReader: ConfigReader[ServerAuthRequirementConfig] =
      deriveReader[ServerAuthRequirementConfig]
    lazy implicit val keepAliveClientConfigReader: ConfigReader[KeepAliveClientConfig] =
      deriveReader[KeepAliveClientConfig]
    lazy implicit val keepAliveServerConfigReader: ConfigReader[KeepAliveServerConfig] =
      deriveReader[KeepAliveServerConfig]
    lazy implicit val tlsServerConfigReader: ConfigReader[TlsServerConfig] =
      deriveReader[TlsServerConfig]
    lazy implicit val tlsClientConfigReader: ConfigReader[TlsClientConfig] =
      deriveReader[TlsClientConfig]
    lazy implicit val initBaseIdentityConfigReader: ConfigReader[InitConfigBase.Identity] =
      deriveReader[InitConfigBase.Identity]
    lazy implicit val initConfigReader: ConfigReader[InitConfig] = deriveReader[InitConfig]
      .enableNestedOpt("auto-init", _.copy(identity = None))
    lazy implicit val ledgerApiParticipantInitConfigReader
        : ConfigReader[ParticipantLedgerApiInitConfig] =
      deriveReader[ParticipantLedgerApiInitConfig]
    lazy implicit val nodeNameConfigReader: ConfigReader[NodeIdentifierConfig.Config.type] =
      deriveReader[NodeIdentifierConfig.Config.type]
    lazy implicit val nodeNameRandomReader: ConfigReader[NodeIdentifierConfig.Random.type] =
      deriveReader[NodeIdentifierConfig.Random.type]
    lazy implicit val nodeNameExplicitReader: ConfigReader[NodeIdentifierConfig.Explicit] =
      deriveReader[NodeIdentifierConfig.Explicit]
    lazy implicit val nodeNameReader: ConfigReader[NodeIdentifierConfig] =
      deriveReader[NodeIdentifierConfig]
    lazy implicit val participantInitConfigReader: ConfigReader[ParticipantInitConfig] =
      deriveReader[ParticipantInitConfig]
        .enableNestedOpt("auto-init", _.copy(identity = None))
    lazy implicit val httpHealthServerConfigReader: ConfigReader[HttpHealthServerConfig] =
      deriveReader[HttpHealthServerConfig]
    implicit val grpcHealthServerConfigReader: ConfigReader[GrpcHealthServerConfig] =
      deriveReader[GrpcHealthServerConfig]
    lazy implicit val communityCryptoProviderReader: ConfigReader[CommunityCryptoProvider] =
      deriveEnumerationReader[CommunityCryptoProvider]
    lazy implicit val cryptoSigningKeySchemeReader: ConfigReader[SigningKeyScheme] =
      deriveEnumerationReader[SigningKeyScheme]
    lazy implicit val cryptoEncryptionKeySchemeReader: ConfigReader[EncryptionKeyScheme] =
      deriveEnumerationReader[EncryptionKeyScheme]
    lazy implicit val cryptoSymmetricKeySchemeReader: ConfigReader[SymmetricKeyScheme] =
      deriveEnumerationReader[SymmetricKeyScheme]
    lazy implicit val cryptoHashAlgorithmReader: ConfigReader[HashAlgorithm] =
      deriveEnumerationReader[HashAlgorithm]
    lazy implicit val cryptoPbkdfSchemeReader: ConfigReader[PbkdfScheme] =
      deriveEnumerationReader[PbkdfScheme]
    lazy implicit val cryptoKeyFormatReader: ConfigReader[CryptoKeyFormat] =
      deriveEnumerationReader[CryptoKeyFormat]
    implicit def cryptoSchemeConfig[S: ConfigReader: Order]: ConfigReader[CryptoSchemeConfig[S]] =
      deriveReader[CryptoSchemeConfig[S]]
    lazy implicit val communityCryptoReader: ConfigReader[CommunityCryptoConfig] =
      deriveReader[CommunityCryptoConfig]
    lazy implicit val clientConfigReader: ConfigReader[ClientConfig] = deriveReader[ClientConfig]
    lazy implicit val remoteParticipantConfigReader: ConfigReader[RemoteParticipantConfig] =
      deriveReader[RemoteParticipantConfig]
    lazy implicit val batchingReader: ConfigReader[BatchingConfig] =
      deriveReader[BatchingConfig]
    lazy implicit val connectionAllocationReader: ConfigReader[ConnectionAllocation] =
      deriveReader[ConnectionAllocation]
    lazy implicit val dbParamsReader: ConfigReader[DbParametersConfig] =
      deriveReader[DbParametersConfig]
    lazy implicit val memoryReader: ConfigReader[CommunityStorageConfig.Memory] =
      deriveReader[CommunityStorageConfig.Memory]
    lazy implicit val h2Reader: ConfigReader[CommunityDbConfig.H2] =
      deriveReader[CommunityDbConfig.H2]
    lazy implicit val postgresReader: ConfigReader[CommunityDbConfig.Postgres] =
      deriveReader[CommunityDbConfig.Postgres]
    lazy implicit val dbConfigReader: ConfigReader[CommunityDbConfig] =
      deriveReader[CommunityDbConfig]
    lazy implicit val nodeMonitoringConfigReader: ConfigReader[NodeMonitoringConfig] =
      deriveReader[NodeMonitoringConfig]
    lazy implicit val communityStorageConfigReader: ConfigReader[CommunityStorageConfig] =
      deriveReader[CommunityStorageConfig]
    lazy implicit val monotonicTimeReader
        : ConfigReader[TestingTimeServiceConfig.MonotonicTime.type] =
      deriveReader[TestingTimeServiceConfig.MonotonicTime.type]
    lazy implicit val testingTimeServiceConfigReader: ConfigReader[TestingTimeServiceConfig] =
      deriveReader[TestingTimeServiceConfig]

    lazy implicit val communityAdminServerReader: ConfigReader[CommunityAdminServerConfig] =
      deriveReader[CommunityAdminServerConfig]
    lazy implicit val tlsBaseServerConfigReader: ConfigReader[TlsBaseServerConfig] =
      deriveReader[TlsBaseServerConfig]
    lazy implicit val communityPublicServerConfigReader: ConfigReader[CommunityPublicServerConfig] =
      deriveReader[CommunityPublicServerConfig]
    lazy implicit val clockConfigRemoteClockReader: ConfigReader[ClockConfig.RemoteClock] =
      deriveReader[ClockConfig.RemoteClock]
    lazy implicit val clockConfigWallClockReader: ConfigReader[ClockConfig.WallClock] =
      deriveReader[ClockConfig.WallClock]
    lazy implicit val clockConfigSimClockReader: ConfigReader[ClockConfig.SimClock.type] =
      deriveReader[ClockConfig.SimClock.type]
    lazy implicit val clockConfigReader: ConfigReader[ClockConfig] = deriveReader[ClockConfig]
    lazy implicit val jwtTimestampLeewayConfigReader: ConfigReader[JwtTimestampLeeway] =
      deriveReader[JwtTimestampLeeway]
    lazy implicit val authServiceConfigUnsafeJwtHmac256Reader
        : ConfigReader[AuthServiceConfig.UnsafeJwtHmac256] =
      deriveReader[AuthServiceConfig.UnsafeJwtHmac256]
    lazy implicit val authServiceConfigJwtEs256CrtReader
        : ConfigReader[AuthServiceConfig.JwtEs256Crt] =
      deriveReader[AuthServiceConfig.JwtEs256Crt]
    lazy implicit val authServiceConfigJwtEs512CrtReader
        : ConfigReader[AuthServiceConfig.JwtEs512Crt] =
      deriveReader[AuthServiceConfig.JwtEs512Crt]
    lazy implicit val authServiceConfigJwtRs256CrtReader
        : ConfigReader[AuthServiceConfig.JwtRs256Crt] =
      deriveReader[AuthServiceConfig.JwtRs256Crt]
    lazy implicit val authServiceConfigJwtRs256JwksReader
        : ConfigReader[AuthServiceConfig.JwtRs256Jwks] =
      deriveReader[AuthServiceConfig.JwtRs256Jwks]
    lazy implicit val authServiceConfigWildcardReader
        : ConfigReader[AuthServiceConfig.Wildcard.type] =
      deriveReader[AuthServiceConfig.Wildcard.type]
    lazy implicit val authServiceConfigReader: ConfigReader[AuthServiceConfig] =
      deriveReader[AuthServiceConfig]
    lazy implicit val rateLimitConfigReader: ConfigReader[RateLimitingConfig] =
      deriveReader[RateLimitingConfig]
    lazy implicit val ledgerApiServerConfigReader: ConfigReader[LedgerApiServerConfig] =
      deriveReader[LedgerApiServerConfig]

    implicit val throttleModeCfgReader: ConfigReader[ThrottleMode] =
      ConfigReader.fromString[ThrottleMode](catchConvertError { s =>
        s.toLowerCase() match {
          case "enforcing" => Right(ThrottleMode.Enforcing)
          case "shaping" => Right(ThrottleMode.Shaping)
          case _ => Left("not one of 'shaping' or 'enforcing'")
        }
      })
    lazy implicit val portFileReader: ConfigReader[Path] =
      ConfigReader.fromString[Path](catchConvertError { s =>
        scala.util.Try(Paths.get(s)).toEither.left.map(_.getMessage)
      })
    lazy implicit val staticContentConfigReader: ConfigReader[StaticContentConfig] =
      deriveReader[StaticContentConfig]
    lazy implicit val wsConfigReader: ConfigReader[WebsocketConfig] =
      deriveReader[WebsocketConfig]

    lazy implicit val httpServerConfigReader: ConfigReader[HttpServerConfig] =
      deriveReader[HttpServerConfig]
    lazy implicit val httpApiServerConfigReader: ConfigReader[HttpApiConfig] =
      deriveReader[HttpApiConfig]
    lazy implicit val activeContractsServiceConfigReader
        : ConfigReader[ActiveContractsServiceStreamsConfig] =
      deriveReader[ActiveContractsServiceStreamsConfig]
    lazy implicit val packageMetadataViewConfigReader: ConfigReader[PackageMetadataViewConfig] =
      deriveReader[PackageMetadataViewConfig]
    lazy implicit val topologyXConfigReader: ConfigReader[TopologyConfig] =
      deriveReader[TopologyConfig]
    lazy implicit val sequencerConnectionConfigCertificateFileReader
        : ConfigReader[SequencerConnectionConfig.CertificateFile] =
      deriveReader[SequencerConnectionConfig.CertificateFile]
    lazy implicit val sequencerConnectionConfigCertificateStringReader
        : ConfigReader[SequencerConnectionConfig.CertificateString] =
      deriveReader[SequencerConnectionConfig.CertificateString]
    lazy implicit val sequencerConnectionConfigCertificateConfigReader
        : ConfigReader[SequencerConnectionConfig.CertificateConfig] =
      deriveReader[SequencerConnectionConfig.CertificateConfig]
    lazy implicit val sequencerConnectionConfigGrpcReader
        : ConfigReader[SequencerConnectionConfig.Grpc] =
      deriveReader[SequencerConnectionConfig.Grpc]
    lazy implicit val sequencerConnectionConfigReader: ConfigReader[SequencerConnectionConfig] =
      deriveReader[SequencerConnectionConfig]
        // since the big majority of users will use GRPC, default to it so that they don't need to specify `type = grpc`
        .orElse(ConfigReader[SequencerConnectionConfig.Grpc])
    lazy implicit val communitySequencerConfigDatabaseReader
        : ConfigReader[CommunitySequencerConfig.Database] =
      deriveReader[CommunitySequencerConfig.Database]
    lazy implicit val communityDatabaseSequencerReaderConfigReader
        : ConfigReader[CommunitySequencerReaderConfig] =
      deriveReader[CommunitySequencerReaderConfig]
    lazy implicit val communitySequencerWriterCommitModeConfigReader: ConfigReader[CommitMode] =
      deriveEnumerationReader[CommitMode]
    lazy implicit val communityNewDatabaseSequencerWriterConfigReader
        : ConfigReader[SequencerWriterConfig] =
      deriveReader[SequencerWriterConfig]
    lazy implicit val communityNewDatabaseSequencerWriterConfigHighThroughputReader
        : ConfigReader[SequencerWriterConfig.HighThroughput] =
      deriveReader[SequencerWriterConfig.HighThroughput]
    lazy implicit val communityNewDatabaseSequencerWriterConfigLowLatencyReader
        : ConfigReader[SequencerWriterConfig.LowLatency] =
      deriveReader[SequencerWriterConfig.LowLatency]
    lazy implicit val sequencerNodeInitXConfigReader: ConfigReader[SequencerNodeInitXConfig] =
      deriveReader[SequencerNodeInitXConfig]
        .enableNestedOpt("auto-init", _.copy(identity = None))

    lazy implicit val communitySequencerConfigReader: ConfigReader[CommunitySequencerConfig] =
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
            case other =>
              objCur.atKey("config").map(CommunitySequencerConfig.External(other, _, None))
          }): ConfigReader.Result[CommunitySequencerConfig]
        } yield config
      }

    lazy implicit val sequencerNodeParametersConfigReader
        : ConfigReader[SequencerNodeParameterConfig] =
      deriveReader[SequencerNodeParameterConfig]
    lazy implicit val SequencerHealthConfigReader: ConfigReader[SequencerHealthConfig] =
      deriveReader[SequencerHealthConfig]
    lazy implicit val remoteSequencerConfigReader: ConfigReader[RemoteSequencerConfig] =
      deriveReader[RemoteSequencerConfig]
    lazy implicit val mediatorNodeParameterConfigReader: ConfigReader[MediatorNodeParameterConfig] =
      deriveReader[MediatorNodeParameterConfig]
    lazy implicit val remoteMediatorConfigReader: ConfigReader[RemoteMediatorConfig] =
      deriveReader[RemoteMediatorConfig]
    lazy implicit val domainParametersConfigReader: ConfigReader[DomainParametersConfig] =
      deriveReader[DomainParametersConfig]
    lazy implicit val acsCommitmentsCatchUpConfigReader: ConfigReader[AcsCommitmentsCatchUpConfig] =
      deriveReader[AcsCommitmentsCatchUpConfig]
    lazy implicit val deadlockDetectionConfigReader: ConfigReader[DeadlockDetectionConfig] =
      deriveReader[DeadlockDetectionConfig]
    lazy implicit val sequencerTrafficConfigReader: ConfigReader[SequencerTrafficConfig] =
      deriveReader[SequencerTrafficConfig]
    lazy implicit val metricsFilterConfigReader: ConfigReader[MetricsConfig.MetricsFilterConfig] =
      deriveReader[MetricsConfig.MetricsFilterConfig]
    lazy implicit val metricsConfigPrometheusReader
        : ConfigReader[MetricsReporterConfig.Prometheus] =
      deriveReader[MetricsReporterConfig.Prometheus]
    lazy implicit val metricsConfigCsvReader: ConfigReader[MetricsReporterConfig.Csv] =
      deriveReader[MetricsReporterConfig.Csv]
    lazy implicit val metricsConfigLoggingReader: ConfigReader[MetricsReporterConfig.Logging] =
      deriveReader[MetricsReporterConfig.Logging]
    lazy implicit val metricsConfigJvmConfigReader: ConfigReader[MetricsConfig.JvmMetrics] =
      deriveReader[MetricsConfig.JvmMetrics]
    lazy implicit val metricsReporterConfigReader: ConfigReader[MetricsReporterConfig] =
      deriveReader[MetricsReporterConfig]
    lazy implicit val histogramDefinitionConfigReader: ConfigReader[HistogramDefinition] =
      deriveReader[HistogramDefinition]
    lazy implicit val metricsConfigReader: ConfigReader[MetricsConfig] = deriveReader[MetricsConfig]
    lazy implicit val queryCostMonitoringConfigReader: ConfigReader[QueryCostMonitoringConfig] =
      deriveReader[QueryCostMonitoringConfig]
    lazy implicit val apiLoggingConfigReader: ConfigReader[ApiLoggingConfig] =
      deriveReader[ApiLoggingConfig]
    lazy implicit val loggingConfigReader: ConfigReader[LoggingConfig] =
      deriveReader[LoggingConfig]
    implicit lazy val monitoringConfigReader: ConfigReader[MonitoringConfig] =
      deriveReader[MonitoringConfig]
    lazy implicit val consoleCommandTimeoutReader: ConfigReader[ConsoleCommandTimeout] =
      deriveReader[ConsoleCommandTimeout]
    lazy implicit val processingTimeoutReader: ConfigReader[ProcessingTimeout] =
      deriveReader[ProcessingTimeout]
    lazy implicit val timeoutSettingsReader: ConfigReader[TimeoutSettings] =
      deriveReader[TimeoutSettings]
    lazy implicit val partyNotificationConfigViaDomainReader
        : ConfigReader[PartyNotificationConfig.ViaDomain.type] =
      deriveReader[PartyNotificationConfig.ViaDomain.type]
    lazy implicit val partyNotificationConfigEagerReader
        : ConfigReader[PartyNotificationConfig.Eager.type] =
      deriveReader[PartyNotificationConfig.Eager.type]
    lazy implicit val partyNotificationConfigReader: ConfigReader[PartyNotificationConfig] =
      deriveReader[PartyNotificationConfig]
    lazy implicit val cacheConfigReader: ConfigReader[CacheConfig] =
      deriveReader[CacheConfig]
    lazy implicit val cacheConfigWithTimeoutReader: ConfigReader[CacheConfigWithTimeout] =
      deriveReader[CacheConfigWithTimeout]
    lazy implicit val sessionKeyCacheConfigReader: ConfigReader[SessionKeyCacheConfig] =
      deriveReader[SessionKeyCacheConfig]
    lazy implicit val cachingConfigsReader: ConfigReader[CachingConfigs] =
      deriveReader[CachingConfigs]
    lazy implicit val adminWorkflowConfigReader: ConfigReader[AdminWorkflowConfig] =
      deriveReader[AdminWorkflowConfig]
    lazy implicit val journalPruningConfigReader: ConfigReader[JournalPruningConfig] =
      deriveReader[JournalPruningConfig]
    lazy implicit val participantStoreConfigReader: ConfigReader[ParticipantStoreConfig] =
      deriveReader[ParticipantStoreConfig]
    lazy implicit val ledgerApiContractLoaderConfigReader: ConfigReader[ContractLoaderConfig] =
      deriveReader[ContractLoaderConfig]
    lazy implicit val ledgerApiServerParametersConfigReader
        : ConfigReader[LedgerApiServerParametersConfig] =
      deriveReader[LedgerApiServerParametersConfig]
    lazy implicit val participantNodeParameterConfigReader
        : ConfigReader[ParticipantNodeParameterConfig] =
      deriveReader[ParticipantNodeParameterConfig]
    lazy implicit val timeTrackerConfigReader: ConfigReader[DomainTimeTrackerConfig] =
      deriveReader[DomainTimeTrackerConfig]
    lazy implicit val timeRequestConfigReader: ConfigReader[TimeProofRequestConfig] =
      deriveReader[TimeProofRequestConfig]
    lazy implicit val authTokenManagerConfigReader: ConfigReader[AuthenticationTokenManagerConfig] =
      deriveReader[AuthenticationTokenManagerConfig]
    lazy implicit val sequencerClientConfigReader: ConfigReader[SequencerClientConfig] =
      deriveReader[SequencerClientConfig]
    lazy implicit val retentionPeriodDefaultsConfigReader: ConfigReader[RetentionPeriodDefaults] =
      deriveReader[RetentionPeriodDefaults]
    lazy implicit val inMemoryDbCacheSettingsReader: ConfigReader[DbCacheConfig] =
      deriveReader[DbCacheConfig]
    @nowarn("cat=unused") lazy implicit val batchAggregatorConfigReader
        : ConfigReader[BatchAggregatorConfig] = {
      implicit val batching = deriveReader[BatchAggregatorConfig.Batching]
      implicit val noBatching = deriveReader[BatchAggregatorConfig.NoBatching.type]

      deriveReader[BatchAggregatorConfig]
    }

    lazy implicit val ammoniteConfigReader: ConfigReader[AmmoniteConsoleConfig] =
      deriveReader[AmmoniteConsoleConfig]
    lazy implicit val cantonParametersReader: ConfigReader[CantonParameters] =
      deriveReader[CantonParameters]
    lazy implicit val cantonFeaturesReader: ConfigReader[CantonFeatures] =
      deriveReader[CantonFeatures]
  }

  /** writers
    * @param confidential if set to true, confidential data which should not be shared for support purposes is blinded
    */
  class ConfigWriters(confidential: Boolean) {
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

    implicit val maxRequestSizeWriter: ConfigWriter[MaxRequestSize] =
      ConfigWriter.toString(x => x.unwrap.toString)
    implicit val existingFileWriter: ConfigWriter[ExistingFile] =
      ConfigWriter.toString(x => x.unwrap.toString)
    implicit val nonEmptyStringWriter: ConfigWriter[NonEmptyString] =
      ConfigWriter.toString(x => x.unwrap)

    implicit val sequencerTestingInterceptorWriter
        : ConfigWriter[DatabaseSequencerConfig.TestingInterceptor] =
      ConfigWriter.toString(_ => "None")

    implicit val contractIdSeedingWriter: ConfigWriter[Seeding] = ConfigWriter.toString(_.name)

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
    implicit val tracingConfigAlwaysOnSamplerWriter: ConfigWriter[TracingConfig.Sampler.AlwaysOn] =
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

    lazy implicit val tlsClientCertificateWriter: ConfigWriter[TlsClientCertificate] =
      deriveWriter[TlsClientCertificate]
    lazy implicit val serverAuthRequirementConfigNoneWriter
        : ConfigWriter[ServerAuthRequirementConfig.None.type] =
      deriveWriter[ServerAuthRequirementConfig.None.type]
    lazy implicit val serverAuthRequirementConfigOptionalWriter
        : ConfigWriter[ServerAuthRequirementConfig.Optional.type] =
      deriveWriter[ServerAuthRequirementConfig.Optional.type]
    lazy implicit val serverAuthRequirementConfigRequireWriter
        : ConfigWriter[ServerAuthRequirementConfig.Require] =
      deriveWriter[ServerAuthRequirementConfig.Require]
    lazy implicit val serverAuthRequirementConfigWriter: ConfigWriter[ServerAuthRequirementConfig] =
      deriveWriter[ServerAuthRequirementConfig]
    lazy implicit val keepAliveClientConfigWriter: ConfigWriter[KeepAliveClientConfig] =
      deriveWriter[KeepAliveClientConfig]
    lazy implicit val keepAliveServerConfigWriter: ConfigWriter[KeepAliveServerConfig] =
      deriveWriter[KeepAliveServerConfig]
    lazy implicit val tlsServerConfigWriter: ConfigWriter[TlsServerConfig] =
      deriveWriter[TlsServerConfig]
    lazy implicit val tlsClientConfigWriter: ConfigWriter[TlsClientConfig] =
      deriveWriter[TlsClientConfig]
    lazy implicit val initBaseIdentityConfigWriter: ConfigWriter[InitConfigBase.Identity] =
      deriveWriter[InitConfigBase.Identity]
    lazy implicit val initConfigWriter: ConfigWriter[InitConfig] =
      InitConfigBase.writerForSubtype(deriveWriter[InitConfig])
    lazy implicit val httpHealthServerConfigWriter: ConfigWriter[HttpHealthServerConfig] =
      deriveWriter[HttpHealthServerConfig]
    lazy implicit val grpcHealthServerConfigWriter: ConfigWriter[GrpcHealthServerConfig] =
      deriveWriter[GrpcHealthServerConfig]
    lazy implicit val ledgerApiParticipantInitConfigWriter
        : ConfigWriter[ParticipantLedgerApiInitConfig] =
      deriveWriter[ParticipantLedgerApiInitConfig]
    lazy implicit val nodeNameConfigWriter: ConfigWriter[NodeIdentifierConfig.Config.type] =
      deriveWriter[NodeIdentifierConfig.Config.type]
    lazy implicit val nodeNameRandomWriter: ConfigWriter[NodeIdentifierConfig.Random.type] =
      deriveWriter[NodeIdentifierConfig.Random.type]
    lazy implicit val nodeNameExplicitWriter: ConfigWriter[NodeIdentifierConfig.Explicit] =
      deriveWriter[NodeIdentifierConfig.Explicit]
    lazy implicit val nodeNameWriter: ConfigWriter[NodeIdentifierConfig] =
      deriveWriter[NodeIdentifierConfig]
    lazy implicit val participantInitConfigWriter: ConfigWriter[ParticipantInitConfig] =
      InitConfigBase.writerForSubtype(deriveWriter[ParticipantInitConfig])
    lazy implicit val communityCryptoProviderWriter: ConfigWriter[CommunityCryptoProvider] =
      deriveEnumerationWriter[CommunityCryptoProvider]
    lazy implicit val cryptoSigningKeySchemeWriter: ConfigWriter[SigningKeyScheme] =
      deriveEnumerationWriter[SigningKeyScheme]
    lazy implicit val cryptoEncryptionKeySchemeWriter: ConfigWriter[EncryptionKeyScheme] =
      deriveEnumerationWriter[EncryptionKeyScheme]
    lazy implicit val cryptoSymmetricKeySchemeWriter: ConfigWriter[SymmetricKeyScheme] =
      deriveEnumerationWriter[SymmetricKeyScheme]
    lazy implicit val cryptoHashAlgorithmWriter: ConfigWriter[HashAlgorithm] =
      deriveEnumerationWriter[HashAlgorithm]
    lazy implicit val cryptoPbkdfSchemeWriter: ConfigWriter[PbkdfScheme] =
      deriveEnumerationWriter[PbkdfScheme]
    lazy implicit val cryptoKeyFormatWriter: ConfigWriter[CryptoKeyFormat] =
      deriveEnumerationWriter[CryptoKeyFormat]
    implicit def cryptoSchemeConfigWriter[S: ConfigWriter]: ConfigWriter[CryptoSchemeConfig[S]] =
      deriveWriter[CryptoSchemeConfig[S]]
    lazy implicit val communityCryptoWriter: ConfigWriter[CommunityCryptoConfig] =
      deriveWriter[CommunityCryptoConfig]
    lazy implicit val clientConfigWriter: ConfigWriter[ClientConfig] = deriveWriter[ClientConfig]
    lazy implicit val remoteParticipantConfigWriter: ConfigWriter[RemoteParticipantConfig] =
      deriveWriter[RemoteParticipantConfig]
    lazy implicit val nodeMonitoringConfigWriter: ConfigWriter[NodeMonitoringConfig] =
      deriveWriter[NodeMonitoringConfig]
    lazy implicit val batchingWriter: ConfigWriter[BatchingConfig] =
      deriveWriter[BatchingConfig]
    lazy implicit val connectionAllocationWriter: ConfigWriter[ConnectionAllocation] =
      deriveWriter[ConnectionAllocation]
    lazy implicit val dbParametersWriter: ConfigWriter[DbParametersConfig] =
      deriveWriter[DbParametersConfig]
    lazy implicit val memoryWriter: ConfigWriter[CommunityStorageConfig.Memory] =
      deriveWriter[CommunityStorageConfig.Memory]
    lazy implicit val h2Writer: ConfigWriter[CommunityDbConfig.H2] =
      confidentialWriter[CommunityDbConfig.H2](x =>
        x.copy(config = DbConfig.hideConfidential(x.config))
      )
    lazy implicit val postgresWriter: ConfigWriter[CommunityDbConfig.Postgres] =
      confidentialWriter[CommunityDbConfig.Postgres](x =>
        x.copy(config = DbConfig.hideConfidential(x.config))
      )
    lazy implicit val dbConfigWriter: ConfigWriter[CommunityDbConfig] =
      deriveWriter[CommunityDbConfig]
    lazy implicit val communityStorageConfigWriter: ConfigWriter[CommunityStorageConfig] =
      deriveWriter[CommunityStorageConfig]
    lazy implicit val monotonicTimeWriter
        : ConfigWriter[TestingTimeServiceConfig.MonotonicTime.type] =
      deriveWriter[TestingTimeServiceConfig.MonotonicTime.type]
    lazy implicit val testingTimeServiceConfigWriter: ConfigWriter[TestingTimeServiceConfig] =
      deriveWriter[TestingTimeServiceConfig]
    lazy implicit val communityAdminServerConfigWriter: ConfigWriter[CommunityAdminServerConfig] =
      deriveWriter[CommunityAdminServerConfig]
    lazy implicit val tlsBaseServerConfigWriter: ConfigWriter[TlsBaseServerConfig] =
      deriveWriter[TlsBaseServerConfig]
    lazy implicit val communityPublicServerConfigWriter: ConfigWriter[CommunityPublicServerConfig] =
      deriveWriter[CommunityPublicServerConfig]
    lazy implicit val clockConfigRemoteClockWriter: ConfigWriter[ClockConfig.RemoteClock] =
      deriveWriter[ClockConfig.RemoteClock]
    lazy implicit val clockConfigWallClockWriter: ConfigWriter[ClockConfig.WallClock] =
      deriveWriter[ClockConfig.WallClock]
    lazy implicit val clockConfigSimClockWriter: ConfigWriter[ClockConfig.SimClock.type] =
      deriveWriter[ClockConfig.SimClock.type]
    lazy implicit val clockConfigWriter: ConfigWriter[ClockConfig] = deriveWriter[ClockConfig]
    lazy implicit val jwtTimestampLeewayConfigWriter: ConfigWriter[JwtTimestampLeeway] =
      deriveWriter[JwtTimestampLeeway]
    lazy implicit val authServiceConfigJwtEs256CrtWriter
        : ConfigWriter[AuthServiceConfig.JwtEs256Crt] =
      deriveWriter[AuthServiceConfig.JwtEs256Crt]
    lazy implicit val authServiceConfigJwtEs512CrtWriter
        : ConfigWriter[AuthServiceConfig.JwtEs512Crt] =
      deriveWriter[AuthServiceConfig.JwtEs512Crt]
    lazy implicit val authServiceConfigJwtRs256CrtWriter
        : ConfigWriter[AuthServiceConfig.JwtRs256Crt] =
      deriveWriter[AuthServiceConfig.JwtRs256Crt]
    lazy implicit val authServiceConfigJwtRs256JwksWriter
        : ConfigWriter[AuthServiceConfig.JwtRs256Jwks] =
      deriveWriter[AuthServiceConfig.JwtRs256Jwks]
    lazy implicit val authServiceConfigUnsafeJwtHmac256Writer
        : ConfigWriter[AuthServiceConfig.UnsafeJwtHmac256] =
      confidentialWriter[AuthServiceConfig.UnsafeJwtHmac256](
        _.copy(secret = NonEmptyString.tryCreate("****"))
      )
    lazy implicit val authServiceConfigWildcardWriter
        : ConfigWriter[AuthServiceConfig.Wildcard.type] =
      deriveWriter[AuthServiceConfig.Wildcard.type]
    lazy implicit val authServiceConfigWriter: ConfigWriter[AuthServiceConfig] =
      deriveWriter[AuthServiceConfig]
    lazy implicit val rateLimitConfigWriter: ConfigWriter[RateLimitingConfig] =
      deriveWriter[RateLimitingConfig]
    lazy implicit val ledgerApiServerConfigWriter: ConfigWriter[LedgerApiServerConfig] =
      deriveWriter[LedgerApiServerConfig]
    lazy implicit val sequencerTrafficConfigWriter: ConfigWriter[SequencerTrafficConfig] =
      deriveWriter[SequencerTrafficConfig]
    implicit val throttleModeCfgWriter: ConfigWriter[ThrottleMode] =
      ConfigWriter.toString[ThrottleMode] {
        case ThrottleMode.Shaping => "shaping"
        case ThrottleMode.Enforcing => "enforcing"
      }

    lazy implicit val portFileWriter: ConfigWriter[Path] =
      ConfigWriter.toString(_.toFile.getAbsolutePath)
    lazy implicit val staticContentConfigWriter: ConfigWriter[StaticContentConfig] =
      deriveWriter[StaticContentConfig]
    lazy implicit val wsConfigWriter: ConfigWriter[WebsocketConfig] =
      deriveWriter[WebsocketConfig]

    lazy implicit val httpServerConfigWriter: ConfigWriter[HttpServerConfig] =
      deriveWriter[HttpServerConfig]
    lazy implicit val httpApiServerConfigWriter: ConfigWriter[HttpApiConfig] =
      deriveWriter[HttpApiConfig]
    lazy implicit val activeContractsServiceConfigWriter
        : ConfigWriter[ActiveContractsServiceStreamsConfig] =
      deriveWriter[ActiveContractsServiceStreamsConfig]
    lazy implicit val packageMetadataViewConfigWriter: ConfigWriter[PackageMetadataViewConfig] =
      deriveWriter[PackageMetadataViewConfig]
    lazy implicit val topologyXConfigWriter: ConfigWriter[TopologyConfig] =
      deriveWriter[TopologyConfig]
    lazy implicit val sequencerConnectionConfigCertificateFileWriter
        : ConfigWriter[SequencerConnectionConfig.CertificateFile] =
      deriveWriter[SequencerConnectionConfig.CertificateFile]
    lazy implicit val sequencerConnectionConfigCertificateStringWriter
        : ConfigWriter[SequencerConnectionConfig.CertificateString] =
      confidentialWriter[SequencerConnectionConfig.CertificateString](_.copy(pemString = "****"))
    lazy implicit val sequencerConnectionConfigCertificateConfigWriter
        : ConfigWriter[SequencerConnectionConfig.CertificateConfig] =
      deriveWriter[SequencerConnectionConfig.CertificateConfig]
    lazy implicit val sequencerConnectionConfigGrpcWriter
        : ConfigWriter[SequencerConnectionConfig.Grpc] =
      deriveWriter[SequencerConnectionConfig.Grpc]
    lazy implicit val sequencerConnectionConfigWriter: ConfigWriter[SequencerConnectionConfig] =
      deriveWriter[SequencerConnectionConfig]
    lazy implicit val communitySequencerConfigDatabaseWriter
        : ConfigWriter[CommunitySequencerConfig.Database] =
      deriveWriter[CommunitySequencerConfig.Database]
    lazy implicit val communityDatabaseSequencerReaderConfigWriter
        : ConfigWriter[CommunitySequencerReaderConfig] =
      deriveWriter[CommunitySequencerReaderConfig]
    lazy implicit val communitySequencerWriterCommitModeConfigWriter: ConfigWriter[CommitMode] =
      deriveEnumerationWriter[CommitMode]
    lazy implicit val communityDatabaseSequencerWriterConfigWriter
        : ConfigWriter[SequencerWriterConfig] =
      deriveWriter[SequencerWriterConfig]
    lazy implicit val communityDatabaseSequencerWriterConfigHighThroughputWriter
        : ConfigWriter[SequencerWriterConfig.HighThroughput] =
      deriveWriter[SequencerWriterConfig.HighThroughput]
    lazy implicit val communityDatabaseSequencerWriterConfigLowLatencyWriter
        : ConfigWriter[SequencerWriterConfig.LowLatency] =
      deriveWriter[SequencerWriterConfig.LowLatency]
    lazy implicit val sequencerNodeInitXConfigWriter: ConfigWriter[SequencerNodeInitXConfig] =
      InitConfigBase.writerForSubtype(deriveWriter[SequencerNodeInitXConfig])

    implicit def communitySequencerConfigWriter[C]: ConfigWriter[CommunitySequencerConfig] = {
      case dbSequencerConfig: CommunitySequencerConfig.Database =>
        import pureconfig.syntax.*
        Map("type" -> "database").toConfig
          .withFallback(communitySequencerConfigDatabaseWriter.to(dbSequencerConfig))

      case otherSequencerConfig: CommunitySequencerConfig.External =>
        import scala.jdk.CollectionConverters.*
        val sequencerType = otherSequencerConfig.sequencerType

        val factory: SequencerDriverFactory {
          type ConfigType = C
        } = DriverBlockSequencerFactory
          .getSequencerDriverFactory(sequencerType, SequencerDriver.DriverApiVersion)

        val configValue = factory.configParser
          .from(otherSequencerConfig.config)
          // in order to make use of the confidential flag, we must first parse the raw sequencer driver config
          // and then write it again now making sure to use hide fields if confidentiality is turned on
          .map(parsedConfig => factory.configWriter(confidential).to(parsedConfig))
          .valueOr(error =>
            sys.error(s"Failed to read $sequencerType sequencer's config. Error: $error")
          )
        ConfigValueFactory.fromMap(
          Map[String, Object]("type" -> sequencerType, "config" -> configValue).asJava
        )
    }

    lazy implicit val sequencerNodeParameterConfigWriter
        : ConfigWriter[SequencerNodeParameterConfig] =
      deriveWriter[SequencerNodeParameterConfig]
    lazy implicit val SequencerHealthConfigWriter: ConfigWriter[SequencerHealthConfig] =
      deriveWriter[SequencerHealthConfig]
    lazy implicit val remoteSequencerConfigWriter: ConfigWriter[RemoteSequencerConfig] =
      deriveWriter[RemoteSequencerConfig]
    lazy implicit val mediatorNodeParameterConfigWriter: ConfigWriter[MediatorNodeParameterConfig] =
      deriveWriter[MediatorNodeParameterConfig]
    lazy implicit val remoteMediatorConfigWriter: ConfigWriter[RemoteMediatorConfig] =
      deriveWriter[RemoteMediatorConfig]
    lazy implicit val domainParametersConfigWriter: ConfigWriter[DomainParametersConfig] =
      deriveWriter[DomainParametersConfig]
    lazy implicit val acsCommitmentsCatchUpConfigWriter: ConfigWriter[AcsCommitmentsCatchUpConfig] =
      deriveWriter[AcsCommitmentsCatchUpConfig]
    lazy implicit val deadlockDetectionConfigWriter: ConfigWriter[DeadlockDetectionConfig] =
      deriveWriter[DeadlockDetectionConfig]

    lazy implicit val metricsFilterConfigWriter: ConfigWriter[MetricsConfig.MetricsFilterConfig] =
      deriveWriter[MetricsConfig.MetricsFilterConfig]
    lazy implicit val metricsConfigPrometheusWriter
        : ConfigWriter[MetricsReporterConfig.Prometheus] =
      deriveWriter[MetricsReporterConfig.Prometheus]
    lazy implicit val metricsConfigCsvWriter: ConfigWriter[MetricsReporterConfig.Csv] =
      deriveWriter[MetricsReporterConfig.Csv]
    lazy implicit val metricsConfigLoggingWriter: ConfigWriter[MetricsReporterConfig.Logging] =
      deriveWriter[MetricsReporterConfig.Logging]
    lazy implicit val metricsConfigJvmMetricsWriter: ConfigWriter[MetricsConfig.JvmMetrics] =
      deriveWriter[MetricsConfig.JvmMetrics]
    lazy implicit val metricsReporterConfigWriter: ConfigWriter[MetricsReporterConfig] =
      deriveWriter[MetricsReporterConfig]
    lazy implicit val histogramDefinitionConfigWriter: ConfigWriter[HistogramDefinition] =
      deriveWriter[HistogramDefinition]
    lazy implicit val metricsConfigWriter: ConfigWriter[MetricsConfig] = deriveWriter[MetricsConfig]
    lazy implicit val queryCostMonitoringConfig: ConfigWriter[QueryCostMonitoringConfig] =
      deriveWriter[QueryCostMonitoringConfig]
    lazy implicit val apiLoggingConfigWriter: ConfigWriter[ApiLoggingConfig] =
      deriveWriter[ApiLoggingConfig]
    lazy implicit val loggingConfigWriter: ConfigWriter[LoggingConfig] =
      deriveWriter[LoggingConfig]
    lazy implicit val monitoringConfigWriter: ConfigWriter[MonitoringConfig] =
      deriveWriter[MonitoringConfig]
    lazy implicit val consoleCommandTimeoutWriter: ConfigWriter[ConsoleCommandTimeout] =
      deriveWriter[ConsoleCommandTimeout]
    lazy implicit val processingTimeoutWriter: ConfigWriter[ProcessingTimeout] =
      deriveWriter[ProcessingTimeout]
    lazy implicit val timeoutSettingsWriter: ConfigWriter[TimeoutSettings] =
      deriveWriter[TimeoutSettings]
    lazy implicit val partyNotificationConfigViaDomainWriter
        : ConfigWriter[PartyNotificationConfig.ViaDomain.type] =
      deriveWriter[PartyNotificationConfig.ViaDomain.type]
    lazy implicit val partyNotificationConfigEagerWriter
        : ConfigWriter[PartyNotificationConfig.Eager.type] =
      deriveWriter[PartyNotificationConfig.Eager.type]
    lazy implicit val partyNotificationConfigWriter: ConfigWriter[PartyNotificationConfig] =
      deriveWriter[PartyNotificationConfig]
    lazy implicit val cacheConfigWriter: ConfigWriter[CacheConfig] =
      deriveWriter[CacheConfig]
    lazy implicit val cacheConfigWithTimeoutWriter: ConfigWriter[CacheConfigWithTimeout] =
      deriveWriter[CacheConfigWithTimeout]
    lazy implicit val sessionKeyCacheConfigWriter: ConfigWriter[SessionKeyCacheConfig] =
      deriveWriter[SessionKeyCacheConfig]
    lazy implicit val cachingConfigsWriter: ConfigWriter[CachingConfigs] =
      deriveWriter[CachingConfigs]
    lazy implicit val adminWorkflowConfigWriter: ConfigWriter[AdminWorkflowConfig] =
      deriveWriter[AdminWorkflowConfig]
    lazy implicit val journalPruningConfigWriter: ConfigWriter[JournalPruningConfig] =
      deriveWriter[JournalPruningConfig]
    lazy implicit val participantStoreConfigWriter: ConfigWriter[ParticipantStoreConfig] =
      deriveWriter[ParticipantStoreConfig]
    lazy implicit val ledgerApiContractLoaderConfigWriter: ConfigWriter[ContractLoaderConfig] =
      deriveWriter[ContractLoaderConfig]
    lazy implicit val ledgerApiServerParametersConfigWriter
        : ConfigWriter[LedgerApiServerParametersConfig] =
      deriveWriter[LedgerApiServerParametersConfig]
    lazy implicit val participantNodeParameterConfigWriter
        : ConfigWriter[ParticipantNodeParameterConfig] =
      deriveWriter[ParticipantNodeParameterConfig]
    lazy implicit val timeTrackerConfigWriter: ConfigWriter[DomainTimeTrackerConfig] =
      deriveWriter[DomainTimeTrackerConfig]
    lazy implicit val timeRequestConfigWriter: ConfigWriter[TimeProofRequestConfig] =
      deriveWriter[TimeProofRequestConfig]
    lazy implicit val authTokenManagerConfigWriter: ConfigWriter[AuthenticationTokenManagerConfig] =
      deriveWriter[AuthenticationTokenManagerConfig]
    lazy implicit val sequencerClientConfigWriter: ConfigWriter[SequencerClientConfig] =
      deriveWriter[SequencerClientConfig]
    lazy implicit val retentionPeriodDefaultsConfigWriter: ConfigWriter[RetentionPeriodDefaults] =
      deriveWriter[RetentionPeriodDefaults]
    lazy implicit val inMemoryDbCacheSettingsWriter: ConfigWriter[DbCacheConfig] =
      deriveWriter[DbCacheConfig]
    lazy implicit val batchAggregatorConfigWriter: ConfigWriter[BatchAggregatorConfig] = {
      @nowarn("cat=unused") implicit val batching: ConfigWriter[BatchAggregatorConfig.Batching] =
        deriveWriter[BatchAggregatorConfig.Batching]
      @nowarn("cat=unused") implicit val noBatching
          : ConfigWriter[BatchAggregatorConfig.NoBatching.type] =
        deriveWriter[BatchAggregatorConfig.NoBatching.type]

      deriveWriter[BatchAggregatorConfig]
    }
    lazy implicit val ammoniteConfigWriter: ConfigWriter[AmmoniteConsoleConfig] =
      deriveWriter[AmmoniteConsoleConfig]
    lazy implicit val cantonParametersWriter: ConfigWriter[CantonParameters] =
      deriveWriter[CantonParameters]
    lazy implicit val cantonFeaturesWriter: ConfigWriter[CantonFeatures] =
      deriveWriter[CantonFeatures]
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
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, Config] = {
    for {
      verifiedFiles <- verifyThatFilesCanBeRead(files)
      parsedFiles <- parseConfigs(verifiedFiles)
      combinedConfig = mergeConfigs(parsedFiles)
    } yield combinedConfig
  }

  /** Renders a configuration file such that we can write it to the log-file on startup */
  def renderForLoggingOnStartup(config: Config): String = {
    import scala.jdk.CollectionConverters.*
    val replace =
      Set("secret", "pw", "password", "ledger-api-jdbc-url", "jdbc", "token", "admin-token")
    val blinded = ConfigValueFactory.fromAnyRef("****")
    def goVal(key: String, c: ConfigValue): ConfigValue = {
      c match {
        case lst: ConfigList => goLst(lst)
        case obj: ConfigObject =>
          goObj(obj)
        case other =>
          if (replace.contains(key))
            blinded
          else other
      }
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
    def go(c: Config): Config = {
      c
        .root()
        .entrySet()
        .asScala
        .map(x => (x.getKey, x.getValue))
        .foldLeft(c) { case (subConfig, (key, obj)) =>
          subConfig.withValue(key, goVal(key, obj))
        }
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

  private def configOrExit[ConfClass: ClassTag](
      result: Either[CantonConfigError, ConfClass]
  ): ConfClass = {
    result.valueOr { _ =>
      sys.exit(1)
    }
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
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, ConfClass] = {
    for {
      nonEmpty <- NonEmpty.from(files).toRight(NoConfigFiles.Error())
      parsedAndMerged <- parseAndMergeConfigs(nonEmpty)
      loaded <- loadAndValidate[ConfClass](parsedAndMerged)
    } yield loaded
  }

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
  )(implicit elc: ErrorLoggingContext): ConfClass = {
    loadAndValidate[ConfClass](config).valueOr(_ => sys.exit(1))
  }

  private[config] def loadRawConfig[ConfClass <: CantonConfig: ClassTag: ConfigReader](
      rawConfig: Config
  )(implicit elc: ErrorLoggingContext): Either[CantonConfigError, ConfClass] = {
    pureconfig.ConfigSource
      .fromConfig(rawConfig)
      .at("canton")
      .load[ConfClass]
      .leftMap(failures => GenericConfigError.Error(ConfigErrors.getMessage[ConfClass](failures)))
  }

  lazy val defaultConfigRenderer: ConfigRenderOptions =
    ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false)
}
