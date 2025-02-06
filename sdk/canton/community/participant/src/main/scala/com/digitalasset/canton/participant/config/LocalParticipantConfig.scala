// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import cats.syntax.option.*
import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.config
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.*
import com.digitalasset.canton.http.JsonApiConfig
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.participant.admin.AdminWorkflowConfig
import com.digitalasset.canton.participant.config.LedgerApiServerConfig.DefaultRateLimit
import com.digitalasset.canton.participant.sync.CommandProgressTrackerConfig
import com.digitalasset.canton.platform.apiserver.ApiServiceOwner
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.config.{
  CommandServiceConfig,
  IdentityProviderManagementConfig,
  IndexServiceConfig as LedgerIndexServiceConfig,
  InteractiveSubmissionServiceConfig,
  PartyManagementServiceConfig,
  UserManagementServiceConfig,
}
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.store.PrunableByTimeParameters
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import io.netty.handler.ssl.SslContext
import monocle.macros.syntax.lens.*

/** Base for all participant configs - both local and remote */
trait BaseParticipantConfig extends NodeConfig {
  def clientLedgerApi: ClientConfig
}

/** Base for local participant configurations */
trait LocalParticipantConfig extends BaseParticipantConfig with LocalNodeConfig {
  override val nodeTypeName: String = "participant"

  /** determines how this node is initialized */
  def init: ParticipantInitConfig

  /** determines the algorithms used for signing, hashing, and encryption */
  def crypto: CryptoConfig

  /** parameters of the interfaces that applications use to change and query the ledger */
  def ledgerApi: LedgerApiServerConfig

  /** parameters for configuring the interaction with ledger via the HTTP JSON API.
    * Configuring this key will enable the HTTP JSON API server.
    */
  def httpLedgerApi: Option[JsonApiConfig]

  /** parameters of the interface used to administrate the participant */
  def adminApi: AdminServerConfig

  /** determines how the participant stores the ledger */
  def storage: StorageConfig

  /** determines whether and how to support the ledger API time service */
  def testingTime: Option[TestingTimeServiceConfig]

  /** general participant node parameters */
  def parameters: ParticipantNodeParameterConfig

  def toRemoteConfig: RemoteParticipantConfig
}

final case class ParticipantProtocolConfig(
    minimumProtocolVersion: Option[ProtocolVersion],
    override val sessionSigningKeys: SessionSigningKeysConfig,
    override val alphaVersionSupport: Boolean,
    override val betaVersionSupport: Boolean,
    override val dontWarnOnDeprecatedPV: Boolean,
) extends ProtocolConfig

/** Configuration parameters for a single participant
  *
  * Please note that any client connecting to the ledger-api of the respective participant must set his GRPC max inbound
  * message size to 2x the value defined here, as we assume that a Canton transaction of N bytes will not be bigger
  * than 2x N on the ledger-api. Though this is just an assumption.
  * Please also note that the participant will refuse to connect to a synchronizer where its max inbound message size is not
  * sufficient to guarantee the processing of all transactions.
  */
final case class CommunityParticipantConfig(
    override val init: ParticipantInitConfig = ParticipantInitConfig(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val ledgerApi: LedgerApiServerConfig = LedgerApiServerConfig(),
    override val httpLedgerApi: Option[JsonApiConfig] = None,
    override val adminApi: AdminServerConfig = AdminServerConfig(),
    override val storage: StorageConfig = StorageConfig.Memory(),
    override val testingTime: Option[TestingTimeServiceConfig] = None,
    override val parameters: ParticipantNodeParameterConfig = ParticipantNodeParameterConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    override val topology: TopologyConfig = TopologyConfig(),
) extends LocalParticipantConfig
    with LocalNodeConfig
    with ConfigDefaults[DefaultPorts, CommunityParticipantConfig] {

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  override def clientLedgerApi: ClientConfig = ledgerApi.clientConfig

  def toRemoteConfig: RemoteParticipantConfig =
    RemoteParticipantConfig(clientAdminApi, clientLedgerApi)

  override def withDefaults(ports: DefaultPorts): CommunityParticipantConfig =
    this
      .focus(_.ledgerApi.internalPort)
      .modify(ports.ledgerApiPort.setDefaultPort)
      .focus(_.adminApi.internalPort)
      .modify(ports.participantAdminApiPort.setDefaultPort)
}

/** Configuration to connect the console to a participant running remotely.
  *
  * @param adminApi the configuration to connect the console to the remote admin api
  * @param ledgerApi the configuration to connect the console to the remote ledger api
  * @param token optional bearer token to use on the ledger-api if jwt authorization is enabled
  */
final case class RemoteParticipantConfig(
    adminApi: ClientConfig,
    ledgerApi: ClientConfig,
    token: Option[String] = None,
) extends BaseParticipantConfig {
  override def clientAdminApi: ClientConfig = adminApi
  override def clientLedgerApi: ClientConfig = ledgerApi
}

/** Canton configuration case class to pass-through configuration options to the ledger api server
  *
  * @param address                   ledger api server host name.
  * @param internalPort              ledger api server port.
  * @param tls                       tls configuration setting from ledger api server.
  * @param authServices              type of authentication services used by ledger-api server. If empty, we use a wildcard.
  *                                  Otherwise, the first service response that does not say "unauthenticated" will be used.
  * @param adminToken                token that should grant admin access when presented by a client on the ledger api
  * @param jwtTimestampLeeway        leeway parameters for JWTs
  * @param keepAliveServer           keep-alive configuration for ledger api requests
  * @param maxInboundMessageSize     maximum inbound message size on the ledger api
  * @param rateLimit                 limit the ledger api server request rates based on system metrics
  * @param postgresDataSource        config for ledger api server when using postgres
  * @param databaseConnectionTimeout database connection timeout
  * @param initSyncTimeout           ledger api server startup delay
  * @param indexService              configurations pertaining to the ledger api server's internal "index service"
  * @param commandService            configurations pertaining to the ledger api server's "command service"
  * @param userManagementService     configurations pertaining to the ledger api server's "user management service"
  * @param partyManagementService    configurations pertaining to the ledger api server's "party management service"
  * @param managementServiceTimeout  ledger api server management service maximum duration. Duration has to be finite
  *                                  as the ledger api server uses java.time.duration that does not support infinite scala durations.
  * @param enableCommandInspection   enable command inspection service over the ledger api
  * @param identityProviderManagement configurations pertaining to the ledger api server's "identity provider management service"
  * @param interactiveSubmissionServiceConfig config for interactive submission service over the ledger api
  */
final case class LedgerApiServerConfig(
    address: String = "127.0.0.1",
    internalPort: Option[Port] = None,
    tls: Option[TlsServerConfig] = None,
    authServices: Seq[AuthServiceConfig] = Seq.empty,
    adminToken: Option[String] = None,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    keepAliveServer: Option[LedgerApiKeepAliveServerConfig] = Some(
      LedgerApiKeepAliveServerConfig()
    ),
    maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize,
    rateLimit: Option[RateLimitingConfig] = Some(DefaultRateLimit),
    postgresDataSource: PostgresDataSourceConfig = PostgresDataSourceConfig(),
    databaseConnectionTimeout: config.NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultDatabaseConnectionTimeout,
    indexService: LedgerIndexServiceConfig = LedgerIndexServiceConfig(),
    commandService: CommandServiceConfig = CommandServiceConfig(),
    userManagementService: UserManagementServiceConfig = UserManagementServiceConfig(),
    partyManagementService: PartyManagementServiceConfig = PartyManagementServiceConfig(),
    managementServiceTimeout: config.NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultManagementServiceTimeout,
    enableCommandInspection: Boolean = true,
    identityProviderManagement: IdentityProviderManagementConfig =
      LedgerApiServerConfig.DefaultIdentityProviderManagementConfig,
    interactiveSubmissionService: InteractiveSubmissionServiceConfig =
      InteractiveSubmissionServiceConfig.Default,
) extends ServerConfig // We can't currently expose enterprise server features at the ledger api anyway
    {

  lazy val clientConfig: ClientConfig =
    ClientConfig(address, port, tls.map(_.clientConfig))

  override def sslContext: Option[SslContext] =
    tls.map(CantonServerBuilder.sslContext(_))

  override def serverCertChainFile: Option[ExistingFile] =
    tls.map(_.certChainFile)

}

object LedgerApiServerConfig {

  private val DefaultManagementServiceTimeout: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofMinutes(2L)
  private val DefaultDatabaseConnectionTimeout: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofSeconds(30)
  private val DefaultIdentityProviderManagementConfig: IdentityProviderManagementConfig =
    ApiServiceOwner.DefaultIdentityProviderManagementConfig
  val DefaultRateLimit: RateLimitingConfig =
    RateLimitingConfig.Default.copy(
      maxApiServicesQueueSize = 20000,
      // The two options below are to turn off memory based rate limiting by default
      maxUsedHeapSpacePercentage = 100,
      minFreeHeapSpaceBytes = 0,
    )

}

/** Optional ledger api time service configuration for demo and testing only */
sealed trait TestingTimeServiceConfig
object TestingTimeServiceConfig {

  /** A variant of [[TestingTimeServiceConfig]] with the ability to read and monotonically advance ledger time */
  case object MonotonicTime extends TestingTimeServiceConfig
}

/** General participant node parameters
  *
  * @param adminWorkflow Configuration options for Canton admin workflows
  * @param partyChangeNotification Determines how eagerly the participant nodes notify the ledger api of party changes.
  *                                By default ensure that parties are added via at least one synchronizer before ACKing party creation to ledger api server indexer.
  *                                This not only avoids flakiness in tests, but reflects that a party is not actually usable in canton until it's
  *                                available through at least one synchronizer.
  * @param maxUnzippedDarSize maximum allowed size of unzipped DAR files (in bytes) the participant can accept for uploading. Defaults to 1GB.
  * @param batching Various parameters that control batching related behavior
  * @param ledgerApiServer ledger api server parameters
  *
  * The following specialized participant node performance tuning parameters may be grouped once a more final set of configs emerges.
  * @param reassignmentTimeProofFreshnessProportion Proportion of the target synchronizer exclusivity timeout that is used as a freshness bound when
  *                                             requesting a time proof. Setting to 3 means we'll take a 1/3 of the target synchronizer exclusivity timeout
  *                                             and potentially we reuse a recent timeout if one exists within that bound, otherwise a new time proof
  *                                             will be requested.
  *                                             Setting to zero will disable reusing recent time proofs and will instead always fetch a new proof.
  * @param minimumProtocolVersion The minimum protocol version that this participant will speak when connecting to a synchronizer
  * @param initialProtocolVersion The initial protocol version used by the participant (default latest), e.g., used to create the initial topology transactions.
  * @param alphaVersionSupport If set to true, will allow the participant to connect to a synchronizer with dev protocol version and will turn on unsafe Daml LF versions.
  * @param dontWarnOnDeprecatedPV If true, then this participant will not emit a warning when connecting to a sequencer using a deprecated protocol version (such as 2.0.0).
  * @param warnIfOverloadedFor If all incoming commands have been rejected due to PARTICIPANT_BACKPRESSURE during this interval, the participant will log a warning.
  * @param excludeInfrastructureTransactions If set, infrastructure transactions (i.e. ping, bong and dar distribution) will be excluded from participant metering.
  * @param journalGarbageCollectionDelay How much time to delay the canton journal garbage collection
  * @param disableUpgradeValidation Disable the package upgrade verification on DAR upload
  * @param packageMetadataView Initialization parameters for the package metadata in-memory store.
  * @param experimentalEnableTopologyEvents If true, topology events are propagated to the Ledger API clients
  * @param enableExternalAuthorization If true, external authentication is supported
  */
final case class ParticipantNodeParameterConfig(
    adminWorkflow: AdminWorkflowConfig = AdminWorkflowConfig(),
    maxUnzippedDarSize: Int = 1024 * 1024 * 1024,
    batching: BatchingConfig = BatchingConfig(),
    caching: CachingConfigs = CachingConfigs(),
    stores: ParticipantStoreConfig = ParticipantStoreConfig(),
    reassignmentTimeProofFreshnessProportion: NonNegativeInt = NonNegativeInt.tryCreate(3),
    minimumProtocolVersion: Option[ParticipantProtocolVersion] = Some(
      ParticipantProtocolVersion(ProtocolVersion.v33)
    ),
    initialProtocolVersion: ParticipantProtocolVersion = ParticipantProtocolVersion(
      ProtocolVersion.latest
    ),
    sessionSigningKeys: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    alphaVersionSupport: Boolean = true,
    betaVersionSupport: Boolean = false,
    dontWarnOnDeprecatedPV: Boolean = false,
    warnIfOverloadedFor: Option[config.NonNegativeFiniteDuration] = Some(
      config.NonNegativeFiniteDuration.ofSeconds(20)
    ),
    ledgerApiServer: LedgerApiServerParametersConfig = LedgerApiServerParametersConfig(),
    excludeInfrastructureTransactions: Boolean = true,
    engine: CantonEngineConfig = CantonEngineConfig(),
    journalGarbageCollectionDelay: config.NonNegativeFiniteDuration =
      config.NonNegativeFiniteDuration.ofSeconds(0),
    disableUpgradeValidation: Boolean = false,
    watchdog: Option[WatchdogConfig] = None,
    packageMetadataView: PackageMetadataViewConfig = PackageMetadataViewConfig(),
    commandProgressTracker: CommandProgressTrackerConfig = CommandProgressTrackerConfig(),
    unsafeOnlinePartyReplication: Option[UnsafeOnlinePartyReplicationConfig] = None,
    experimentalEnableTopologyEvents: Boolean = false,
    enableExternalAuthorization: Boolean = false,
) extends LocalNodeParametersConfig

/** Parameters for the participant node's stores
  *
  * @param pruningMetricUpdateInterval  How frequently to update the `max-event-age` pruning progress metric in the background.
  *                                     A setting of None disables background metric updating.
  */
final case class ParticipantStoreConfig(
    pruningMetricUpdateInterval: Option[config.PositiveDurationSeconds] =
      config.PositiveDurationSeconds.ofHours(1L).some,
    journalPruning: JournalPruningConfig = JournalPruningConfig(),
)

/** Control background journal pruning
  *
  * During processing, Canton will keep some data in journals (contract keys, active contracts).
  * These journals can be pruned in order to reclaim space.
  *
  * Background pruning is initiated by the ACS commitment processor once a commitment interval
  * has been completed. Therefore, pruning can't run more frequently than the reconciliation interval
  * of a synchronizer.
  *
  * @param targetBatchSize The target batch size for pruning. The actual batch size will evolve under load.
  * @param initialInterval The initial interval size for pruning
  * @param maxBuckets The maximum number of buckets used for any pruning interval
  */
final case class JournalPruningConfig(
    targetBatchSize: PositiveInt = JournalPruningConfig.DefaultTargetBatchSize,
    initialInterval: config.NonNegativeFiniteDuration = JournalPruningConfig.DefaultInitialInterval,
    maxBuckets: PositiveInt = JournalPruningConfig.DefaultMaxBuckets,
) {
  def toInternal: PrunableByTimeParameters =
    PrunableByTimeParameters(
      targetBatchSize,
      initialInterval = initialInterval.toInternal,
      maxBuckets = maxBuckets,
    )
}

object JournalPruningConfig {
  private val DefaultTargetBatchSize = PositiveInt.tryCreate(5000)
  private val DefaultInitialInterval = config.NonNegativeFiniteDuration.ofSeconds(5)
  private val DefaultMaxBuckets = PositiveInt.tryCreate(100)
}

/** Parameters for the ledger api server
  *
  * @param contractIdSeeding  test-only way to override the contract-id seeding scheme. Must be Strong in production (and Strong is the default).
  *                           Only configurable to reduce the amount of secure random numbers consumed by tests and to avoid flaky timeouts during continuous integration.
  * @param indexer            parameters how the participant populates the index db used to serve the ledger api
  * @param tokenExpiryGracePeriodForStreams grace periods for streams that postpone termination beyond the JWT expiry
  */
final case class LedgerApiServerParametersConfig(
    contractIdSeeding: Seeding = Seeding.Strong,
    indexer: IndexerConfig = IndexerConfig(),
    tokenExpiryGracePeriodForStreams: Option[NonNegativeDuration] = None,
    contractLoader: ContractLoaderConfig = ContractLoaderConfig(),
)

/** Parameters to control batch loading during phase 1 / interpretation
  *
  * @param maxQueueSize how many parallel lookups can be queued before we start to backpressure loading
  * @param maxBatchSize how many contract lookups to batch together
  * @param parallelism  how many parallel contract lookup requests should be sent to the db when prepopulating the cache
  */
final case class ContractLoaderConfig(
    maxQueueSize: PositiveInt = ContractLoaderConfig.defaultMaxQueueSize,
    maxBatchSize: PositiveInt = ContractLoaderConfig.defaultMaxBatchSize,
    parallelism: PositiveInt = ContractLoaderConfig.defaultMaxParallelism,
)

object ContractLoaderConfig {
  private val defaultMaxQueueSize: PositiveInt = PositiveInt.tryCreate(10000)
  private val defaultMaxBatchSize: PositiveInt = PositiveInt.tryCreate(50)
  private val defaultMaxParallelism: PositiveInt = PositiveInt.tryCreate(5)
}

/** Parameters for the Online Party Replication (OPR) preview feature (unsafe for production)
  *
  * @param pauseSynchronizerIndexingDuringPartyReplication whether to pause synchronizer indexing during party replication
  */
final case class UnsafeOnlinePartyReplicationConfig(
    pauseSynchronizerIndexingDuringPartyReplication: Boolean = false
)
