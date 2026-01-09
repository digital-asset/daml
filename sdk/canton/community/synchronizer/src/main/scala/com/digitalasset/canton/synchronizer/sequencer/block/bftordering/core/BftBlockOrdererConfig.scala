// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.{
  ActiveRequestLimitsConfig,
  AdminTokenConfig,
  AuthServiceConfig,
  BasicKeepAliveServerConfig,
  BatchAggregatorConfig,
  ClientConfig,
  JwksCacheConfig,
  KeepAliveClientConfig,
  PemFileOrString,
  ServerConfig,
  StorageConfig,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  BftBlockOrderingStandaloneNetworkConfig,
  DefaultAvailabilityMaxNonOrderedBatchesPerNode,
  DefaultAvailabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning,
  DefaultConsensusBlockCompletionTimeout,
  DefaultConsensusEmptyBlockCreationTimeout,
  DefaultConsensusQueueMaxSize,
  DefaultConsensusQueuePerNodeQuota,
  DefaultDedicatedExecutionContextDivisor,
  DefaultDelayedInitQueueMaxSize,
  DefaultEpochLength,
  DefaultEpochStateTransferTimeout,
  DefaultLeaderSelectionPolicy,
  DefaultMaxBatchCreationInterval,
  DefaultMaxBatchesPerProposal,
  DefaultMaxMempoolQueueSize,
  DefaultMaxRequestPayloadBytes,
  DefaultMaxRequestsInBatch,
  DefaultMinRequestsInBatch,
  DefaultOutputFetchTimeout,
  DefaultOutputFetchTimeoutCap,
  LeaderSelectionPolicyConfig,
  P2PNetworkConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.BlacklistStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext

import java.io.File
import scala.concurrent.duration.*

/** Configuration class for the BFT Block Orderer.
  *
  * @param epochLength
  *   The total number of blocks in aggregate to be ordered across all leader-driven segments within
  *   a consensus epoch. Recall that topology changes occur only across epochs (not within).
  * @param maxRequestPayloadBytes
  *   The maximum number of bytes allowed per individual request submitted by clients
  * @param maxMempoolQueueSize
  *   The maximum number of pending requests that will be held in the in-memory mempool. Once this
  *   queue size is reached, subsequent requests are rejected with a mempool overloaded error.
  * @param maxRequestsInBatch
  *   The maximum number of requests in a batch. Needs to be the same across the network for the BFT
  *   time assumptions to hold.
  * @param minRequestsInBatch
  *   When the mempool does not create and send a batch due to other factors (see
  *   [[maxBatchCreationInterval]]), this is the number of requests that the mempool will wait for
  *   before shipping a batch to the availability module.
  * @param maxBatchCreationInterval
  *   The maximum amount of time the mempool will wait for incoming requests before creating and
  *   sending a batch to the availability module.
  * @param availabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning
  *   The number of consecutive failed attempts to retrieve data for an ordered batch before the
  *   ordering node emits a warning to the logs
  * @param availabilityMaxNonOrderedBatchesPerNode
  *   The maximum number of unordered batches that the availability module will accept from each
  *   peer. The purpose of this limit is to prevent malicious peers from consuming memory and
  *   storage by flooding new batches to correct nodes.
  * @param maxBatchesPerBlockProposal
  *   The maximum number of batches per block proposal (pre-prepare). Needs to be the same across
  *   the network for the BFT time assumptions to hold.
  * @param consensusQueueMaxSize
  *   The maximum size per consensus-related queue.
  * @param consensusQueuePerNodeQuota
  *   The maximum number of messages per node stored in consensus-related queues (quotas are
  *   maintained separately per queue).
  * @param consensusBlockCompletionTimeout
  *   The maximum time that nodes will wait for each leader to order the next expected block in the
  *   corresponding index. After each block in a segment is ordered (and at the start of each
  *   epoch), this timeout is reset for that corresponding segment. If the timeout is reached for a
  *   given segment, the node will vote for a View Change.
  * @param consensusEmptyBlockCreationTimeout
  *   The time after which a leader will create an empty block if there is no regular block
  *   available to propose, in order to ensure that: (i) View Changes are avoided, and (ii) BFT time
  *   advances at a regular frequency. Note that due to (i), it is recommended that this
  *   consensusEmptyBlockCreationTimeout be substantially less than the
  *   consensusBlockCompletionTimeout to account for latency, retransmissions, etc.
  * @param delayedInitQueueMaxSize
  *   The maximum size of the delayed init queue. This queue is used by modules to save incoming
  *   events in memory while the module is still initializing. Once startup is complete, the module
  *   processes all events from the delayed init queue first before continuing to read newly
  *   received events.
  * @param epochStateTransferRetryTimeout
  *   The state transfer retry timeout covering periods from requesting blocks from a single epoch
  *   up to receiving all the corresponding batches.
  * @param outputFetchTimeout
  *   The baseline timeout that a node's availability module will wait while fetching batch data
  *   from a peer before giving up and trying a different peer.
  * @param outputFetchTimeoutCap
  *   The maximum timeout, after jitter and backoff are considered, that a node's availability
  *   module will wait while fetching batch data from a peer.
  * @param initialNetwork
  *   Optionally, the set of peers for which the local node has connections with on startup. If set
  *   to [[scala.None]], the peer starts up with no preexisting peers.
  * @param standalone
  *   Optionally, a startup mode in which the BFT ordering node runs as a "standalone" service,
  *   allowing the BFT layer to bypass the sequencer to directly receive requests from and serve
  *   reads to clients. This mode is useful for isolated performance and scale testing. If set to
  *   [[scala.None]], the BFT layer behaves as normal with the co-located Sequencer component.
  * @param leaderSelectionPolicy
  *   The leader selection policy to enforce in the presence of View Changes of segments. There are
  *   currently two policies to choose from: `Simple` and `Blacklisting`. With the `Simple` policy,
  *   every node in the topology is assigned a segment, regardless of past behavior. Whereas with
  *   the `Blacklisting` policy, nodes that recently misbehaved (i.e., their segment resulted in a
  *   View Change) are penalized and not assigned a segment in the following epoch(s) until
  *   sufficient time has elapsed.
  * @param storage
  *   Optionally, a dedicated storage solution for the BFT ordering layer, separate from the
  *   co-located sequencer. If set to [[scala.None]], the BFT layer shares the same storage as the
  *   sequencer.
  * @param enablePerformanceMetrics
  *   If `true`, the BFT layer emits metrics to track real-time and historical performance.
  * @param batchAggregator
  *   Configuration file to use for the BatchAggregator, which converts frequent individual requests
  *   to the storage layer with batches of requests in order to reduce the impact on the connection
  *   pool. Example config settings include: `maximumInFlight` and `maximumBatchSize`.
  * @param dedicatedExecutionContextDivisor
  *   Optionally, a dedicated execution context for the BFT orderer. When set, the BFT orderer
  *   creates an independent execution context, separate from the colocated sequencer, using
  *   `Threading.detectNumberOfThreads` / `dedicatedExecutionContextDivisor`. This is potentially
  *   useful in deployments with heavy execution context contention between the sequencer and BFT
  *   ordering layer. If set to the default [[scala.None]], the BFT orderer shares the same
  *   execution context as the sequencer.
  */
final case class BftBlockOrdererConfig(
    // TODO(#24184) make a dynamic sequencing parameter
    epochLength: Long = DefaultEpochLength,
    maxRequestPayloadBytes: Int = DefaultMaxRequestPayloadBytes,
    maxMempoolQueueSize: Int = DefaultMaxMempoolQueueSize,
    // TODO(#24184) make a dynamic sequencing parameter
    maxRequestsInBatch: Short = DefaultMaxRequestsInBatch,
    minRequestsInBatch: Short = DefaultMinRequestsInBatch,
    maxBatchCreationInterval: FiniteDuration = DefaultMaxBatchCreationInterval,
    availabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning: Int =
      DefaultAvailabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning,
    availabilityMaxNonOrderedBatchesPerNode: Short = DefaultAvailabilityMaxNonOrderedBatchesPerNode,
    // TODO(#24184) make a dynamic sequencing parameter
    maxBatchesPerBlockProposal: Short = DefaultMaxBatchesPerProposal,
    consensusQueueMaxSize: Int = DefaultConsensusQueueMaxSize,
    consensusQueuePerNodeQuota: Int = DefaultConsensusQueuePerNodeQuota,
    consensusBlockCompletionTimeout: FiniteDuration = DefaultConsensusBlockCompletionTimeout,
    consensusEmptyBlockCreationTimeout: FiniteDuration = DefaultConsensusEmptyBlockCreationTimeout,
    delayedInitQueueMaxSize: Int = DefaultDelayedInitQueueMaxSize,
    epochStateTransferRetryTimeout: FiniteDuration = DefaultEpochStateTransferTimeout,
    outputFetchTimeout: FiniteDuration = DefaultOutputFetchTimeout,
    outputFetchTimeoutCap: FiniteDuration = DefaultOutputFetchTimeoutCap,
    initialNetwork: Option[P2PNetworkConfig] = None,
    standalone: Option[BftBlockOrderingStandaloneNetworkConfig] = None,
    // TODO(#24184) make a dynamic sequencing parameter
    leaderSelectionPolicy: LeaderSelectionPolicyConfig = DefaultLeaderSelectionPolicy,
    storage: Option[StorageConfig] = None,
    // We may want to flip the default once we're satisfied with initial performance
    enablePerformanceMetrics: Boolean = true,
    batchAggregator: BatchAggregatorConfig = BatchAggregatorConfig(),
    dedicatedExecutionContextDivisor: Option[Int] = DefaultDedicatedExecutionContextDivisor,
) {
  private val maxRequestsPerBlock = maxBatchesPerBlockProposal * maxRequestsInBatch
  require(
    maxRequestsPerBlock < BftTime.MaxRequestsPerBlock,
    s"Maximum block size too big: $maxRequestsInBatch maximum requests per batch and " +
      s"$maxBatchesPerBlockProposal maximum batches per block proposal means " +
      s"$maxRequestsPerBlock maximum requests per block, " +
      s"but the maximum number allowed of requests per block is ${BftTime.MaxRequestsPerBlock}",
  )
}

object BftBlockOrdererConfig {

  // Minimum epoch length that allows 16 nodes (i.e., the current CN load test target) to all act as consensus leaders
  val DefaultEpochLength: EpochLength = EpochLength(16)

  val DefaultMaxRequestPayloadBytes: Int = 1 * 1024 * 1024
  val DefaultMaxMempoolQueueSize: Int = 10 * 1024
  val DefaultMaxRequestsInBatch: Short = 32
  val DefaultMinRequestsInBatch: Short = 3
  val DefaultMaxBatchCreationInterval: FiniteDuration = 100.milliseconds
  val DefaultMaxBatchesPerProposal: Short = 16
  val DefaultAvailabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning: Int = 5
  val DefaultAvailabilityMaxNonOrderedBatchesPerNode: Short = 1000
  val DefaultConsensusQueueMaxSize: Int = 10 * 1024
  val DefaultConsensusQueuePerNodeQuota: Int = 1024
  val DefaultConsensusBlockCompletionTimeout: FiniteDuration = 10.seconds
  val DefaultConsensusEmptyBlockCreationTimeout: FiniteDuration = 5.seconds
  val DefaultDelayedInitQueueMaxSize: Int = 1024
  val DefaultEpochStateTransferTimeout: FiniteDuration = 10.seconds
  val DefaultOutputFetchTimeout: FiniteDuration = 2.second
  val DefaultOutputFetchTimeoutCap: FiniteDuration = 20.second

  val DefaultHowLongToBlackList: LeaderSelectionPolicyConfig.HowLongToBlacklist =
    LeaderSelectionPolicyConfig.HowLongToBlacklist.Linear
  val DefaultHowManyCanWeBlacklist: LeaderSelectionPolicyConfig.HowManyCanWeBlacklist =
    LeaderSelectionPolicyConfig.HowManyCanWeBlacklist.NumFaultsTolerated
  val DefaultLeaderSelectionPolicy: LeaderSelectionPolicyConfig =
    LeaderSelectionPolicyConfig.Blacklisting()
  val DefaultDedicatedExecutionContextDivisor: Option[Int] = None

  /** Trait for configuring the blacklist leader selection policy.
    *
    * The leader selection policy must define
    *   - how many peers can be blacklisted simultaneously
    *   - how long, in terms of Consensus epochs, a blacklisted peer should remain on the blacklist
    */
  trait BlacklistLeaderSelectionPolicyConfig {
    def howLongToBlackList: LeaderSelectionPolicyConfig.HowLongToBlacklist
    def howManyCanWeBlacklist: LeaderSelectionPolicyConfig.HowManyCanWeBlacklist
  }

  val DefaultAuthenticationTokenManagerConfig: AuthenticationTokenManagerConfig =
    AuthenticationTokenManagerConfig()

  /** Configuration for peer-to-peer network settings
    *
    * @param serverEndpoint
    *   server-side settings for remote peers to establish connections to this node
    * @param endpointAuthentication
    *   authentication settings, including token management
    * @param connectionManagementConfig
    *   connection-specific settings, such as number of retries and delays
    * @param peerEndpoints
    *   set of peers that the local node expects to connect to on startup
    * @param overwriteStoredEndpoints
    *   whether the peers in storage should be replaced by this config's peers endpoint on startup
    */
  final case class P2PNetworkConfig(
      serverEndpoint: P2PServerConfig,
      endpointAuthentication: P2PNetworkAuthenticationConfig = P2PNetworkAuthenticationConfig(),
      connectionManagementConfig: P2PConnectionManagementConfig = P2PConnectionManagementConfig(),
      peerEndpoints: Seq[P2PEndpointConfig] = Seq.empty,
      overwriteStoredEndpoints: Boolean = false,
  )

  /** Configuration for peer-to-peer authentication
    *
    * @param authToken
    *   The authentication token manager config
    * @param enabled
    *   Whether authentication should be setup between peers. For security reasons, this should NOT
    *   be disabled in any real production deployments, unless a decision is explicitly made to
    *   exclusively rely on mTLS for peer-to-peer authentication.
    */
  final case class P2PNetworkAuthenticationConfig(
      authToken: AuthenticationTokenManagerConfig = DefaultAuthenticationTokenManagerConfig,
      enabled: Boolean = true,
  )

  /** Configuration for peer-to-peer connections
    *
    * @param maxConnectionAttemptsBeforeWarning
    *   The number of failed connection attempts after which a WARN log is emitted
    * @param initialConnectionMaxDelay
    *   The base max delay between retry attempts, used to bootstrap the jitter mechanism
    * @param initialConnectionRetryDelay
    *   The base delay between retry attempts when connecting to a peer
    * @param maxConnectionRetryDelay
    *   The maximum delay between retry attempts to connect to a peer
    * @param connectionRetryDelayMultiplier
    *   The backoff factor applied to the delay between subsequent failed retry attempts
    */
  final case class P2PConnectionManagementConfig(
      // The maximum number of connection attempts before we log a warning.
      //  Together with the retry delays, it limits the maximum time spent trying to connect to a peer before
      //  failure is logged at as a warning.
      //  This time must be long enough to allow the sequencer to start up and shut down gracefully.
      maxConnectionAttemptsBeforeWarning: NonNegativeInt = NonNegativeInt.tryCreate(30),
      initialConnectionMaxDelay: config.NonNegativeFiniteDuration =
        config.NonNegativeFiniteDuration.ofMillis(500),
      initialConnectionRetryDelay: config.NonNegativeFiniteDuration =
        config.NonNegativeFiniteDuration.ofMillis(500),
      maxConnectionRetryDelay: config.NonNegativeFiniteDuration =
        config.NonNegativeFiniteDuration.ofMinutes(2),
      connectionRetryDelayMultiplier: NonNegativeInt = NonNegativeInt.two,
  )

  /** Configuration for the peer-to-peer server that accepts incoming connections
    *
    * @param address
    *   The internal binding address for the server.
    * @param internalPort
    *   The internal binding port for the server.
    * @param externalAddress
    *   The external address that remote peers should connect to in order to reach the local server.
    *   In practice, this may not be the actual internal [[address]] that the local server listens
    *   on, and instead may represent, for example, an intermediate TLS-terminating proxy.
    * @param externalPort
    *   The external port that remote peers should connect to in order to reach the local server. In
    *   practice, this may not be the actual internal [[internalPort]] that the local server listens
    *   on, and instead may represent, for example, an intermediate TLS-terminating proxy.
    * @param externalTlsConfig
    *   Optionally, the TLS client config. The local node uses this TLS client config when
    *   connecting back to the remote peer for authentication purposes. For security reasons, this
    *   should NOT be set to [[scala.None]] in any real production deployments.
    * @param tls
    *   Optionally, the TLS server config that the local server uses when accepting connections from
    *   remote peers (which act as clients). For security reasons, either the server should be
    *   behind a TLS-terminating proxy or this should NOT be set to [[scala.None]] in any real
    *   production deployments.
    * @param maxInboundMessageSize
    *   maximum allowed size of incoming messages
    * @param limits
    *   Optionally, the number of open requests allowed for this server
    *
    * The [[externalAddress]], [[externalPort]] and [[externalTlsConfig]] must be configured
    * correctly for a peer (acting as a client) to correctly connect/authenticate with the local
    * server,
    *
    * as the client tells the server its endpoint for authentication based on this information.
    */
  final case class P2PServerConfig(
      override val address: String,
      override val internalPort: Option[Port] = None,
      externalAddress: String,
      externalPort: Port,
      externalTlsConfig: Option[TlsClientConfig] = Some(
        TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = true)
      ),
      tls: Option[TlsServerConfig] = None,
      override val maxInboundMessageSize: NonNegativeInt =
        ServerConfig.defaultMaxInboundMessageSize,
      override val limits: Option[ActiveRequestLimitsConfig] = None,
  ) extends ServerConfig {
    override val name: String = "peer-to-peer"
    override val maxTokenLifetime: config.NonNegativeDuration =
      config.NonNegativeDuration(Duration.Inf)
    override val jwksCacheConfig: JwksCacheConfig = JwksCacheConfig()
    override val jwtTimestampLeeway: Option[JwtTimestampLeeway] = None
    override val keepAliveServer: Option[BasicKeepAliveServerConfig] = None
    override val authServices: Seq[AuthServiceConfig] = Seq.empty
    override val adminTokenConfig: AdminTokenConfig = AdminTokenConfig()

    override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.sslContext(_))

    override def serverCertChainFile: Option[PemFileOrString] = tls.map(_.certChainFile)

    private[bftordering] def serverToClientAuthenticationEndpointConfig: P2PEndpointConfig =
      P2PEndpointConfig(externalAddress, externalPort, externalTlsConfig)
  }

  /** Configuration for peer-to-peer client-side endpoints.
    *
    * Each endpoint is identified by the tuple: ([[address]], [[port]], [[tlsConfig]]). Since this
    * endpoint represents the client perspective, [[tlsConfig]] is a TLS Client Configuration. For
    * security reasons, this should NOT be set to [[scala.None]] in any real production deployments.
    */
  final case class P2PEndpointConfig(
      override val address: String,
      override val port: Port,
      override val tlsConfig: Option[TlsClientConfig] = Some(
        TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = true)
      ),
  ) extends ClientConfig {
    override val keepAliveClient: Option[KeepAliveClientConfig] = None
  }

  final case class EndpointId(
      address: String,
      port: Port,
      tls: Boolean,
  )

  /** Trait that defines the leader selection policy
    *
    * The current implementations include:
    *   - [[LeaderSelectionPolicyConfig.Simple]]
    *   - [[LeaderSelectionPolicyConfig.Blacklisting]]
    */
  sealed trait LeaderSelectionPolicyConfig

  object LeaderSelectionPolicyConfig {

    /** Simple leader selection policy.
      *
      * With this policy, no blacklisting of peers actually occurs. All peers in the topology are
      * assigned Consensus segments to lead in each epoch, regardless of past behavior. While this
      * approach is straightforward, it can result in delays if a node is down, disconnected, or
      * actively malicious.
      */
    final case object Simple extends LeaderSelectionPolicyConfig

    /** Blacklisting leader selection policy
      *
      * With this policy, peers that fail to complete their consensus segment within the allotted
      * time in an epoch are temporarily banned from leading segments in subsequent epochs.
      *   - The number of epochs spent on the blacklist before this peer is given another chance to
      *     lead a segment in a future Epoch. This duration is defined by [[howLongToBlackList]],
      *     and defaults to a linearly increasing penalty for peers that fail to complete their
      *     assigned segment across consecutive trials.
      *   - The number of peers that can be simultaneously blacklisted is defined by
      *     [[howManyCanWeBlacklist]], and defaults to the number of tolerated faults: f = (N-1)/3.
      * @note
      *   [[howManyCanWeBlacklist]] should not exceed 2f, as this could blacklist all correct nodes
      *   in the network for a consensus, leaving only the f malicious nodes with assigned segments
      *   and potentially leading to a network-wide halt.
      */
    final case class Blacklisting(
        override val howLongToBlackList: LeaderSelectionPolicyConfig.HowLongToBlacklist =
          DefaultHowLongToBlackList,
        override val howManyCanWeBlacklist: LeaderSelectionPolicyConfig.HowManyCanWeBlacklist =
          DefaultHowManyCanWeBlacklist,
    ) extends LeaderSelectionPolicyConfig
        with BlacklistLeaderSelectionPolicyConfig

    sealed trait HowLongToBlacklist {
      def compute(failedEpochSoFar: Long): BlacklistStatus
    }

    object HowLongToBlacklist {

      case object Linear extends HowLongToBlacklist {
        override def compute(failedEpochSoFar: Long): BlacklistStatus =
          BlacklistStatus.Blacklisted(failedEpochSoFar, failedEpochSoFar)
      }

      case object NoBlacklisting extends HowLongToBlacklist {
        override def compute(failedEpochSoFar: Long): BlacklistStatus = BlacklistStatus.Clean
      }
    }

    sealed trait HowManyCanWeBlacklist {
      def howManyCanWeBlacklist(orderingTopology: OrderingTopology): Int
    }

    object HowManyCanWeBlacklist {

      case object NumFaultsTolerated extends HowManyCanWeBlacklist {
        override def howManyCanWeBlacklist(orderingTopology: OrderingTopology): Int =
          orderingTopology.numFaultsTolerated
      }

      case object NoBlacklisting extends HowManyCanWeBlacklist {
        override def howManyCanWeBlacklist(orderingTopology: OrderingTopology): Int = 0
      }
    }
  }

  final case class BftBlockOrderingStandaloneNetworkConfig(
      thisSequencerId: String,
      signingPrivateKeyProtoFile: File,
      signingPublicKeyProtoFile: File,
      peers: Seq[BftBlockOrderingStandalonePeerConfig],
  )

  final case class BftBlockOrderingStandalonePeerConfig(
      sequencerId: String,
      signingPublicKeyProtoFile: File,
  )

}
