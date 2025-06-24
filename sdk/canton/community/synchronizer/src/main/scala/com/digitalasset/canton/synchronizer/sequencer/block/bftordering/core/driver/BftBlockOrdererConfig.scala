// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  AuthServiceConfig,
  BasicKeepAliveServerConfig,
  CantonConfigValidator,
  CantonConfigValidatorInstances,
  ClientConfig,
  KeepAliveClientConfig,
  PemFileOrString,
  ServerConfig,
  StorageConfig,
  TlsClientConfig,
  TlsServerConfig,
  UniformCantonConfigValidation,
}
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.{
  BlacklistLeaderSelectionPolicyConfig,
  DefaultAvailabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning,
  DefaultConsensusQueueMaxSize,
  DefaultConsensusQueuePerNodeQuota,
  DefaultDelayedInitQueueMaxSize,
  DefaultEpochLength,
  DefaultEpochStateTransferTimeout,
  DefaultHowLongToBlackList,
  DefaultHowManyCanWeBlacklist,
  DefaultLeaderSelectionPolicy,
  DefaultMaxBatchCreationInterval,
  DefaultMaxBatchesPerProposal,
  DefaultMaxMempoolQueueSize,
  DefaultMaxRequestPayloadBytes,
  DefaultMaxRequestsInBatch,
  DefaultMinRequestsInBatch,
  DefaultOutputFetchTimeout,
  DefaultPruningConfig,
  LeaderSelectionPolicyConfig,
  P2PNetworkConfig,
  PruningConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.BlacklistStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import io.netty.handler.ssl.SslContext

import scala.concurrent.duration.*

/** @param maxRequestsInBatch
  *   A maximum number of requests in a batch. Needs to be the same across the network for the BFT
  *   time assumptions to hold. It is validated in runtime.
  * @param maxBatchesPerBlockProposal
  *   A maximum number of batches per block proposal (pre-prepare). Needs to be the same across the
  *   network for the BFT time assumptions to hold. It is validated in runtime.
  * @param consensusQueueMaxSize
  *   A maximum size per consensus-related queue.
  * @param consensusQueuePerNodeQuota
  *   A maximum number of messages per node stored in consensus-related queues (quotas are
  *   maintained separately per queue).
  * @param delayedInitQueueMaxSize
  *   A maximum size per delayed init queue.
  * @param epochStateTransferRetryTimeout
  *   A state transfer retry timeout covering periods from requesting blocks from a single epoch up
  *   to receiving all the corresponding batches.
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
    // TODO(#24184) make a dynamic sequencing parameter
    maxBatchesPerBlockProposal: Short = DefaultMaxBatchesPerProposal,
    consensusQueueMaxSize: Int = DefaultConsensusQueueMaxSize,
    consensusQueuePerNodeQuota: Int = DefaultConsensusQueuePerNodeQuota,
    delayedInitQueueMaxSize: Int = DefaultDelayedInitQueueMaxSize,
    epochStateTransferRetryTimeout: FiniteDuration = DefaultEpochStateTransferTimeout,
    outputFetchTimeout: FiniteDuration = DefaultOutputFetchTimeout,
    pruningConfig: PruningConfig = DefaultPruningConfig,
    initialNetwork: Option[P2PNetworkConfig] = None,
    howLongToBlacklist: LeaderSelectionPolicyConfig.HowLongToBlacklist = DefaultHowLongToBlackList,
    howManyCanWeBlacklist: LeaderSelectionPolicyConfig.HowManyCanWeBlacklist =
      DefaultHowManyCanWeBlacklist,
    leaderSelectionPolicy: LeaderSelectionPolicyConfig = DefaultLeaderSelectionPolicy,
    storage: Option[StorageConfig] = None,
    // We may want to flip the default once we're satisfied with initial performance
    enablePerformanceMetrics: Boolean = true,
) extends UniformCantonConfigValidation {
  private val maxRequestsPerBlock = maxBatchesPerBlockProposal * maxRequestsInBatch
  require(
    maxRequestsPerBlock < BftTime.MaxRequestsPerBlock,
    s"Maximum block size too big: $maxRequestsInBatch maximum requests per batch and " +
      s"$maxBatchesPerBlockProposal maximum batches per block proposal means " +
      s"$maxRequestsPerBlock maximum requests per block, " +
      s"but the maximum number allowed of requests per block is ${BftTime.MaxRequestsPerBlock}",
  )

  def blacklistLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig =
    BlacklistLeaderSelectionPolicyConfig(howLongToBlacklist, howManyCanWeBlacklist)
}

object BftBlockOrdererConfig {

  // Minimum epoch length that allows 16 nodes (i.e., the current CN load test target) to all act as consensus leaders
  val DefaultEpochLength: EpochLength = EpochLength(16)

  val DefaultMaxRequestPayloadBytes: Int = 1 * 1024 * 1024
  val DefaultMaxMempoolQueueSize: Int = 10 * 1024
  val DefaultMaxRequestsInBatch: Short = 16
  val DefaultMinRequestsInBatch: Short = 3
  val DefaultMaxBatchCreationInterval: FiniteDuration = 100.milliseconds
  val DefaultMaxBatchesPerProposal: Short = 16
  val DefaultAvailabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning: Int = 5
  val DefaultConsensusQueueMaxSize: Int = 10 * 1024
  val DefaultConsensusQueuePerNodeQuota: Int = 1024
  val DefaultDelayedInitQueueMaxSize: Int = 1024
  val DefaultEpochStateTransferTimeout: FiniteDuration = 10.seconds
  val DefaultOutputFetchTimeout: FiniteDuration = 2.second
  val DefaultPruningConfig: PruningConfig = PruningConfig(
    enabled = true,
    retentionPeriod = 30.days,
    minNumberOfBlocksToKeep = 100,
    pruningFrequency = 1.hour,
  )
  val DefaultLeaderSelectionPolicy: LeaderSelectionPolicyConfig = LeaderSelectionPolicyConfig.Simple
  val DefaultHowLongToBlackList: LeaderSelectionPolicyConfig.HowLongToBlacklist =
    LeaderSelectionPolicyConfig.HowLongToBlacklist.Linear
  val DefaultHowManyCanWeBlacklist: LeaderSelectionPolicyConfig.HowManyCanWeBlacklist =
    LeaderSelectionPolicyConfig.HowManyCanWeBlacklist.NumFaultsTolerated

  final case class BlacklistLeaderSelectionPolicyConfig(
      howLongToBlackList: LeaderSelectionPolicyConfig.HowLongToBlacklist,
      howManyCanWeBlacklist: LeaderSelectionPolicyConfig.HowManyCanWeBlacklist,
  )

  implicit val configCantonConfigValidator: CantonConfigValidator[BftBlockOrdererConfig] =
    CantonConfigValidatorDerivation[BftBlockOrdererConfig]

  val DefaultAuthenticationTokenManagerConfig: AuthenticationTokenManagerConfig =
    AuthenticationTokenManagerConfig()

  final case class P2PNetworkConfig(
      serverEndpoint: P2PServerConfig,
      endpointAuthentication: P2PNetworkAuthenticationConfig = P2PNetworkAuthenticationConfig(),
      peerEndpoints: Seq[P2PEndpointConfig] = Seq.empty,
      overwriteStoredEndpoints: Boolean = false,
  ) extends UniformCantonConfigValidation
  object P2PNetworkConfig {
    implicit val p2pNetworkCantonConfigValidator: CantonConfigValidator[P2PNetworkConfig] =
      CantonConfigValidatorDerivation[P2PNetworkConfig]
  }

  final case class P2PNetworkAuthenticationConfig(
      authToken: AuthenticationTokenManagerConfig = DefaultAuthenticationTokenManagerConfig,
      enabled: Boolean = true,
  ) extends UniformCantonConfigValidation
  object P2PNetworkAuthenticationConfig {
    implicit val bftNetworAuthenticationCantonConfigValidator
        : CantonConfigValidator[P2PNetworkAuthenticationConfig] =
      CantonConfigValidatorDerivation[P2PNetworkAuthenticationConfig]
  }

  /** The [[externalAddress]], [[externalPort]] and [[externalTlsConfig]] must be configured
    * correctly for the client to correctly authenticate the server, as the client tells the server
    * its endpoint for authentication based on this information.
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
      override val maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize,
  ) extends ServerConfig
      with UniformCantonConfigValidation {

    override val jwtTimestampLeeway: Option[JwtTimestampLeeway] = None
    override val keepAliveServer: Option[BasicKeepAliveServerConfig] = None
    override val authServices: Seq[AuthServiceConfig] = Seq.empty
    override val adminToken: Option[String] = None

    override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.sslContext(_))

    override def serverCertChainFile: Option[PemFileOrString] = tls.map(_.certChainFile)

    private[core] def serverToClientAuthenticationEndpointConfig: P2PEndpointConfig =
      P2PEndpointConfig(externalAddress, externalPort, externalTlsConfig)
  }
  object P2PServerConfig {
    implicit val adminServerConfigCantonConfigValidator: CantonConfigValidator[P2PServerConfig] = {
      import CantonConfigValidatorInstances.*
      CantonConfigValidatorDerivation[P2PServerConfig]
    }
  }

  final case class P2PEndpointConfig(
      override val address: String,
      override val port: Port,
      override val tlsConfig: Option[TlsClientConfig] = Some(
        TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = true)
      ),
  ) extends ClientConfig
      with UniformCantonConfigValidation {
    override val keepAliveClient: Option[KeepAliveClientConfig] = None
  }
  object P2PEndpointConfig {
    implicit val p2pEndpointConfigCantonConfigValidator
        : CantonConfigValidator[P2PEndpointConfig] = {
      import CantonConfigValidatorInstances.*
      CantonConfigValidatorDerivation[P2PEndpointConfig]
    }
  }

  final case class EndpointId(
      address: String,
      port: Port,
      tls: Boolean,
  )

  final case class PruningConfig(
      enabled: Boolean = DefaultPruningConfig.enabled,
      retentionPeriod: FiniteDuration = DefaultPruningConfig.retentionPeriod,
      minNumberOfBlocksToKeep: Int = DefaultPruningConfig.minNumberOfBlocksToKeep,
      pruningFrequency: FiniteDuration = DefaultPruningConfig.pruningFrequency,
  ) extends UniformCantonConfigValidation
  object PruningConfig {
    implicit val pruningConfigCantonConfigValidator: CantonConfigValidator[PruningConfig] =
      CantonConfigValidatorDerivation[PruningConfig]
  }

  sealed trait LeaderSelectionPolicyConfig extends UniformCantonConfigValidation

  object LeaderSelectionPolicyConfig {
    implicit val leaderSelectionPolicyConfigValidator
        : CantonConfigValidator[LeaderSelectionPolicyConfig] =
      CantonConfigValidatorDerivation[LeaderSelectionPolicyConfig]

    final case object Simple extends LeaderSelectionPolicyConfig

    final case object Blacklisting extends LeaderSelectionPolicyConfig

    sealed trait HowLongToBlacklist extends UniformCantonConfigValidation {
      def compute(failedEpochSoFar: Long): BlacklistStatus
    }

    object HowLongToBlacklist {
      implicit val howLongToBlacklistConfigValidator: CantonConfigValidator[HowLongToBlacklist] =
        CantonConfigValidatorDerivation[HowLongToBlacklist]

      case object Linear extends HowLongToBlacklist {
        override def compute(failedEpochSoFar: Long): BlacklistStatus =
          BlacklistStatus.Blacklisted(failedEpochSoFar, failedEpochSoFar)
      }

      case object NoBlacklisting extends HowLongToBlacklist {
        override def compute(failedEpochSoFar: Long): BlacklistStatus = BlacklistStatus.Clean
      }
    }

    sealed trait HowManyCanWeBlacklist extends UniformCantonConfigValidation {
      def howManyCanWeBlacklist(orderingTopology: OrderingTopology): Int
    }

    object HowManyCanWeBlacklist {
      implicit val howManyCanWeBlacklist: CantonConfigValidator[HowManyCanWeBlacklist] =
        CantonConfigValidatorDerivation[HowManyCanWeBlacklist]

      case object NumFaultsTolerated extends HowManyCanWeBlacklist {
        override def howManyCanWeBlacklist(orderingTopology: OrderingTopology): Int =
          orderingTopology.numFaultsTolerated
      }

      case object NoBlacklisting extends HowManyCanWeBlacklist {
        override def howManyCanWeBlacklist(orderingTopology: OrderingTopology): Int = 0
      }
    }
  }
}
