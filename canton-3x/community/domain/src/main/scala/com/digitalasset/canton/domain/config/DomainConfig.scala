// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config

import com.digitalasset.canton.config.DeprecatedConfigUtils.DeprecatedFieldsFor
import com.digitalasset.canton.config.LocalNodeConfig.LocalNodeConfigDeprecationImplicits
import com.digitalasset.canton.config.RequireTypes.{
  ExistingFile,
  NonNegativeInt,
  Port,
  PositiveDouble,
}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.domain.sequencing.sequencer.CommunitySequencerConfig
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import io.netty.handler.ssl.SslContext
import monocle.macros.syntax.lens.*

/** The public server configuration ServerConfig used by the domain.
  *
  * TODO(i4056): Client authentication over TLS is currently unsupported,
  *  because there is a token based protocol to authenticate clients. This may change in the future.
  */
trait PublicServerConfig extends ServerConfig {

  def tls: Option[TlsBaseServerConfig]

  /** Expiration time for a nonce that is generated for an
    * authentication challenge. as an authentication request is
    * expected to be followed up with almost immediately to generate
    * an authentication token the nonce expiry should be short. the
    * nonce is automatically invalided on use.
    */
  def nonceExpirationTime: NonNegativeFiniteDuration

  /** Expiration time for authentication tokens. Tokens are used to authenticate participants.
    * Choose a shorter time for better security and a longer time for better performance.
    */
  def tokenExpirationTime: NonNegativeFiniteDuration

  lazy val clientConfig: ClientConfig =
    ClientConfig(address, port, tls.map(c => TlsClientConfig(Some(c.certChainFile), None)))

  override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.baseSslContext)

  override def serverCertChainFile: Option[ExistingFile] = tls.map(_.certChainFile)

  /** overrides the default maximum request size in bytes on the sequencer node */
  def overrideMaxRequestSize: Option[NonNegativeInt]

  /** This setting has no effect. Therfore hardcoding it to 0.
    */
  override final def maxInboundMessageSize: NonNegativeInt = NonNegativeInt.tryCreate(0)
  def connection: String = {
    val scheme = tls.fold("http")(_ => "https")
    s"$scheme://$address:$port"
  }
}

final case class CommunityPublicServerConfig(
    override val address: String = "127.0.0.1",
    override val internalPort: Option[Port] = None,
    override val tls: Option[TlsBaseServerConfig] = None,
    override val keepAliveServer: Option[KeepAliveServerConfig] = Some(KeepAliveServerConfig()),
    override val nonceExpirationTime: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofMinutes(1),
    override val tokenExpirationTime: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofHours(1),
    override val overrideMaxRequestSize: Option[NonNegativeInt] = None,
) extends PublicServerConfig
    with CommunityServerConfig

object DomainBaseConfig {

  // TODO(i10108): remove when backwards compatibility can be discarded
  /** Adds deprecations specific to DomainBaseConfig
    * We need to manually combine it with the upstream deprecations from LocalNodeConfig
    * in order to not lose them.
    */
  trait DomainBaseConfigDeprecationImplicits extends LocalNodeConfigDeprecationImplicits {
    implicit def deprecatedDomainBaseConfig[X <: DomainBaseConfig]: DeprecatedFieldsFor[X] =
      new DeprecatedFieldsFor[DomainBaseConfig] {
        override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
          DeprecatedConfigUtils.MovedConfigPath(
            "domain-parameters",
            "init.domain-parameters",
          )
        ) ++ deprecatedLocalNodeConfig.movedFields

        override def deprecatePath: List[DeprecatedConfigUtils.DeprecatedConfigPath[_]] = List(
          DeprecatedConfigUtils
            .DeprecatedConfigPath[Boolean]("domain-parameters.unique-contract-keys", "2.7.0"),
          DeprecatedConfigUtils
            .DeprecatedConfigPath[Boolean]("init.domain-parameters.unique-contract-keys", "2.7.0"),
        )
      }
  }
}

trait DomainBaseConfig extends LocalNodeConfig {

  /** determines how this node is initialized */
  def init: DomainInitConfig

  /** if enabled, selected events will be logged by the ParticipantAuditor class */
  def auditLogging: Boolean

  /** parameters of the interface used for administrating the domain */
  def adminApi: AdminServerConfig

  /** determines how the domain stores received messages and state */
  def storage: StorageConfig

  /** determines the crypto provider used for signing, hashing, and encryption and its configuration */
  def crypto: CryptoConfig

  /** determines how the domain performs topology management */
  def topology: TopologyConfig

  /** Configuration of parameters related to time tracking using the domain sequencer. Used by the IDM and optionally the mediator and sequencer components. */
  def timeTracker: DomainTimeTrackerConfig

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  override val topologyX: TopologyXConfig = TopologyXConfig.NotUsed
}

/** Configuration parameters for a single domain. */
trait DomainConfig extends DomainBaseConfig {

  override val nodeTypeName: String = "domain"

  /** parameters of the interface used to communicate with participants */
  def publicApi: PublicServerConfig

  def sequencerConnectionConfig: SequencerConnectionConfig.Grpc =
    publicApi.toSequencerConnectionConfig

  def toRemoteConfig: RemoteDomainConfig = RemoteDomainConfig(
    adminApi = adminApi.clientConfig,
    publicApi = sequencerConnectionConfig,
  )

  /** General node parameters */
  def parameters: DomainNodeParametersConfig

}

/** Various domain node parameters
  *
  * @param maxBurstFactor how forgiving should the participant rate limiting be with respect to bursts
  */
final case class DomainNodeParametersConfig(
    maxBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.5),
    batching: BatchingConfig = BatchingConfig(),
    caching: CachingConfigs = CachingConfigs(),
) extends LocalNodeParametersConfig

final case class CommunityDomainConfig(
    override val init: DomainInitConfig = DomainInitConfig(),
    override val auditLogging: Boolean = false,
    override val publicApi: CommunityPublicServerConfig = CommunityPublicServerConfig(),
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig(),
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val topology: TopologyConfig = TopologyConfig(),
    sequencer: CommunitySequencerConfig.Database = CommunitySequencerConfig.Database(),
    override val timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val parameters: DomainNodeParametersConfig = DomainNodeParametersConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
) extends DomainConfig
    with CommunityLocalNodeConfig
    with ConfigDefaults[DefaultPorts, CommunityDomainConfig] {

  override def withDefaults(ports: DefaultPorts): CommunityDomainConfig = {
    this
      .focus(_.publicApi.internalPort)
      .modify(ports.domainPublicApiPort.setDefaultPort)
      .focus(_.adminApi.internalPort)
      .modify(ports.domainAdminApiPort.setDefaultPort)
  }
}

/** Configuration parameters to connect to a domain running remotely
  *
  * @param adminApi the client settings used to connect to the admin api of the remote process.
  * @param publicApi these details are provided to other nodes to use for how they should
  *                            connect to the sequencer if the domain node has an embedded sequencer
  */
final case class RemoteDomainConfig(
    adminApi: ClientConfig,
    publicApi: SequencerConnectionConfig.Grpc,
) extends NodeConfig {
  override def clientAdminApi: ClientConfig = adminApi
}

/** Configuration parameters for the domain topology manager.
  *
  * @param open if set to true (default), the domain is open. Anyone who is able to connect to a sequencer node
  *             can join. If false, new participants are only accepted if their ParticipantState has already been
  *             registered (equivalent to allow-listing).
  */
final case class TopologyConfig(
    // TODO(i4933) make false default for distributed enterprise deployments and move permissioning to enterprise
    open: Boolean = true
)
