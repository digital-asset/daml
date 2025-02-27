// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.SequencerHighAvailabilityConfig
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.synchronizer.sequencer.{SequencerConfig, SequencerHealthConfig}
import monocle.macros.syntax.lens.*

/** Configuration parameters for a single sequencer node
  *
  * @param init
  *   determines how this node is initialized
  * @param publicApi
  *   The configuration for the public sequencer API
  * @param adminApi
  *   parameters of the interface used to administrate the sequencer
  * @param storage
  *   determines how the sequencer stores this state
  * @param crypto
  *   determines the algorithms used for signing, hashing, and encryption
  * @param sequencer
  *   determines the type of sequencer
  * @param health
  *   Health check related sequencer config
  * @param monitoring
  *   Monitoring configuration for a canton node.
  * @param parameters
  *   general sequencer node parameters
  * @param replication
  *   replication configuration used for node startup
  * @param topology
  *   configuration for the topology service of the sequencer
  * @param trafficConfig
  *   Configuration for the traffic purchased entry manager.
  */
final case class SequencerNodeConfig(
    override val init: SequencerNodeInitConfig = SequencerNodeInitConfig(),
    publicApi: PublicServerConfig = PublicServerConfig(),
    override val adminApi: AdminServerConfig = AdminServerConfig(),
    override val storage: StorageConfig = StorageConfig.Memory(),
    override val crypto: CryptoConfig = CryptoConfig(),
    sequencer: SequencerConfig = SequencerConfig.default,
    timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val parameters: SequencerNodeParameterConfig = SequencerNodeParameterConfig(),
    health: SequencerHealthConfig = SequencerHealthConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    replication: Option[ReplicationConfig] = None,
    override val topology: TopologyConfig = TopologyConfig(),
    trafficConfig: SequencerTrafficConfig = SequencerTrafficConfig(),
) extends LocalNodeConfig
    with ConfigDefaults[DefaultPorts, SequencerNodeConfig]
    with UniformCantonConfigValidation {

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  override def nodeTypeName: String = "sequencer"

  def toRemoteConfig: RemoteSequencerConfig =
    RemoteSequencerConfig(
      adminApi.clientConfig,
      publicApi.clientConfig,
      monitoring.grpcHealthServer.map(_.toRemoteConfig),
    )

  override def withDefaults(
      ports: DefaultPorts,
      edition: CantonEdition,
  ): SequencerNodeConfig = {
    val withDefaults = this
      .focus(_.publicApi.internalPort)
      .modify(ports.sequencerPublicApiPort.setDefaultPort)
      .focus(_.adminApi.internalPort)
      .modify(ports.sequencerAdminApiPort.setDefaultPort)
      .focus(_.sequencer)
      .modify {
        case db: SequencerConfig.Database =>
          val enabled =
            ReplicationConfig.withDefault(storage, db.highAvailability.flatMap(_.enabled), edition)
          db
            .focus(_.highAvailability)
            .modify(
              _.map(_.copy(enabled = enabled))
                .orElse(
                  enabled.map(enabled => SequencerHighAvailabilityConfig(enabled = Some(enabled)))
                )
            )
        case other => other
      }
    withDefaults
      .focus(_.replication)
      .modify(replication =>
        // The block sequencer does not support replicas, so we must not enable replication
        // even if the storage supports it (sse #13844).
        if (withDefaults.sequencer.supportsReplicas)
          ReplicationConfig.withDefaultO(storage, replication, edition)
        else replication.map(_.copy(enabled = Some(false)))
      )
  }
}

object SequencerNodeConfig {
  implicit val sequencerNodeConfigCantonConfigValidator
      : CantonConfigValidator[SequencerNodeConfig] =
    CantonConfigValidatorDerivation[SequencerNodeConfig]
}

/** SequencerNodeInitConfig supports and defaults to auto-init
  */
final case class SequencerNodeInitConfig(
    identity: Option[InitConfigBase.Identity] = Some(InitConfigBase.Identity()),
    state: Option[StateConfig] = None,
) extends config.InitConfigBase
    with UniformCantonConfigValidation

object SequencerNodeInitConfig {
  implicit val sequencerNodeInitConfigCantonConfigValidator
      : CantonConfigValidator[SequencerNodeInitConfig] =
    CantonConfigValidatorDerivation[SequencerNodeInitConfig]
}
