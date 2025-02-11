// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config
import com.digitalasset.canton.config.*
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.synchronizer.sequencer.{
  CommunitySequencerConfig,
  SequencerHealthConfig,
}
import monocle.macros.syntax.lens.*

/** CommunitySequencerNodeConfig supports and defaults to auto-init
  */
final case class CommunitySequencerNodeConfig(
    override val init: SequencerNodeInitConfig = SequencerNodeInitConfig(),
    override val publicApi: PublicServerConfig = PublicServerConfig(),
    override val adminApi: AdminServerConfig = AdminServerConfig(),
    override val storage: StorageConfig = StorageConfig.Memory(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val sequencer: CommunitySequencerConfig = CommunitySequencerConfig.default,
    override val auditLogging: Boolean = false,
    override val timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val parameters: SequencerNodeParameterConfig = SequencerNodeParameterConfig(),
    override val health: SequencerHealthConfig = SequencerHealthConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    override val topology: TopologyConfig = TopologyConfig(),
    override val trafficConfig: SequencerTrafficConfig = SequencerTrafficConfig(),
) extends SequencerNodeConfigCommon(
      init,
      publicApi,
      adminApi,
      storage,
      crypto,
      sequencer,
      auditLogging,
      timeTracker,
      sequencerClient,
      parameters,
      health,
      monitoring,
      trafficConfig,
    )
    with ConfigDefaults[DefaultPorts, CommunitySequencerNodeConfig] {

  override val nodeTypeName: String = "sequencer"

  override def withDefaults(ports: DefaultPorts): CommunitySequencerNodeConfig =
    this
      .focus(_.publicApi.internalPort)
      .modify(ports.sequencerPublicApiPort.setDefaultPort)
      .focus(_.adminApi.internalPort)
      .modify(ports.sequencerAdminApiPort.setDefaultPort)
}

/** SequencerNodeInitConfig supports auto-init
  */
final case class SequencerNodeInitConfig(
    identity: Option[InitConfigBase.Identity] = Some(InitConfigBase.Identity()),
    state: Option[StateConfig] = None,
) extends config.InitConfigBase
