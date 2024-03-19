// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config.*
import com.digitalasset.canton.domain.config.CommunityPublicServerConfig
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.domain.sequencing.sequencer.{
  CommunitySequencerConfig,
  SequencerHealthConfig,
}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import monocle.macros.syntax.lens.*

/** CommunitySequencerNodeXConfig supports and defaults to auto-init as compared to the "classic" SequencerNodeConfig
  */
final case class CommunitySequencerNodeXConfig(
    override val init: SequencerNodeInitXConfig = SequencerNodeInitXConfig(),
    override val publicApi: CommunityPublicServerConfig = CommunityPublicServerConfig(),
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig(),
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val sequencer: CommunitySequencerConfig = CommunitySequencerConfig.default,
    override val auditLogging: Boolean = false,
    override val timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
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
    with ConfigDefaults[DefaultPorts, CommunitySequencerNodeXConfig] {

  override val nodeTypeName: String = "sequencerx"

  override def withDefaults(ports: DefaultPorts): CommunitySequencerNodeXConfig = {
    this
      .focus(_.publicApi.internalPort)
      .modify(ports.sequencerPublicApiPort.setDefaultPort)
      .focus(_.adminApi.internalPort)
      .modify(ports.sequencerAdminApiPort.setDefaultPort)
  }
}

/** SequencerNodeInitXConfig supports auto-init unlike "classic" SequencerNodeInitConfig
  */
final case class SequencerNodeInitXConfig(
    identity: Option[InitConfigBase.Identity] = Some(InitConfigBase.Identity())
) extends SequencerNodeInitConfigCommon()
