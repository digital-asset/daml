// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config.*
import com.digitalasset.canton.domain.config.CommunityPublicServerConfig
import com.digitalasset.canton.domain.sequencing.sequencer.{
  CommunitySequencerConfig,
  SequencerHealthConfig,
}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import monocle.macros.syntax.lens.*

import java.io.File

/** CommunitySequencerNodeXConfig supports and defaults to auto-init as compared to the "classic" SequencerNodeConfig
  */
final case class CommunitySequencerNodeXConfig(
    override val init: SequencerNodeInitXConfig = SequencerNodeInitXConfig(),
    override val publicApi: CommunityPublicServerConfig = CommunityPublicServerConfig(),
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig(),
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val sequencer: CommunitySequencerConfig = CommunitySequencerConfig.Database(),
    override val auditLogging: Boolean = false,
    override val serviceAgreement: Option[File] = None,
    override val timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    parameters: SequencerNodeParameterConfig = SequencerNodeParameterConfig(),
    override val health: SequencerHealthConfig = SequencerHealthConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    override val topologyX: TopologyXConfig = TopologyXConfig(),
) extends SequencerNodeConfigCommon(
      init,
      publicApi,
      adminApi,
      storage,
      crypto,
      sequencer,
      auditLogging,
      serviceAgreement,
      timeTracker,
      sequencerClient,
      caching,
      parameters,
      health,
      monitoring,
    )
    with ConfigDefaults[DefaultPorts, CommunitySequencerNodeXConfig] {

  override val nodeTypeName: String = "sequencerx"

  override def withDefaults(ports: DefaultPorts): CommunitySequencerNodeXConfig = {
    this
      .focus(_.publicApi.internalPort)
      .modify(ports.sequencerXPublicApiPort.setDefaultPort)
      .focus(_.adminApi.internalPort)
      .modify(ports.sequencerXAdminApiPort.setDefaultPort)
  }
}

/** SequencerNodeInitXConfig supports auto-init unlike "classic" SequencerNodeInitConfig
  */
final case class SequencerNodeInitXConfig(
    identity: Option[InitConfigBase.Identity] = Some(InitConfigBase.Identity())
) extends SequencerNodeInitConfigCommon() {

  /** the following case class match will help us detect any additional configuration options added
    * for "classic" non-X nodes that may apply to X-nodes as well.
    */
  private def _completenessCheck(
      classicConfig: CommunitySequencerNodeInitConfig
  ): SequencerNodeInitXConfig =
    classicConfig match {
      case CommunitySequencerNodeInitConfig() =>
        SequencerNodeInitXConfig( /* identity */ )
    }
}
