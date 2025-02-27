// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.*
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.synchronizer.sequencer.{SequencerConfig, SequencerHealthConfig}

abstract class SequencerNodeConfigCommon(
    override val init: SequencerNodeInitConfig,
    val publicApi: PublicServerConfig,
    override val adminApi: AdminServerConfig,
    override val storage: StorageConfig,
    override val crypto: CryptoConfig,
    val sequencer: SequencerConfig,
    val auditLogging: Boolean,
    val timeTracker: SynchronizerTimeTrackerConfig,
    override val sequencerClient: SequencerClientConfig,
    override val parameters: SequencerNodeParameterConfig,
    val health: SequencerHealthConfig,
    override val monitoring: NodeMonitoringConfig,
    val trafficConfig: SequencerTrafficConfig,
) extends LocalNodeConfig {

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  def toRemoteConfig: RemoteSequencerConfig =
    RemoteSequencerConfig(
      adminApi.clientConfig,
      publicApi.clientConfig,
      monitoring.grpcHealthServer.map(_.toRemoteConfig),
    )
}
