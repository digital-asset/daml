// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config.*
import com.digitalasset.canton.domain.config.PublicServerConfig
import com.digitalasset.canton.domain.sequencing.sequencer.{SequencerConfig, SequencerHealthConfig}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig

import java.io.File

abstract class SequencerNodeConfigCommon(
    val init: SequencerNodeInitConfigCommon,
    val publicApi: PublicServerConfig,
    val adminApi: AdminServerConfig,
    val storage: StorageConfig,
    val crypto: CryptoConfig,
    val sequencer: SequencerConfig,
    val auditLogging: Boolean,
    val serviceAgreement: Option[File],
    val timeTracker: DomainTimeTrackerConfig,
    val sequencerClient: SequencerClientConfig,
    val caching: CachingConfigs,
    parameters: SequencerNodeParameterConfig,
    val health: SequencerHealthConfig,
    val monitoring: NodeMonitoringConfig,
) extends LocalNodeConfig {

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  def toRemoteConfig: RemoteSequencerConfig.Grpc =
    RemoteSequencerConfig.Grpc(
      adminApi.clientConfig,
      publicApi.toSequencerConnectionConfig,
      monitoring.grpcHealthServer.map(_.toRemoteConfig),
    )
}
