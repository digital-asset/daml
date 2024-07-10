// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.sequencing.client.SequencerClientConfig

trait NodeConfig {
  def clientAdminApi: ClientConfig
}

trait LocalNodeConfig extends NodeConfig {

  /** Human readable name for the type of node used for displaying config error messages */
  val nodeTypeName: String

  def init: InitConfigBase
  def adminApi: AdminServerConfig
  def storage: StorageConfig
  def crypto: CryptoConfig
  def sequencerClient: SequencerClientConfig
  def monitoring: NodeMonitoringConfig
  def topology: TopologyConfig

  def parameters: LocalNodeParametersConfig

}

trait LocalNodeParametersConfig {
  def batching: BatchingConfig

  /** Various cache sizes */
  def caching: CachingConfigs
  def useUnifiedSequencer: Boolean
  def alphaVersionSupport: Boolean
  def watchdog: Option[WatchdogConfig]
}

trait CommunityLocalNodeConfig extends LocalNodeConfig {
  override def storage: CommunityStorageConfig
}
