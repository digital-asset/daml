// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.{CantonConfig, LocalNodeConfig, StorageConfig}
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnvironmentSetupPlugin,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig

/** Test plugin for replicated nodes by copying the storage configuration of the active node to all
  * replicas.
  */
class UseSharedStorage[C <: LocalNodeConfig](
    sourceNodeNames: String,
    targetNodeNames: Seq[String],
    getNodeConfig: CantonConfig => String => C,
    updateStorageConfig: C => StorageConfig => C,
    updateAllNodes: ((String, C) => C) => ConfigTransform,
    override protected val loggerFactory: NamedLoggerFactory,
) extends EnvironmentSetupPlugin {

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    val activeStorageConfig = getNodeConfig(config)(sourceNodeNames).storage
    val sharedConfigTransform = updateAllNodes {
      case (nodeName, config) if targetNodeNames.contains(nodeName) =>
        updateStorageConfig(config)(activeStorageConfig)
      case (_, config) => config
    }
    sharedConfigTransform(config)
  }
}

object UseSharedStorage {

  def forParticipants(
      sourceNodeName: String,
      targetNodeNames: Seq[String],
      loggerFactory: NamedLoggerFactory,
  ): UseSharedStorage[LocalParticipantConfig] =
    new UseSharedStorage[LocalParticipantConfig](
      sourceNodeName,
      targetNodeNames,
      config => name => config.participantsByString(name),
      config => storage => config.copy(storage = storage),
      ConfigTransforms.updateAllParticipantConfigs,
      loggerFactory,
    )

  def forSequencers(
      sourceNodeName: String,
      targetNodeNames: Seq[String],
      loggerFactory: NamedLoggerFactory,
  ): UseSharedStorage[SequencerNodeConfig] =
    new UseSharedStorage[SequencerNodeConfig](
      sourceNodeName,
      targetNodeNames,
      config => name => config.sequencersByString(name),
      config => storage => config.copy(storage = storage),
      ConfigTransforms.updateAllSequencerConfigs,
      loggerFactory,
    )

  def forMediators(
      sourceNodeName: String,
      targetNodeNames: Seq[String],
      loggerFactory: NamedLoggerFactory,
  ): UseSharedStorage[MediatorNodeConfig] =
    new UseSharedStorage[MediatorNodeConfig](
      sourceNodeName,
      targetNodeNames,
      config => name => config.mediatorsByString(name),
      config => storage => config.copy(storage = storage),
      ConfigTransforms.updateAllMediatorConfigs,
      loggerFactory,
    )

}
