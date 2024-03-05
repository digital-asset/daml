// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.config.*
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.health.admin.data.MediatorNodeStatus
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.{Clock, HasUptime}
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.Future

abstract class MediatorNodeConfigCommon(
    val adminApi: AdminServerConfig,
    val storage: StorageConfig,
    val crypto: CryptoConfig,
    val init: InitConfig,
    val timeTracker: DomainTimeTrackerConfig,
    val sequencerClient: SequencerClientConfig,
    val caching: CachingConfigs,
    val parameters: MediatorNodeParameterConfig,
    val monitoring: NodeMonitoringConfig,
) extends LocalNodeConfig {

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  def toRemoteConfig: RemoteMediatorConfig = RemoteMediatorConfig(adminApi.clientConfig)

  def replicationEnabled: Boolean
}

/** Various parameters for non-standard mediator settings
  *
  * @param dontWarnOnDeprecatedPV if true, then this mediator will not emit a warning when connecting to a sequencer using a deprecated protocol version.
  */
final case class MediatorNodeParameterConfig(
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    override val devVersionSupport: Boolean = true,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    override val initialProtocolVersion: ProtocolVersion = ProtocolVersion.latest,
    override val batching: BatchingConfig = BatchingConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val useNewTrafficControl: Boolean = false,
) extends ProtocolConfig
    with LocalNodeParametersConfig

final case class MediatorNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters

final case class RemoteMediatorConfig(
    adminApi: ClientConfig
) extends NodeConfig {
  override def clientAdminApi: ClientConfig = adminApi
}

// TODO(#15161): Fold MediatorNodeCommon into MediatorNodeX
class MediatorNodeCommon(
    config: MediatorNodeConfigCommon,
    mediatorId: MediatorId,
    domainId: DomainId,
    replicaManager: MediatorReplicaManager,
    storage: Storage,
    override protected val clock: Clock,
    override protected val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends CantonNode
    with NamedLogging
    with HasUptime {

  def isActive: Boolean = replicaManager.isActive

  def status: Future[MediatorNodeStatus] = {
    val ports = Map("admin" -> config.adminApi.port)
    Future.successful(
      MediatorNodeStatus(
        mediatorId.uid,
        domainId,
        uptime(),
        ports,
        replicaManager.isActive,
        replicaManager.getTopologyQueueStatus,
        healthData,
      )
    )
  }

  override def close(): Unit =
    Lifecycle.close(
      replicaManager,
      storage,
    )(logger)
}
