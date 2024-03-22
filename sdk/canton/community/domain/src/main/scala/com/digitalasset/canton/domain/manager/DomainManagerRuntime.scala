// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.manager

import com.digitalasset.canton.domain.initialization.TopologyManagementComponents
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import io.grpc.ServerServiceDefinition

/** Domain manager component and its supporting services */
trait DomainManagerRuntime extends FlagCloseable {
  def domainTopologyManager: DomainTopologyManager
  def topologyManagementComponents: TopologyManagementComponents

  def writeService: ServerServiceDefinition
  def initializationService: ServerServiceDefinition

  def getTopologyQueueStatus: TopologyQueueStatus = TopologyQueueStatus(
    manager = domainTopologyManager.queueSize,
    dispatcher = topologyManagementComponents.dispatcher.queueSize,
    clients = topologyManagementComponents.client.numPendingChanges,
  )

  override protected def onClosed(): Unit = Lifecycle.close(
    domainTopologyManager,
    topologyManagementComponents,
  )(logger)
}
