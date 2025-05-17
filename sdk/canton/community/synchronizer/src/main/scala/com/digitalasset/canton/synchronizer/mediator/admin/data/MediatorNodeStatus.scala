// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.admin.data

import cats.syntax.option.*
import com.digitalasset.canton.admin.mediator.v30 as mediatorV30
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.health.admin.data.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.admin.data.{NodeStatus, TopologyQueueStatus}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration

final case class MediatorNodeStatus(
    uid: UniqueIdentifier,
    synchronizerId: PhysicalSynchronizerId,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    protocolVersion: ProtocolVersion, // TODO(#25482) Reduce duplication in parameters
) extends NodeStatus.Status {
  override protected def pretty: Pretty[MediatorNodeStatus] =
    prettyOfString(_ =>
      Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        s"Synchronizer id: ${synchronizerId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: ${version.fullVersion}",
        s"Protocol version: $protocolVersion",
      ).mkString(System.lineSeparator())
    )

  def toMediatorStatusProto: mediatorV30.MediatorStatusResponse.MediatorStatusResponseStatus =
    mediatorV30.MediatorStatusResponse.MediatorStatusResponseStatus(
      commonStatus = toProtoV30.some,
      physicalSynchronizerId = synchronizerId.toProtoPrimitive,
      protocolVersion = protocolVersion.toProtoPrimitive,
    )
}
