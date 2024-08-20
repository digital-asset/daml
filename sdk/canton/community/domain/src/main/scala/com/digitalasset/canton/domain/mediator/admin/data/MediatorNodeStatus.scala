// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.data

import cats.syntax.option.*
import com.digitalasset.canton.admin.domain.v30 as domainV30
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.health.admin.data.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.admin.data.{NodeStatus, TopologyQueueStatus}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration

final case class MediatorNodeStatus(
    uid: UniqueIdentifier,
    domainId: DomainId,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    protocolVersion: ProtocolVersion,
) extends NodeStatus.Status {
  override def pretty: Pretty[MediatorNodeStatus] =
    prettyOfString(_ =>
      Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        s"Domain id: ${domainId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: ${version.fullVersion}",
        s"Protocol version: $protocolVersion",
      ).mkString(System.lineSeparator())
    )

  def toMediatorStatusProto: domainV30.MediatorStatusResponse.MediatorStatusResponseStatus =
    domainV30.MediatorStatusResponse.MediatorStatusResponseStatus(
      commonStatus = toProtoV30.some,
      domainId = domainId.toProtoPrimitive,
      protocolVersion = protocolVersion.toProtoPrimitive,
    )
}
