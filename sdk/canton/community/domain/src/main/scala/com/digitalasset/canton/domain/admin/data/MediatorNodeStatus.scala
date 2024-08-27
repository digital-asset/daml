// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.data

import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.admin.v0 as domainV0
import com.digitalasset.canton.health.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.health.{
  ComponentStatus,
  NodeStatus,
  SimpleStatus,
  TopologyQueueStatus,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import io.scalaland.chimney.dsl.*

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

  override def toProtoV0: v0.NodeStatus.Status =
    this
      .into[SimpleStatus]
      .transform
      .toProtoV0
      .copy(
        extra = v0
          .MediatorNodeStatus(domainId.toProtoPrimitive)
          .toByteString
      )

  def toMediatorStatusProto: domainV0.MediatorStatusResponse.MediatorStatusResponseStatus =
    domainV0.MediatorStatusResponse.MediatorStatusResponseStatus(
      commonStatus = toProtoV1.some,
      domainId = domainId.toProtoPrimitive,
      protocolVersion = protocolVersion.toProtoPrimitive,
    )
}
