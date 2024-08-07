// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.NodeStatus.{multiline, portsString, protocolVersionsString}
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.health.{
  ComponentStatus,
  NodeStatus,
  SimpleStatus,
  TopologyQueueStatus,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.admin.v0.ConnectedDomain
import com.digitalasset.canton.participant.admin.v0 as participantV0
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import io.scalaland.chimney.dsl.*

import java.time.Duration

final case class ParticipantStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    connectedDomains: Map[DomainId, Boolean],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    supportedProtocolVersions: Seq[ProtocolVersion],
) extends NodeStatus.Status {

  val id: ParticipantId = ParticipantId(uid)

  override def pretty: Pretty[ParticipantStatus] =
    prettyOfString(_ =>
      (Seq(
        s"Participant id: ${id.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected domains: ${multiline(connectedDomains.filter(_._2).map(_._1.toString).toSeq)}",
        s"Unhealthy domains: ${multiline(connectedDomains.filterNot(_._2).map(_._1.toString).toSeq)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: ${version.fullVersion}",
      ) ++ protocolVersionsString(supportedProtocolVersions))
        .mkString(System.lineSeparator())
    )

  override def toProtoV0: v0.NodeStatus.Status = {
    val domains = connectedDomains.map { case (domainId, healthy) =>
      v0.ParticipantStatusInfo.ConnectedDomain(
        domain = domainId.toProtoPrimitive,
        healthy = healthy,
      )
    }.toList

    this
      .into[SimpleStatus]
      .transform
      .toProtoV0
      .copy(extra = v0.ParticipantStatusInfo(domains, active).toByteString)
  }

  override def toProtoV1: v1.Status =
    this
      .into[SimpleStatus]
      .transform
      .toProtoV1

  def toParticipantStatusProto
      : participantV0.ParticipantStatusResponse.ParticipantStatusResponseStatus = {

    val domains = connectedDomains.map { case (domainId, healthy) =>
      ConnectedDomain(
        domainId = domainId.toProtoPrimitive,
        healthy = healthy,
      )
    }.toList

    participantV0.ParticipantStatusResponse.ParticipantStatusResponseStatus(
      commonStatus = Some(toProtoV1),
      connectedDomains = domains,
      active = active,
      supportedProtocolVersions = supportedProtocolVersions.map(_.toProtoPrimitive),
    )
  }
}
