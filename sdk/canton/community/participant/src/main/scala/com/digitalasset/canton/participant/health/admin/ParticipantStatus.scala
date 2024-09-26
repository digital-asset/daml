// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.health.admin

import com.digitalasset.canton.admin.participant.v30.ConnectedDomain
import com.digitalasset.canton.admin.participant.v30 as participantV30
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.health.admin.data.NodeStatus.{
  multiline,
  portsString,
  protocolVersionsString,
}
import com.digitalasset.canton.health.admin.data.{NodeStatus, TopologyQueueStatus}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.sync.SyncDomain.SubmissionReady
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration
import scala.collection.immutable

final case class ParticipantStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    connectedDomains: Map[DomainId, SubmissionReady],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    supportedProtocolVersions: Seq[ProtocolVersion],
) extends NodeStatus.Status {
  val id: ParticipantId = ParticipantId(uid)

  private def connectedHealthyDomains: immutable.Iterable[DomainId] = connectedDomains.collect {
    case (domainId, submissionReady) if submissionReady.unwrap => domainId
  }

  private def connectedUnhealthyDomains: immutable.Iterable[DomainId] = connectedDomains.collect {
    case (domainId, submissionReady) if !submissionReady.unwrap => domainId
  }

  override protected def pretty: Pretty[ParticipantStatus] =
    prettyOfString(_ =>
      (Seq(
        s"Participant id: ${id.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected domains: ${multiline(connectedHealthyDomains.map(_.toString))}",
        s"Unhealthy domains: ${multiline(connectedUnhealthyDomains.map(_.toString))}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: ${version.fullVersion}",
      ) ++ protocolVersionsString(supportedProtocolVersions)).mkString(System.lineSeparator())
    )

  def toParticipantStatusProto
      : participantV30.ParticipantStatusResponse.ParticipantStatusResponseStatus = {

    val domains = connectedDomains.map { case (domainId, isHealthy) =>
      val health =
        if (isHealthy.unwrap) participantV30.ConnectedDomain.Health.HEALTH_HEALTHY
        else participantV30.ConnectedDomain.Health.HEALTH_UNHEALTHY

      ConnectedDomain(
        domainId = domainId.toProtoPrimitive,
        health = health,
      )
    }.toList

    participantV30.ParticipantStatusResponse.ParticipantStatusResponseStatus(
      commonStatus = Some(toProtoV30),
      connectedDomains = domains,
      active = active,
      supportedProtocolVersions = supportedProtocolVersions.map(_.toProtoPrimitive),
    )
  }
}
