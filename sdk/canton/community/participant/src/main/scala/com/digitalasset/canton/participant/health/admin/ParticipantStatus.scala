// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.health.admin

import com.digitalasset.canton.admin.participant.v30 as participantV30
import com.digitalasset.canton.admin.participant.v30.ConnectedSynchronizer
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.health.admin.data.NodeStatus.{
  multiline,
  portsString,
  protocolVersionsString,
}
import com.digitalasset.canton.health.admin.data.{NodeStatus, TopologyQueueStatus}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer.SubmissionReady
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration
import scala.collection.immutable

final case class ParticipantStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    connectedSynchronizers: Map[PhysicalSynchronizerId, SubmissionReady],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    supportedProtocolVersions: Seq[ProtocolVersion],
) extends NodeStatus.Status {
  val id: ParticipantId = ParticipantId(uid)

  private def connectedHealthySynchronizers: immutable.Iterable[PhysicalSynchronizerId] =
    connectedSynchronizers.collect {
      case (synchronizerId, submissionReady) if submissionReady.unwrap => synchronizerId
    }

  private def connectedUnhealthySynchronizers: immutable.Iterable[PhysicalSynchronizerId] =
    connectedSynchronizers.collect {
      case (synchronizerId, submissionReady) if !submissionReady.unwrap => synchronizerId
    }

  override protected def pretty: Pretty[ParticipantStatus] =
    prettyOfString(_ =>
      (Seq(
        s"Participant id: ${id.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected synchronizers: ${multiline(connectedHealthySynchronizers.map(_.toString))}",
        s"Unhealthy synchronizers: ${multiline(connectedUnhealthySynchronizers.map(_.toString))}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: ${version.fullVersion}",
      ) ++ protocolVersionsString(supportedProtocolVersions)).mkString(System.lineSeparator())
    )

  def toParticipantStatusProto
      : participantV30.ParticipantStatusResponse.ParticipantStatusResponseStatus = {

    val synchronizers = connectedSynchronizers.map { case (synchronizerId, isHealthy) =>
      val health =
        if (isHealthy.unwrap) participantV30.ConnectedSynchronizer.Health.HEALTH_HEALTHY
        else participantV30.ConnectedSynchronizer.Health.HEALTH_UNHEALTHY

      ConnectedSynchronizer(
        physicalSynchronizerId = synchronizerId.toProtoPrimitive,
        health = health,
      )
    }.toList

    participantV30.ParticipantStatusResponse.ParticipantStatusResponseStatus(
      commonStatus = Some(toProtoV30),
      connectedSynchronizers = synchronizers,
      active = active,
      supportedProtocolVersions = supportedProtocolVersions.map(_.toProtoPrimitive),
    )
  }
}
