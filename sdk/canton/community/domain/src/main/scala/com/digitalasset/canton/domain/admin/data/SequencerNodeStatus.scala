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
  SequencerAdminStatus,
  SequencerHealthStatus,
  SimpleStatus,
  TopologyQueueStatus,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import io.scalaland.chimney.dsl.*

import java.time.Duration

final case class SequencerNodeStatus(
    uid: UniqueIdentifier,
    domainId: DomainId,
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
    admin: Option[SequencerAdminStatus],
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    protocolVersion: ProtocolVersion,
) extends NodeStatus.Status {

  override def active: Boolean = sequencer.isActive

  override def pretty: Pretty[SequencerNodeStatus] =
    prettyOfString(_ =>
      (
        Seq(
          s"Sequencer id: ${uid.toProtoPrimitive}",
          s"Domain id: ${domainId.toProtoPrimitive}",
          show"Uptime: $uptime",
          s"Ports: ${portsString(ports)}",
          s"Connected Participants: ${multiline(connectedParticipants.map(_.toString))}",
          show"Sequencer: $sequencer",
          s"details-extra: ${sequencer.details}",
          s"Components: ${multiline(components.map(_.toString))}",
          s"Version: ${version.fullVersion}",
          s"Protocol version: $protocolVersion",
        )
          ++ admin.toList.map(adminStatus =>
            s"Accepts admin changes: ${adminStatus.acceptsAdminChanges}"
          ),
      ).mkString(System.lineSeparator())
    )

  override def toProtoV0: v0.NodeStatus.Status = {
    val participants = connectedParticipants.map(_.toProtoPrimitive)

    this
      .into[SimpleStatus]
      .enableMethodAccessors
      .transform
      .toProtoV0
      .copy(
        extra = v0
          .SequencerNodeStatus(
            participants,
            sequencer.toProtoV0.some,
            domainId.toProtoPrimitive,
            admin.map(_.toProtoV0),
          )
          .toByteString
      )
  }

  def toSequencerStatusProto: domainV0.SequencerStatusResponse.SequencerStatusResponseStatus =
    domainV0.SequencerStatusResponse.SequencerStatusResponseStatus(
      commonStatus = toProtoV1.some,
      connectedParticipantsUid = connectedParticipants.map(_.uid.toProtoPrimitive),
      sequencer = sequencer.toProtoV0.some,
      domainId = domainId.toProtoPrimitive,
      admin = admin.map(_.toProtoV0),
      protocolVersion = protocolVersion.toProtoPrimitive,
    )
}
