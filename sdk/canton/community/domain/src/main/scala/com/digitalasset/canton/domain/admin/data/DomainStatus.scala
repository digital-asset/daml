// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.data

import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.admin.v0 as domainV0
import com.digitalasset.canton.health.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.health.{
  ComponentStatus,
  NodeStatus,
  SequencerHealthStatus,
  SimpleStatus,
  TopologyQueueStatus,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.topology.{DomainTopologyManagerId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import io.scalaland.chimney.dsl.TransformerOps

import java.time.Duration

final case class DomainStatus(
    domainId: DomainTopologyManagerId,
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    protocolVersion: ProtocolVersion,
) extends NodeStatus.Status {
  val uid: UniqueIdentifier = domainId.uid

  // A domain node is not replicated and always active
  override def active: Boolean = true

  override def pretty: Pretty[DomainStatus] =
    prettyOfString(_ =>
      Seq(
        s"Domain id: ${uid.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected Participants: ${multiline(connectedParticipants.map(_.toString))}",
        show"Sequencer: $sequencer",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: ${version.fullVersion}",
        s"Protocol version: $protocolVersion",
      ).mkString(System.lineSeparator())
    )

  def toProtoV0: v0.NodeStatus.Status = {
    val participants = connectedParticipants.map(_.toProtoPrimitive)
    this
      .into[SimpleStatus]
      .enableMethodAccessors
      .transform
      .toProtoV0
      .copy(
        extra = v0
          .DomainStatusInfo(participants, Some(sequencer.toProtoV0))
          .toByteString
      )
  }

  override def toProtoV1: v1.Status = this
    .into[SimpleStatus]
    .enableMethodAccessors
    .transform
    .toProtoV1

  def toDomainStatusProto: domainV0.DomainStatusResponse.DomainStatusResponseStatus =
    domainV0.DomainStatusResponse.DomainStatusResponseStatus(
      commonStatus = toProtoV1.some,
      connectedParticipantsUid = connectedParticipants.map(_.uid.toProtoPrimitive),
      sequencer = sequencer.toProtoV0.some,
      protocolVersion = protocolVersion.toProtoPrimitive,
    )
}
