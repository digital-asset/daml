// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.data

import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.admin.v0.SequencerStatusResponse
import com.digitalasset.canton.domain.admin.v0 as domainV0
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.health.admin.data.NodeStatus.{
  multiline,
  portsString,
  protocolVersionString,
  versionString,
}
import com.digitalasset.canton.health.admin.data.{
  NodeStatus,
  SequencerAdminStatus,
  SequencerHealthStatus,
  SimpleStatus,
  TopologyQueueStatus,
}
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
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
    version: Option[ReleaseVersion],
    protocolVersion: Option[ProtocolVersion],
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
        ) ++ versionString(version) ++ protocolVersionString(protocolVersion)
          ++ admin.toList.map(adminStatus =>
            s"Accepts admin changes: ${adminStatus.acceptsAdminChanges}"
          ),
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
          .SequencerNodeStatus(
            participants,
            sequencer.toProtoV0.some,
            domainId.toProtoPrimitive,
            admin.map(_.toProtoV0),
          )
          .toByteString
      )
  }

  override def toProtoV1: v1.Status = this
    .into[SimpleStatus]
    .enableMethodAccessors
    .transform
    .toProtoV1

  def toSequencerStatusProto: domainV0.SequencerStatusResponse.SequencerStatusResponseStatus =
    domainV0.SequencerStatusResponse.SequencerStatusResponseStatus(
      commonStatus = toProtoV1.some,
      connectedParticipantsUid = connectedParticipants.map(_.uid.toProtoPrimitive),
      sequencer = sequencer.toProtoV0.some,
      domainId = domainId.toProtoPrimitive,
      admin = admin.map(_.toProtoV0),
      // TODO(#20463) resolve Option[ProtocolVersion]
      protocolVersion = protocolVersion.getOrElse(ProtocolVersion.latestStable).toProtoPrimitive,
    )
}

object SequencerNodeStatus {
  def fromProtoV0(
      sequencerP: v0.NodeStatus.Status
  ): ParsingResult[SequencerNodeStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(sequencerP)
      sequencerNodeStatus <- ProtoConverter.parse[SequencerNodeStatus, v0.SequencerNodeStatus](
        v0.SequencerNodeStatus.parseFrom,
        fromSequencerNodeStatusProtoV0(_, status),
        sequencerP.extra,
      )
    } yield sequencerNodeStatus

  private def fromSequencerNodeStatusProtoV0(
      sequencerNodeStatusP: v0.SequencerNodeStatus,
      status: SimpleStatus,
  ): ParsingResult[SequencerNodeStatus] =
    for {
      participants <- sequencerNodeStatusP.connectedParticipants.traverse(pId =>
        ParticipantId.fromProtoPrimitive(pId, s"SequencerNodeStatus.connected_participants")
      )
      sequencer <- ProtoConverter.parseRequired(
        SequencerHealthStatus.fromProto,
        "SequencerNodeStatus.sequencer",
        sequencerNodeStatusP.sequencer,
      )
      domainId <- DomainId.fromProtoPrimitive(
        sequencerNodeStatusP.domainId,
        s"SequencerNodeStatus.domain_id",
      )
      admin <- sequencerNodeStatusP.admin.traverse(SequencerAdminStatus.fromProto)
    } yield SequencerNodeStatus(
      status.uid,
      domainId,
      status.uptime,
      status.ports,
      participants,
      sequencer,
      status.topologyQueue,
      admin,
      status.components,
      version = None,
      protocolVersion = None,
    )

  def fromProtoV1(proto: SequencerStatusResponse): ParsingResult[NodeStatus[SequencerNodeStatus]] =
    proto.kind match {
      case domainV0.SequencerStatusResponse.Kind.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("SequencerStatusResponse.Kind"))
      case domainV0.SequencerStatusResponse.Kind.Status(sequencerStatusP) =>
        for {
          statusP <- ProtoConverter.required(
            "SequencerStatusResponse.common_status",
            sequencerStatusP.commonStatus,
          )
          status <- SimpleStatus.fromProtoV1(statusP)

          participants <- sequencerStatusP.connectedParticipantsUid.traverse(uid =>
            UniqueIdentifier
              .fromProtoPrimitive(uid, "connected_participants_uid")
              .map(ParticipantId(_))
          )
          sequencer <- ProtoConverter.parseRequired(
            SequencerHealthStatus.fromProto,
            "SequencerStatusResponse.SequencerHealthStatus.sequencer",
            sequencerStatusP.sequencer,
          )
          domainId <- DomainId.fromProtoPrimitive(
            sequencerStatusP.domainId,
            "SequencerStatusResponse.domain_id",
          )
          admin <- sequencerStatusP.admin.traverse(SequencerAdminStatus.fromProto)
          protocolVersion <- ProtocolVersion.fromProtoPrimitive(
            sequencerStatusP.protocolVersion,
            allowDeleted = true,
          )
        } yield NodeStatus.Success(
          SequencerNodeStatus(
            uid = status.uid,
            uptime = status.uptime,
            ports = status.ports,
            connectedParticipants = participants,
            topologyQueue = status.topologyQueue,
            components = status.components,
            version = Some(ReleaseVersion.current),
            protocolVersion = protocolVersion.some,
            domainId = domainId,
            sequencer = sequencer,
            admin = admin,
          )
        )

      case domainV0.SequencerStatusResponse.Kind.Unavailable(notInitialized) =>
        Right(NodeStatus.NotInitialized(notInitialized.active))
      case domainV0.SequencerStatusResponse.Kind.Failure(failure) =>
        Right(NodeStatus.Failure(failure.errorMessage))
    }
}
