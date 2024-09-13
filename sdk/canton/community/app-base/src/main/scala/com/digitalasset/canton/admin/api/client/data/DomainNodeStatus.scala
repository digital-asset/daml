// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.NodeStatus.{
  protocolVersionString,
  versionString,
}
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.admin.v0 as domainV0
import com.digitalasset.canton.health.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration

final case class DomainNodeStatus(
    id: DomainId,
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: Option[ReleaseVersion],
    protocolVersion: Option[ProtocolVersion],
) extends NodeStatus.Status {
  val uid: UniqueIdentifier = id.uid

  // A domain node is not replicated and always active
  override def active: Boolean = true

  override def pretty: Pretty[DomainNodeStatus] =
    prettyOfString(_ =>
      (Seq(
        s"Domain id: ${uid.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected Participants: ${multiline(connectedParticipants.map(_.toString))}",
        show"Sequencer: $sequencer",
        s"Components: ${multiline(components.map(_.toString))}",
      ) ++ versionString(version) ++ protocolVersionString(protocolVersion))
        .mkString(System.lineSeparator())
    )
}

object DomainNodeStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[DomainNodeStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(proto)
      domainStatus <- ProtoConverter
        .parse[DomainNodeStatus, v0.DomainStatusInfo](
          v0.DomainStatusInfo.parseFrom,
          fromDomainStatusProtoV0(_, status),
          proto.extra,
        )
    } yield domainStatus

  private def fromDomainStatusProtoV0(
      domainStatusInfoP: v0.DomainStatusInfo,
      status: SimpleStatus,
  ): ParsingResult[DomainNodeStatus] =
    for {
      participants <- domainStatusInfoP.connectedParticipants.traverse(pId =>
        ParticipantId.fromProtoPrimitive(pId, "NodeStatus.Status.connected_participants")
      )
      sequencer <- ProtoConverter.parseRequired(
        SequencerHealthStatus.fromProto,
        "NodeStatus.Status.sequencer",
        domainStatusInfoP.sequencer,
      )

    } yield DomainNodeStatus(
      DomainId(status.uid),
      status.uptime,
      status.ports,
      participants,
      sequencer,
      status.topologyQueue,
      status.components,
      version = None,
      protocolVersion = None,
    )

  def fromProtoV1(
      proto: domainV0.DomainStatusResponse
  ): ParsingResult[NodeStatus[DomainNodeStatus]] =
    proto.kind match {
      case domainV0.DomainStatusResponse.Kind.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("DomainStatusResponse.Kind"))

      case domainV0.DomainStatusResponse.Kind.Status(domainStatusP) =>
        for {
          statusP <- ProtoConverter.required(
            "DomainStatusResponse.common_status",
            domainStatusP.commonStatus,
          )
          status <- SimpleStatus.fromProtoV1(statusP)
          participants <- domainStatusP.connectedParticipantsUid.traverse(uid =>
            UniqueIdentifier
              .fromProtoPrimitive(uid, "DomainStatusResponse.connected_participants_uid")
              .map(ParticipantId(_))
          )
          sequencer <- ProtoConverter.parseRequired(
            SequencerHealthStatus.fromProto,
            "DomainStatusResponse.sequencer",
            domainStatusP.sequencer,
          )
          protocolVersion <- ProtocolVersion.fromProtoPrimitive(
            domainStatusP.protocolVersion,
            allowDeleted = true,
          )
        } yield NodeStatus.Success(
          DomainNodeStatus(
            id = DomainId(status.uid),
            uptime = status.uptime,
            ports = status.ports,
            connectedParticipants = participants,
            sequencer = sequencer,
            topologyQueue = status.topologyQueue,
            components = status.components,
            version = status.version,
            protocolVersion = protocolVersion.some,
          )
        )

      case domainV0.DomainStatusResponse.Kind.Unavailable(notInitialized) =>
        Right(NodeStatus.NotInitialized(notInitialized.active))

      case domainV0.DomainStatusResponse.Kind.Failure(failure) =>
        Right(NodeStatus.Failure(failure.errorMessage))
    }
}
