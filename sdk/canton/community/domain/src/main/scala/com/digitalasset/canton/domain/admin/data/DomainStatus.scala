// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.data

import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.admin.v0.DomainStatusResponse
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
  SequencerHealthStatus,
  SimpleStatus,
  TopologyQueueStatus,
}
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.health.admin.v0.DomainStatusInfo
import com.digitalasset.canton.health.admin.v1.Status
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import io.scalaland.chimney.dsl.TransformerOps

import java.time.Duration

final case class DomainStatus(
    uid: UniqueIdentifier, // TODO(#20463) Should be DomainId
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: Option[ReleaseVersion],
    protocolVersion: Option[ProtocolVersion],
) extends NodeStatus.Status {
  val id: DomainId = DomainId(uid)

  // A domain node is not replicated and always active
  override def active: Boolean = true

  override def pretty: Pretty[DomainStatus] =
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

  override def toProtoV1: Status = this
    .into[SimpleStatus]
    .enableMethodAccessors
    .transform
    .toProtoV1

  def toDomainStatusProto: domainV0.DomainStatusResponse.DomainStatusResponseStatus =
    domainV0.DomainStatusResponse.DomainStatusResponseStatus(
      commonStatus = toProtoV1.some,
      connectedParticipantsUid = connectedParticipants.map(_.uid.toProtoPrimitive),
      sequencer = sequencer.toProtoV0.some,
      // TODO(#20463) resolve Option[ProtocolVersion]
      protocolVersion = protocolVersion.getOrElse(ProtocolVersion.latestStable).toProtoPrimitive,
    )
}

object DomainStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[DomainStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(proto)
      domainStatus <- ProtoConverter
        .parse[DomainStatus, v0.DomainStatusInfo](
          v0.DomainStatusInfo.parseFrom,
          fromDomainStatusProtoV0(_, status),
          proto.extra,
        )
    } yield domainStatus

  private def fromDomainStatusProtoV0(
      domainStatusInfoP: DomainStatusInfo,
      status: SimpleStatus,
  ): ParsingResult[DomainStatus] =
    for {
      participants <- domainStatusInfoP.connectedParticipants.traverse(pId =>
        ParticipantId.fromProtoPrimitive(pId, "NodeStatus.Status.connected_participants")
      )
      sequencer <- ProtoConverter.parseRequired(
        SequencerHealthStatus.fromProto,
        "NodeStatus.Status.sequencer",
        domainStatusInfoP.sequencer,
      )

    } yield DomainStatus(
      status.uid,
      status.uptime,
      status.ports,
      participants,
      sequencer,
      status.topologyQueue,
      status.components,
      version = None,
      protocolVersion = None,
    )

  def fromProtoV1(proto: DomainStatusResponse): ParsingResult[NodeStatus[DomainStatus]] =
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
          DomainStatus(
            uid = status.uid,
            uptime = status.uptime,
            ports = status.ports,
            connectedParticipants = participants,
            sequencer = sequencer,
            topologyQueue = status.topologyQueue,
            components = status.components,
            version = Some(ReleaseVersion.current),
            protocolVersion = protocolVersion.some,
          )
        )

      case domainV0.DomainStatusResponse.Kind.Unavailable(notInitialized) =>
        Right(NodeStatus.NotInitialized(notInitialized.active))

      case domainV0.DomainStatusResponse.Kind.Failure(failure) =>
        Right(NodeStatus.Failure(failure.errorMessage))
    }
}
