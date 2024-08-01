// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.health.admin.data.NodeStatus.{
  multiline,
  portsString,
  protocolVersionsString,
  versionString,
}
import com.digitalasset.canton.health.admin.data.{NodeStatus, SimpleStatus, TopologyQueueStatus}
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.admin.v0.ConnectedDomain
import com.digitalasset.canton.participant.admin.v0 as participantV0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
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
    version: Option[ReleaseVersion],
    supportedProtocolVersions: Option[Seq[ProtocolVersion]],
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
      ) ++ versionString(version) ++ protocolVersionsString(supportedProtocolVersions))
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

    val protocolVersions = supportedProtocolVersions.getOrElse(Seq.empty).map(_.toProtoPrimitive)

    participantV0.ParticipantStatusResponse.ParticipantStatusResponseStatus(
      commonStatus = Some(toProtoV1),
      connectedDomains = domains,
      active = active,
      supportedProtocolVersions = protocolVersions,
    )
  }
}

object ParticipantStatus {

  private def connectedDomainFromProtoV0(
      proto: v0.ParticipantStatusInfo.ConnectedDomain
  ): ParsingResult[(DomainId, Boolean)] =
    DomainId.fromProtoPrimitive(proto.domain, "ParticipantStatusInfo.connected_domains").map {
      domainId =>
        (domainId, proto.healthy)
    }

  def fromProtoV0(
      proto: v0.NodeStatus.Status
  ): ParsingResult[ParticipantStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(proto)
      participantStatus <- ProtoConverter
        .parse[ParticipantStatus, v0.ParticipantStatusInfo](
          v0.ParticipantStatusInfo.parseFrom,
          participantStatusInfoP =>
            for {
              connectedDomains <- participantStatusInfoP.connectedDomains.traverse(
                connectedDomainFromProtoV0
              )
            } yield ParticipantStatus(
              status.uid,
              status.uptime,
              status.ports,
              connectedDomains.toMap,
              active = participantStatusInfoP.active,
              status.topologyQueue,
              status.components,
              version = None,
              supportedProtocolVersions = None,
            ),
          proto.extra,
        )
    } yield participantStatus

  private def connectedDomainFromProtoV1(
      proto: participantV0.ConnectedDomain
  ): ParsingResult[(DomainId, Boolean)] =
    DomainId.fromProtoPrimitive(proto.domainId, "ParticipantStatusResponse.connected_domains").map {
      domainId =>
        (domainId, proto.healthy)
    }

  def fromProtoV1(
      proto: participantV0.ParticipantStatusResponse
  ): ParsingResult[NodeStatus[ParticipantStatus]] =
    proto.kind match {
      case participantV0.ParticipantStatusResponse.Kind.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("ParticipantStatusResponse.Kind"))

      case participantV0.ParticipantStatusResponse.Kind.Status(participantStatusP) =>
        for {
          statusP <- ProtoConverter.required(
            "ParticipantStatusResponse.common_status",
            participantStatusP.commonStatus,
          )
          status <- SimpleStatus.fromProtoV1(statusP)
          connectedDomains <- participantStatusP.connectedDomains.traverse(
            connectedDomainFromProtoV1
          )
          supportedProtocolVersions <- participantStatusP.supportedProtocolVersions.traverse(
            ProtocolVersion.fromProtoPrimitive(_, allowDeleted = true)
          )
        } yield NodeStatus.Success(
          ParticipantStatus(
            status.uid,
            status.uptime,
            status.ports,
            connectedDomains.toMap: Map[DomainId, Boolean],
            active = status.active,
            status.topologyQueue,
            status.components,
            Some(ReleaseVersion.current),
            Some(supportedProtocolVersions),
          )
        )

      case participantV0.ParticipantStatusResponse.Kind.Unavailable(notInitialized) =>
        Right(NodeStatus.NotInitialized(notInitialized.active))
      case participantV0.ParticipantStatusResponse.Kind.Failure(failure) =>
        Right(NodeStatus.Failure(failure.errorMessage))
    }
}
