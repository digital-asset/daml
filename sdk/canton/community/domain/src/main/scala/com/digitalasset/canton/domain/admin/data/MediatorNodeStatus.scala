// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.data

import cats.syntax.option.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.admin.v0.MediatorStatusResponse
import com.digitalasset.canton.domain.admin.v0 as domainV0
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.health.admin.data.NodeStatus.{
  multiline,
  portsString,
  protocolVersionString,
  versionString,
}
import com.digitalasset.canton.health.admin.data.{NodeStatus, SimpleStatus, TopologyQueueStatus}
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
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
    version: Option[ReleaseVersion],
    protocolVersion: Option[ProtocolVersion],
) extends NodeStatus.Status {

  override def pretty: Pretty[MediatorNodeStatus] =
    prettyOfString(_ =>
      (Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        s"Domain id: ${domainId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
      ) ++ versionString(version) ++ protocolVersionString(protocolVersion))
        .mkString(System.lineSeparator())
    )

  def toProtoV0: v0.NodeStatus.Status =
    this
      .into[SimpleStatus]
      .transform
      .toProtoV0
      .copy(
        extra = v0
          .MediatorNodeStatus(domainId.toProtoPrimitive)
          .toByteString
      )

  override def toProtoV1: v1.Status = this
    .into[SimpleStatus]
    .enableMethodAccessors
    .transform
    .toProtoV1

  def toMediatorStatusProto: domainV0.MediatorStatusResponse.MediatorStatusResponseStatus =
    domainV0.MediatorStatusResponse.MediatorStatusResponseStatus(
      commonStatus = toProtoV1.some,
      domainId = domainId.toProtoPrimitive,
      // TODO(#20463) resolve Option[ProtocolVersion]
      protocolVersion = protocolVersion.getOrElse(ProtocolVersion.latestStable).toProtoPrimitive,
    )
}

object MediatorNodeStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[MediatorNodeStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(proto)
      mediatorNodeStatus <- ProtoConverter.parse[MediatorNodeStatus, v0.MediatorNodeStatus](
        v0.MediatorNodeStatus.parseFrom,
        mediatorNodeStatusP =>
          DomainId
            .fromProtoPrimitive(mediatorNodeStatusP.domainId, "MediatorNodeStatus.domain_id")
            .map { domainId =>
              MediatorNodeStatus(
                status.uid,
                domainId,
                status.uptime,
                status.ports,
                status.active,
                status.topologyQueue,
                status.components,
                version = None,
                protocolVersion = None,
              )
            },
        proto.extra,
      )
    } yield mediatorNodeStatus

  def fromProtoV1(proto: MediatorStatusResponse): ParsingResult[NodeStatus[MediatorNodeStatus]] =
    proto.kind match {
      case domainV0.MediatorStatusResponse.Kind.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("MediatorStatusResponse.Kind"))

      case domainV0.MediatorStatusResponse.Kind.Status(mediatorStatusP) =>
        for {
          statusP <- ProtoConverter.required(
            "MediatorStatusResponse.common_status",
            mediatorStatusP.commonStatus,
          )
          status <- SimpleStatus.fromProtoV1(statusP)
          domainId <- DomainId.fromProtoPrimitive(
            mediatorStatusP.domainId,
            "MediatorStatusResponse.domain_id",
          )
          protocolVersion <- ProtocolVersion.fromProtoPrimitive(
            mediatorStatusP.protocolVersion,
            allowDeleted = true,
          )
        } yield NodeStatus.Success(
          MediatorNodeStatus(
            uid = status.uid,
            domainId = domainId,
            uptime = status.uptime,
            ports = status.ports,
            active = status.active,
            topologyQueue = status.topologyQueue,
            components = status.components,
            version = Some(ReleaseVersion.current),
            protocolVersion = protocolVersion.some,
          )
        )

      case domainV0.MediatorStatusResponse.Kind.Unavailable(notInitialized) =>
        Right(NodeStatus.NotInitialized(notInitialized.active))

      case domainV0.MediatorStatusResponse.Kind.Failure(failure) =>
        Right(NodeStatus.Failure(failure.errorMessage))
    }
}
