// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.data

import cats.syntax.option.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.admin.v0.DomainManagerStatusResponse
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
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import io.scalaland.chimney.dsl.*

import java.time.Duration

final case class DomainManagerNodeStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: Option[ReleaseVersion],
    protocolVersion: Option[ProtocolVersion],
) extends NodeStatus.Status {

  override def pretty: Pretty[DomainManagerNodeStatus] =
    prettyOfString(_ =>
      (Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
      ) ++ versionString(version) ++ protocolVersionString(protocolVersion))
        .mkString(System.lineSeparator())
    )

  override def toProtoV0: v0.NodeStatus.Status = this
    .into[SimpleStatus]
    .enableMethodAccessors
    .transform
    .toProtoV0

  override def toProtoV1: v1.Status = this
    .into[SimpleStatus]
    .enableMethodAccessors
    .transform
    .toProtoV1

  def toDomainManagerStatusProto
      : domainV0.DomainManagerStatusResponse.DomainManagerStatusResponseStatus =
    domainV0.DomainManagerStatusResponse.DomainManagerStatusResponseStatus(
      commonStatus = toProtoV1.some,
      // TODO(#20463) resolve Option[ProtocolVersion]
      protocolVersion = protocolVersion.getOrElse(ProtocolVersion.latestStable).toProtoPrimitive,
    )

}

object DomainManagerNodeStatus {

  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[DomainManagerNodeStatus] =
    SimpleStatus.fromProtoV0(proto).map { status =>
      DomainManagerNodeStatus(
        status.uid,
        status.uptime,
        status.ports,
        status.active,
        status.topologyQueue,
        status.components,
        version = None,
        protocolVersion = None,
      )
    }

  def fromProtoV1(
      proto: DomainManagerStatusResponse
  ): ParsingResult[NodeStatus[DomainManagerNodeStatus]] =
    proto.kind match {
      case domainV0.DomainManagerStatusResponse.Kind.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("DomainManagerStatusResponse.Kind"))

      case domainV0.DomainManagerStatusResponse.Kind.Status(domainManagerStatusP) =>
        for {
          statusP <- ProtoConverter.required(
            "DomainManagerStatusResponse.common_status",
            domainManagerStatusP.commonStatus,
          )
          status <- SimpleStatus.fromProtoV1(statusP)
          protocolVersion <- ProtocolVersion.fromProtoPrimitive(
            domainManagerStatusP.protocolVersion,
            allowDeleted = true,
          )
        } yield NodeStatus.Success(
          DomainManagerNodeStatus(
            uid = status.uid,
            uptime = status.uptime,
            ports = status.ports,
            active = status.active,
            topologyQueue = status.topologyQueue,
            components = status.components,
            version = Some(ReleaseVersion.current),
            protocolVersion = protocolVersion.some,
          )
        )

      case domainV0.DomainManagerStatusResponse.Kind.Unavailable(notInitialized) =>
        Right(NodeStatus.NotInitialized(notInitialized.active))

      case domainV0.DomainManagerStatusResponse.Kind.Failure(failure) =>
        Right(NodeStatus.Failure(failure.errorMessage))
    }

}
