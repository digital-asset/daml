// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.option.*
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
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration

final case class DomainManagerStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: Option[ReleaseVersion],
    protocolVersion: Option[ProtocolVersion],
) extends NodeStatus.Status {

  override def pretty: Pretty[DomainManagerStatus] =
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
}

object DomainManagerStatus {

  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[DomainManagerStatus] =
    SimpleStatus.fromProtoV0(proto).map { status =>
      DomainManagerStatus(
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
      proto: domainV0.DomainManagerStatusResponse
  ): ParsingResult[NodeStatus[DomainManagerStatus]] =
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
          DomainManagerStatus(
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
