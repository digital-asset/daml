// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.NodeStatus.*
import com.digitalasset.canton.admin.domain.v30 as domainV30
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration

final case class MediatorStatus(
    uid: UniqueIdentifier,
    domainId: DomainId,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    protocolVersion: ProtocolVersion,
) extends NodeStatus.Status {

  override protected def pretty: Pretty[MediatorStatus] =
    prettyOfString(_ =>
      Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        s"Domain id: ${domainId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: $version",
        s"Protocol version: $protocolVersion",
      ).mkString(System.lineSeparator())
    )
}

object MediatorStatus {
  def fromProtoV30(
      proto: domainV30.MediatorStatusResponse
  ): ParsingResult[NodeStatus[MediatorStatus]] =
    proto.kind match {
      case domainV30.MediatorStatusResponse.Kind.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("MediatorStatusResponse.Kind"))

      case domainV30.MediatorStatusResponse.Kind.Status(mediatorStatusP) =>
        for {
          statusP <- ProtoConverter.required(
            "MediatorStatusResponse.common_status",
            mediatorStatusP.commonStatus,
          )
          status <- SimpleStatus.fromProtoV30(statusP)
          domainId <- DomainId.fromProtoPrimitive(
            mediatorStatusP.domainId,
            "MediatorStatusResponse.domain_id",
          )
          protocolVersion <- ProtocolVersion.fromProtoPrimitive(
            mediatorStatusP.protocolVersion,
            allowDeleted = true,
          )
        } yield NodeStatus.Success(
          MediatorStatus(
            uid = status.uid,
            domainId = domainId,
            uptime = status.uptime,
            ports = status.ports,
            active = status.active,
            topologyQueue = status.topologyQueue,
            components = status.components,
            version = status.version,
            protocolVersion = protocolVersion,
          )
        )

      case domainV30.MediatorStatusResponse.Kind.NotInitialized(notInitialized) =>
        WaitingForExternalInput
          .fromProtoV30(notInitialized.waitingForExternalInput)
          .map(NodeStatus.NotInitialized(notInitialized.active, _))
    }
}
