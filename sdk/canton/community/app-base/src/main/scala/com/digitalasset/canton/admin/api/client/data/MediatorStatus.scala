// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.NodeStatus.*
import com.digitalasset.canton.admin.mediator.v30 as mediatorV30
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration

final case class MediatorStatus(
    uid: UniqueIdentifier,
    synchronizerId: SynchronizerId,
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
        s"Synchronizer id: ${synchronizerId.toProtoPrimitive}",
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
      proto: mediatorV30.MediatorStatusResponse
  ): ParsingResult[NodeStatus[MediatorStatus]] =
    proto.kind match {
      case mediatorV30.MediatorStatusResponse.Kind.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("MediatorStatusResponse.Kind"))

      case mediatorV30.MediatorStatusResponse.Kind.Status(mediatorStatusP) =>
        for {
          statusP <- ProtoConverter.required(
            "MediatorStatusResponse.common_status",
            mediatorStatusP.commonStatus,
          )
          status <- SimpleStatus.fromProtoV30(statusP)
          synchronizerId <- SynchronizerId.fromProtoPrimitive(
            mediatorStatusP.synchronizerId,
            "MediatorStatusResponse.synchronizer_id",
          )
          protocolVersion <- ProtocolVersion.fromProtoPrimitive(
            mediatorStatusP.protocolVersion,
            allowDeleted = true,
          )
        } yield NodeStatus.Success(
          MediatorStatus(
            uid = status.uid,
            synchronizerId = synchronizerId,
            uptime = status.uptime,
            ports = status.ports,
            active = status.active,
            topologyQueue = status.topologyQueue,
            components = status.components,
            version = status.version,
            protocolVersion = protocolVersion,
          )
        )

      case mediatorV30.MediatorStatusResponse.Kind.NotInitialized(notInitialized) =>
        WaitingForExternalInput
          .fromProtoV30(notInitialized.waitingForExternalInput)
          .map(NodeStatus.NotInitialized(notInitialized.active, _))
    }
}
