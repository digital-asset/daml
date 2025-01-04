// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.NodeStatus.*
import com.digitalasset.canton.admin.domain.v30 as domainV30
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{
  MediatorId,
  ParticipantId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.util.ShowUtil
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration

final case class SequencerStatus(
    uid: UniqueIdentifier,
    synchronizerId: SynchronizerId,
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    connectedMediators: Seq[MediatorId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
    admin: SequencerAdminStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    protocolVersion: ProtocolVersion,
) extends NodeStatus.Status {

  override def active: Boolean = sequencer.isActive

  override protected def pretty: Pretty[SequencerStatus] =
    prettyOfString(_ =>
      Seq(
        s"Sequencer id: ${uid.toProtoPrimitive}",
        s"Synchronizer id: ${synchronizerId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected participants: ${multiline(connectedParticipants.map(_.toString))}",
        s"Connected mediators: ${multiline(connectedMediators.map(_.toString))}",
        show"Sequencer: $sequencer",
        s"details-extra: ${sequencer.details}",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Accepts admin changes: ${admin.acceptsAdminChanges}",
        s"Version: $version",
        s"Protocol version: $protocolVersion",
      ).mkString(System.lineSeparator())
    )
}

object SequencerStatus {
  private def fromProtoV30(
      proto: domainV30.SequencerStatusResponse.ConnectedMediator
  ): ParsingResult[MediatorId] =
    UniqueIdentifier
      .fromProtoPrimitive(proto.uid, "uid")
      .map(MediatorId(_))

  private def fromProtoV30(
      proto: domainV30.SequencerStatusResponse.ConnectedParticipant
  ): ParsingResult[ParticipantId] =
    UniqueIdentifier
      .fromProtoPrimitive(proto.uid, "uid")
      .map(ParticipantId(_))

  def fromProtoV30(
      proto: domainV30.SequencerStatusResponse
  ): ParsingResult[NodeStatus[SequencerStatus]] =
    proto.kind match {
      case domainV30.SequencerStatusResponse.Kind.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("SequencerStatusResponse.Kind"))
      case domainV30.SequencerStatusResponse.Kind.Status(sequencerStatusP) =>
        for {
          statusP <- ProtoConverter.required(
            "SequencerStatusResponse.common_status",
            sequencerStatusP.commonStatus,
          )
          status <- SimpleStatus.fromProtoV30(statusP)

          participants <- sequencerStatusP.connectedParticipants.traverse(fromProtoV30)
          mediators <- sequencerStatusP.connectedMediators.traverse(fromProtoV30)

          sequencer <- ProtoConverter.parseRequired(
            SequencerHealthStatus.fromProtoV30,
            "SequencerStatusResponse.SequencerHealthStatus.sequencer",
            sequencerStatusP.sequencer,
          )
          synchronizerId <- SynchronizerId.fromProtoPrimitive(
            sequencerStatusP.synchronizerId,
            "SequencerStatusResponse.synchronizer_id",
          )
          adminP <- ProtoConverter.required("admin", sequencerStatusP.admin)
          admin <- SequencerAdminStatus.fromProtoV30(adminP)
          protocolVersion <- ProtocolVersion.fromProtoPrimitive(
            sequencerStatusP.protocolVersion,
            allowDeleted = true,
          )
        } yield NodeStatus.Success(
          SequencerStatus(
            uid = status.uid,
            uptime = status.uptime,
            ports = status.ports,
            connectedParticipants = participants,
            connectedMediators = mediators,
            topologyQueue = status.topologyQueue,
            components = status.components,
            version = status.version,
            protocolVersion = protocolVersion,
            synchronizerId = synchronizerId,
            sequencer = sequencer,
            admin = admin,
          )
        )

      case domainV30.SequencerStatusResponse.Kind.NotInitialized(notInitialized) =>
        WaitingForExternalInput
          .fromProtoV30(notInitialized.waitingForExternalInput)
          .map(NodeStatus.NotInitialized(notInitialized.active, _))
    }
}

/** Health status of the sequencer component itself.
  * @param isActive implementation specific flag indicating whether the sequencer is active
  */
final case class SequencerHealthStatus(isActive: Boolean, details: Option[String] = None)
    extends PrettyPrinting {
  override protected def pretty: Pretty[SequencerHealthStatus] =
    SequencerHealthStatus.prettySequencerHealthStatus
}

object SequencerHealthStatus extends PrettyUtil with ShowUtil {
  def fromProtoV30(
      statusP: domainV30.SequencerHealthStatus
  ): ParsingResult[SequencerHealthStatus] =
    Right(SequencerHealthStatus(statusP.active, statusP.details))

  implicit val implicitPrettyString: Pretty[String] = PrettyInstances.prettyString
  implicit val prettySequencerHealthStatus: Pretty[SequencerHealthStatus] =
    prettyOfClass[SequencerHealthStatus](
      param("active", _.isActive),
      paramIfDefined("details", _.details.map(_.unquoted)),
    )
}

/** Admin status of the sequencer node.
  * @param acceptsAdminChanges indicates whether the sequencer node accepts administration commands
  */
final case class SequencerAdminStatus(acceptsAdminChanges: Boolean) extends PrettyPrinting {
  override protected def pretty: Pretty[SequencerAdminStatus] =
    SequencerAdminStatus.prettySequencerHealthStatus
}

object SequencerAdminStatus extends PrettyUtil with ShowUtil {
  def fromProtoV30(
      statusP: domainV30.SequencerAdminStatus
  ): ParsingResult[SequencerAdminStatus] =
    Right(SequencerAdminStatus(statusP.acceptsAdminChanges))

  implicit val implicitPrettyString: Pretty[String] = PrettyInstances.prettyString
  implicit val prettySequencerHealthStatus: Pretty[SequencerAdminStatus] =
    prettyOfClass[SequencerAdminStatus](
      param("admin", _.acceptsAdminChanges)
    )
}
