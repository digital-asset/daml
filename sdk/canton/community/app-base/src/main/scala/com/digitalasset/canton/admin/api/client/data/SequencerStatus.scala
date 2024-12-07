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
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.util.ShowUtil
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration

final case class SequencerStatus(
    uid: UniqueIdentifier,
    domainId: DomainId,
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    connectedMediators: Seq[MediatorId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
    admin: Option[SequencerAdminStatus],
    components: Seq[ComponentStatus],
    version: Option[ReleaseVersion],
    protocolVersion: Option[ProtocolVersion],
) extends NodeStatus.Status {

  override def active: Boolean = sequencer.isActive

  override def pretty: Pretty[SequencerStatus] =
    prettyOfString(_ =>
      (
        Seq(
          s"Sequencer id: ${uid.toProtoPrimitive}",
          s"Domain id: ${domainId.toProtoPrimitive}",
          show"Uptime: $uptime",
          s"Ports: ${portsString(ports)}",
          s"Connected participants: ${multiline(connectedParticipants.map(_.toString))}",
          s"Connected mediators: ${multiline(connectedMediators.map(_.toString))}",
          show"Sequencer: $sequencer",
          s"details-extra: ${sequencer.details}",
          s"Components: ${multiline(components.map(_.toString))}",
        ) ++ versionString(version) ++ protocolVersionString(protocolVersion)
          ++ admin.toList.map(adminStatus =>
            s"Accepts admin changes: ${adminStatus.acceptsAdminChanges}"
          ),
      ).mkString(System.lineSeparator())
    )
}

object SequencerStatus {
  def fromProtoV0(
      sequencerP: v0.NodeStatus.Status
  ): ParsingResult[SequencerStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(sequencerP)
      sequencerNodeStatus <- ProtoConverter.parse[SequencerStatus, v0.SequencerNodeStatus](
        v0.SequencerNodeStatus.parseFrom,
        fromSequencerNodeStatusProtoV0(_, status),
        sequencerP.extra,
      )
    } yield sequencerNodeStatus

  private def fromSequencerNodeStatusProtoV0(
      sequencerNodeStatusP: v0.SequencerNodeStatus,
      status: SimpleStatus,
  ): ParsingResult[SequencerStatus] =
    for {
      participants <- sequencerNodeStatusP.connectedParticipants.traverse(pId =>
        ParticipantId.fromProtoPrimitive(pId, s"SequencerNodeStatus.connected_participants")
      )
      mediators <- sequencerNodeStatusP.connectedMediators.traverse(pId =>
        MediatorId.fromProtoPrimitive(pId, s"SequencerNodeStatus.connected_mediators")
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
    } yield SequencerStatus(
      status.uid,
      domainId,
      status.uptime,
      status.ports,
      participants,
      mediators,
      sequencer,
      status.topologyQueue,
      admin,
      status.components,
      version = None,
      protocolVersion = None,
    )

  def fromProtoV1(
      proto: domainV0.SequencerStatusResponse
  ): ParsingResult[NodeStatus[SequencerStatus]] =
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
          mediators <- sequencerStatusP.connectedMediatorsUid.traverse(uid =>
            UniqueIdentifier
              .fromProtoPrimitive(uid, "connected_mediators_uid")
              .map(MediatorId(_))
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
          SequencerStatus(
            uid = status.uid,
            uptime = status.uptime,
            ports = status.ports,
            connectedParticipants = participants,
            connectedMediators = mediators,
            topologyQueue = status.topologyQueue,
            components = status.components,
            version = status.version,
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

/** Health status of the sequencer component itself.
  * @param isActive implementation specific flag indicating whether the sequencer is active
  */
final case class SequencerHealthStatus(isActive: Boolean, details: Option[String] = None)
    extends PrettyPrinting {
  def toProtoV0: v0.SequencerHealthStatus = v0.SequencerHealthStatus(isActive, details)

  override def pretty: Pretty[SequencerHealthStatus] =
    SequencerHealthStatus.prettySequencerHealthStatus
}

object SequencerHealthStatus extends PrettyUtil with ShowUtil {
  val shutdownStatus: SequencerHealthStatus =
    SequencerHealthStatus(isActive = false, details = Some("Sequencer is closed"))

  def fromProto(
      statusP: v0.SequencerHealthStatus
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
  def toProtoV0: v0.SequencerAdminStatus = v0.SequencerAdminStatus(acceptsAdminChanges)

  override def pretty: Pretty[SequencerAdminStatus] =
    SequencerAdminStatus.prettySequencerHealthStatus
}

object SequencerAdminStatus extends PrettyUtil with ShowUtil {
  def fromProto(
      statusP: v0.SequencerAdminStatus
  ): ParsingResult[SequencerAdminStatus] =
    Right(SequencerAdminStatus(statusP.acceptsAdminChanges))

  implicit val implicitPrettyString: Pretty[String] = PrettyInstances.prettyString
  implicit val prettySequencerHealthStatus: Pretty[SequencerAdminStatus] =
    prettyOfClass[SequencerAdminStatus](
      param("admin", _.acceptsAdminChanges)
    )
}
