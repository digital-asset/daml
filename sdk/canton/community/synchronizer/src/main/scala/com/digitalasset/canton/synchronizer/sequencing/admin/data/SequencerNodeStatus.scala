// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.admin.data

import cats.syntax.option.*
import com.digitalasset.canton.admin.domain.v30 as domainV30
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.ComponentHealthState.UnhealthyState
import com.digitalasset.canton.health.admin.data.*
import com.digitalasset.canton.health.admin.data.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.{
  ComponentHealthState,
  ComponentStatus,
  ToComponentHealthState,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{
  MediatorId,
  Member,
  ParticipantId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.util.ShowUtil
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration

final case class SequencerNodeStatus(
    uid: UniqueIdentifier,
    synchronizerId: SynchronizerId,
    uptime: Duration,
    ports: Map[String, Port],
    connectedMembers: Seq[Member],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
    admin: SequencerAdminStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    protocolVersion: ProtocolVersion,
) extends NodeStatus.Status {
  override def active: Boolean = sequencer.isActive

  private val connectedParticipants = connectedMembers.collect { case participant: ParticipantId =>
    participant
  }

  private val connectedMediators = connectedMembers.collect { case mediator: MediatorId =>
    mediator
  }

  override protected def pretty: Pretty[SequencerNodeStatus] =
    prettyOfString(_ =>
      Seq(
        s"Sequencer id: ${uid.toProtoPrimitive}",
        s"Synchronizer id: ${synchronizerId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected participants: ${multiline(connectedParticipants.map(_.toString))}",
        s"Connected mediators: ${multiline(connectedMediators.map(_.toString))}",
        show"Sequencer: $sequencer",
        s"Accepts admin changes: ${admin.acceptsAdminChanges}",
        s"details-extra: ${sequencer.details}",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: ${version.fullVersion}",
        s"Protocol version: $protocolVersion",
      ).mkString(System.lineSeparator())
    )

  def toSequencerStatusProto: domainV30.SequencerStatusResponse.SequencerStatusResponseStatus = {

    val connectedParticipantsP = connectedParticipants.map { p =>
      domainV30.SequencerStatusResponse.ConnectedParticipant(p.uid.toProtoPrimitive)
    }

    val connectedMediatorsP = connectedMediators.map { m =>
      domainV30.SequencerStatusResponse.ConnectedMediator(m.uid.toProtoPrimitive)
    }

    domainV30.SequencerStatusResponse.SequencerStatusResponseStatus(
      commonStatus = toProtoV30.some,
      connectedParticipants = connectedParticipantsP,
      connectedMediators = connectedMediatorsP,
      sequencer = sequencer.toProtoV30.some,
      synchronizerId = synchronizerId.toProtoPrimitive,
      admin = admin.toProtoV30.some,
      protocolVersion = protocolVersion.toProtoPrimitive,
    )
  }
}

/** Health status of the sequencer component itself.
  * @param isActive implementation specific flag indicating whether the sequencer is active
  */
final case class SequencerHealthStatus(isActive: Boolean, details: Option[String] = None)
    extends ToComponentHealthState
    with PrettyPrinting {
  def toProtoV30: domainV30.SequencerHealthStatus =
    domainV30.SequencerHealthStatus(isActive, details)

  override def toComponentHealthState: ComponentHealthState = if (isActive)
    ComponentHealthState.Ok(details)
  else
    ComponentHealthState.Failed(UnhealthyState(details))

  override protected def pretty: Pretty[SequencerHealthStatus] =
    SequencerHealthStatus.prettySequencerHealthStatus
}

object SequencerHealthStatus extends PrettyUtil with ShowUtil {
  val shutdownStatus: SequencerHealthStatus =
    SequencerHealthStatus(isActive = false, details = Some("Sequencer is closed"))

  def fromProto(
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
  * @param acceptsAdminChanges implementation specific flag indicating whether the sequencer node accepts administration commands
  */
final case class SequencerAdminStatus(acceptsAdminChanges: Boolean)
    extends ToComponentHealthState
    with PrettyPrinting {
  def toProtoV30: domainV30.SequencerAdminStatus =
    domainV30.SequencerAdminStatus(acceptsAdminChanges)

  override def toComponentHealthState: ComponentHealthState =
    ComponentHealthState.Ok(Option.when(acceptsAdminChanges)("sequencer accepts admin commands"))

  override protected def pretty: Pretty[SequencerAdminStatus] =
    SequencerAdminStatus.prettySequencerHealthStatus
}

object SequencerAdminStatus extends PrettyUtil with ShowUtil {
  def fromProto(
      statusP: domainV30.SequencerAdminStatus
  ): ParsingResult[SequencerAdminStatus] =
    Right(SequencerAdminStatus(statusP.acceptsAdminChanges))

  implicit val implicitPrettyString: Pretty[String] = PrettyInstances.prettyString
  implicit val prettySequencerHealthStatus: Pretty[SequencerAdminStatus] =
    prettyOfClass[SequencerAdminStatus](
      param("admin", _.acceptsAdminChanges)
    )
}
