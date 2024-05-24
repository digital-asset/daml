// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health.admin.data

import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.ComponentHealthState.UnhealthyState
import com.digitalasset.canton.health.admin.data.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.health.{
  ComponentHealthState,
  ComponentStatus,
  ToComponentHealthState,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{DurationConverter, ParsingResult}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.util.ShowUtil
import com.google.protobuf.ByteString

import java.time.Duration

sealed trait NodeStatus[+S <: NodeStatus.Status]
    extends PrettyPrinting
    with Product
    with Serializable {
  def trySuccess: S
  def successOption: Option[S]

  /** Return the node activeness if it is known or None otherwise.
    */
  def isActive: Option[Boolean]
}

object NodeStatus {

  /** A failure to query the node's status */
  final case class Failure(msg: String) extends NodeStatus[Nothing] {
    override def pretty: Pretty[Failure] = prettyOfString(_.msg)
    override def trySuccess: Nothing =
      sys.error(s"Status did not complete successfully. Error: $msg")
    override def successOption: Option[Nothing] = None

    override def isActive: Option[Boolean] = None
  }

  /** A node is running but not yet initialized. */
  final case class NotInitialized(active: Boolean) extends NodeStatus[Nothing] {
    override def pretty: Pretty[NotInitialized] = prettyOfClass(param("active", _.active))
    override def trySuccess: Nothing = sys.error(s"Node is not yet initialized.")
    override def successOption: Option[Nothing] = None

    override def isActive: Option[Boolean] = Some(active)
  }

  final case class Success[S <: Status](status: S) extends NodeStatus[S] {
    override def trySuccess: S = status
    override def pretty: Pretty[Success.this.type] = prettyOfParam(_.status)
    override def successOption: Option[S] = status.some

    override def isActive: Option[Boolean] = Some(status.active)
  }

  trait Status extends PrettyPrinting with Product with Serializable {
    def uid: UniqueIdentifier
    def uptime: Duration
    def ports: Map[String, Port]
    def active: Boolean
    def toProtoV0: v0.NodeStatus.Status // explicitly making it public
    def components: Seq[ComponentStatus]
  }

  private[data] def portsString(ports: Map[String, Port]): String =
    multiline(ports.map { case (portDescription, port) =>
      s"$portDescription: ${port.unwrap}"
    }.toSeq)
  private[data] def multiline(elements: Seq[String]): String =
    if (elements.isEmpty) "None" else elements.map(el => s"\n\t$el").mkString
}

final case class SimpleStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
) extends NodeStatus.Status {
  override def pretty: Pretty[SimpleStatus] =
    prettyOfString(_ =>
      Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
      ).mkString(System.lineSeparator())
    )

  def toProtoV0: v0.NodeStatus.Status =
    v0.NodeStatus.Status(
      uid.toProtoPrimitive,
      Some(DurationConverter.toProtoPrimitive(uptime)),
      ports.fmap(_.unwrap),
      ByteString.EMPTY,
      active,
      topologyQueues = Some(topologyQueue.toProtoV0),
      components = components.map(_.toProtoV0),
    )
}

object SimpleStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[SimpleStatus] = {
    for {
      uid <- UniqueIdentifier.fromProtoPrimitive(proto.id, "Status.id")
      uptime <- ProtoConverter
        .required("Status.uptime", proto.uptime)
        .flatMap(DurationConverter.fromProtoPrimitive)
      ports <- proto.ports.toList
        .traverse { case (s, i) =>
          Port.create(i).leftMap(InvariantViolation.toProtoDeserializationError).map(p => (s, p))
        }
        .map(_.toMap)
      topology <- ProtoConverter.parseRequired(
        TopologyQueueStatus.fromProto,
        "topologyQueues",
        proto.topologyQueues,
      )
      components <- proto.components.toList.traverse(ComponentStatus.fromProtoV0)
    } yield SimpleStatus(
      uid,
      uptime,
      ports,
      proto.active,
      topology,
      components,
    )
  }
}

/** Health status of the sequencer component itself.
  * @param isActive implementation specific flag indicating whether the sequencer is active
  */
final case class SequencerHealthStatus(isActive: Boolean, details: Option[String] = None)
    extends ToComponentHealthState
    with PrettyPrinting {
  def toProtoV0: v0.SequencerHealthStatus = v0.SequencerHealthStatus(isActive, details)

  override def toComponentHealthState: ComponentHealthState = if (isActive)
    ComponentHealthState.Ok(details)
  else
    ComponentHealthState.Failed(UnhealthyState(details))

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
final case class SequencerAdminStatus(acceptsAdminChanges: Boolean)
    extends ToComponentHealthState
    with PrettyPrinting {
  def toProtoV0: v0.SequencerAdminStatus = v0.SequencerAdminStatus(acceptsAdminChanges)

  override def toComponentHealthState: ComponentHealthState =
    ComponentHealthState.Ok(Option.when(acceptsAdminChanges)("sequencer accepts admin changes"))

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

/** Topology manager queue status
  *
  * Status around topology management queues
  * @param manager number of queued commands in the topology manager
  * @param dispatcher number of queued transactions in the dispatcher
  * @param clients number of observed transactions that are not yet effective
  */
final case class TopologyQueueStatus(manager: Int, dispatcher: Int, clients: Int)
    extends PrettyPrinting {
  def toProtoV0: v0.TopologyQueueStatus =
    v0.TopologyQueueStatus(manager = manager, dispatcher = dispatcher, clients = clients)

  def isIdle: Boolean = Seq(manager, dispatcher, clients).forall(_ == 0)

  override def pretty: Pretty[TopologyQueueStatus.this.type] = prettyOfClass(
    param("manager", _.manager),
    param("dispatcher", _.dispatcher),
    param("clients", _.clients),
  )
}

object TopologyQueueStatus {
  def fromProto(
      statusP: v0.TopologyQueueStatus
  ): ParsingResult[TopologyQueueStatus] = {
    val v0.TopologyQueueStatus(manager, dispatcher, clients) = statusP
    Right(TopologyQueueStatus(manager = manager, dispatcher = dispatcher, clients = clients))
  }
}

final case class DomainStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
) extends NodeStatus.Status {
  val id: DomainId = DomainId(uid)

  // A domain node is not replicated and always active
  override def active: Boolean = true

  override def pretty: Pretty[DomainStatus] =
    prettyOfString(_ =>
      Seq(
        s"Domain id: ${uid.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected Participants: ${multiline(connectedParticipants.map(_.toString))}",
        show"Sequencer: $sequencer",
        s"Components: ${multiline(components.map(_.toString))}",
      ).mkString(System.lineSeparator())
    )

  def toProtoV0: v0.NodeStatus.Status = {
    val participants = connectedParticipants.map(_.toProtoPrimitive)
    SimpleStatus(uid, uptime, ports, active, topologyQueue, components).toProtoV0
      .copy(
        extra = v0.DomainStatusInfo(participants, Some(sequencer.toProtoV0)).toByteString
      )
  }
}

object DomainStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[DomainStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(proto)
      domainStatus <- ProtoConverter
        .parse[DomainStatus, v0.DomainStatusInfo](
          v0.DomainStatusInfo.parseFrom,
          domainStatusInfoP => {
            for {
              participants <- domainStatusInfoP.connectedParticipants.traverse(pId =>
                ParticipantId.fromProtoPrimitive(pId, s"DomainStatus.connectedParticipants")
              )
              sequencer <- ProtoConverter.parseRequired(
                SequencerHealthStatus.fromProto,
                "sequencer",
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
            )
          },
          proto.extra,
        )
    } yield domainStatus
}

final case class ParticipantStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    connectedDomains: Map[DomainId, Boolean],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
) extends NodeStatus.Status {
  val id: ParticipantId = ParticipantId(uid)
  override def pretty: Pretty[ParticipantStatus] =
    prettyOfString(_ =>
      Seq(
        s"Participant id: ${id.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected domains: ${multiline(connectedDomains.filter(_._2).map(_._1.toString).toSeq)}",
        s"Unhealthy domains: ${multiline(connectedDomains.filterNot(_._2).map(_._1.toString).toSeq)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
      ).mkString(System.lineSeparator())
    )

  def toProtoV0: v0.NodeStatus.Status = {
    val domains = connectedDomains.map { case (domainId, healthy) =>
      v0.ParticipantStatusInfo.ConnectedDomain(
        domain = domainId.toProtoPrimitive,
        healthy = healthy,
      )
    }.toList
    SimpleStatus(uid, uptime, ports, active, topologyQueue, components).toProtoV0
      .copy(extra = v0.ParticipantStatusInfo(domains, active).toByteString)
  }
}

object ParticipantStatus {

  private def connectedDomainFromProtoV0(
      proto: v0.ParticipantStatusInfo.ConnectedDomain
  ): ParsingResult[(DomainId, Boolean)] = {
    DomainId.fromProtoPrimitive(proto.domain, s"ParticipantStatus.connectedDomains").map {
      domainId =>
        (domainId, proto.healthy)
    }
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
              connectedDomains.toMap: Map[DomainId, Boolean],
              participantStatusInfoP.active,
              status.topologyQueue,
              status.components,
            ),
          proto.extra,
        )
    } yield participantStatus
}

final case class SequencerNodeStatus(
    uid: UniqueIdentifier,
    domainId: DomainId,
    uptime: Duration,
    ports: Map[String, Port],
    connectedParticipants: Seq[ParticipantId],
    sequencer: SequencerHealthStatus,
    topologyQueue: TopologyQueueStatus,
    admin: Option[SequencerAdminStatus],
    components: Seq[ComponentStatus],
) extends NodeStatus.Status {
  override def active: Boolean = sequencer.isActive
  def toProtoV0: v0.NodeStatus.Status = {
    val participants = connectedParticipants.map(_.toProtoPrimitive)
    SimpleStatus(uid, uptime, ports, active, topologyQueue, components).toProtoV0.copy(
      extra = v0
        .SequencerNodeStatus(
          participants,
          sequencer.toProtoV0.some,
          domainId.toProtoPrimitive,
          admin.map(_.toProtoV0),
        )
        .toByteString
    )
  }

  override def pretty: Pretty[SequencerNodeStatus] =
    prettyOfString(_ =>
      (
        Seq(
          s"Sequencer id: ${uid.toProtoPrimitive}",
          s"Domain id: ${domainId.toProtoPrimitive}",
          show"Uptime: $uptime",
          s"Ports: ${portsString(ports)}",
          s"Connected Participants: ${multiline(connectedParticipants.map(_.toString))}",
          show"Sequencer: $sequencer",
          s"details-extra: ${sequencer.details}",
          s"Components: ${multiline(components.map(_.toString))}",
        ) ++
          admin.toList
            .map(adminStatus => s"Accepts admin changes: ${adminStatus.acceptsAdminChanges}"),
      ).mkString(System.lineSeparator())
    )
}

object SequencerNodeStatus {
  def fromProtoV0(
      sequencerP: v0.NodeStatus.Status
  ): ParsingResult[SequencerNodeStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(sequencerP)
      sequencerNodeStatus <- ProtoConverter.parse[SequencerNodeStatus, v0.SequencerNodeStatus](
        v0.SequencerNodeStatus.parseFrom,
        sequencerNodeStatusP =>
          for {
            participants <- sequencerNodeStatusP.connectedParticipants.traverse(pId =>
              ParticipantId.fromProtoPrimitive(pId, s"SequencerNodeStatus.connectedParticipants")
            )
            sequencer <- ProtoConverter.parseRequired(
              SequencerHealthStatus.fromProto,
              "sequencer",
              sequencerNodeStatusP.sequencer,
            )
            domainId <- DomainId.fromProtoPrimitive(
              sequencerNodeStatusP.domainId,
              s"SequencerNodeStatus.domainId",
            )
            admin <- sequencerNodeStatusP.admin.traverse(SequencerAdminStatus.fromProto)
          } yield SequencerNodeStatus(
            status.uid,
            domainId,
            status.uptime,
            status.ports,
            participants,
            sequencer,
            status.topologyQueue,
            admin,
            status.components,
          ),
        sequencerP.extra,
      )
    } yield sequencerNodeStatus

}

final case class MediatorNodeStatus(
    uid: UniqueIdentifier,
    domainId: DomainId,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
) extends NodeStatus.Status {
  override def pretty: Pretty[MediatorNodeStatus] =
    prettyOfString(_ =>
      Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        s"Domain id: ${domainId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
      ).mkString(System.lineSeparator())
    )

  def toProtoV0: v0.NodeStatus.Status =
    SimpleStatus(uid, uptime, ports, active, topologyQueue, components).toProtoV0.copy(
      extra = v0
        .MediatorNodeStatus(domainId.toProtoPrimitive)
        .toByteString
    )
}

object MediatorNodeStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[MediatorNodeStatus] =
    for {
      status <- SimpleStatus.fromProtoV0(proto)
      mediatorNodeStatus <- ProtoConverter.parse[MediatorNodeStatus, v0.MediatorNodeStatus](
        v0.MediatorNodeStatus.parseFrom,
        mediatorNodeStatusP =>
          for {
            domainId <- DomainId.fromProtoPrimitive(
              mediatorNodeStatusP.domainId,
              s"MediatorNodeStatus.domainId",
            )
          } yield MediatorNodeStatus(
            status.uid,
            domainId,
            status.uptime,
            status.ports,
            status.active,
            status.topologyQueue,
            status.components,
          ),
        proto.extra,
      )
    } yield mediatorNodeStatus
}
