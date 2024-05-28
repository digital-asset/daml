// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health.admin.data

import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.{InvariantViolation, UnrecognizedEnum}
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.ComponentHealthState.UnhealthyState
import com.digitalasset.canton.health.admin.data.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.admin.v30
import com.digitalasset.canton.health.admin.v30.StatusResponse.NotInitialized.WaitingForExternalInput as V30WaitingForExternalInput
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
  final case class NotInitialized(active: Boolean, waitingFor: Option[WaitingForExternalInput])
      extends NodeStatus[Nothing] {
    override def pretty: Pretty[NotInitialized] =
      prettyOfClass(param("active", _.active), paramIfDefined("waitingFor", _.waitingFor))
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
    def toProtoV30: v30.StatusResponse.Status // explicitly making it public
    def components: Seq[ComponentStatus]
  }

  private[data] def portsString(ports: Map[String, Port]): String =
    multiline(ports.map { case (portDescription, port) =>
      s"$portDescription: ${port.unwrap}"
    }.toSeq)
  private[data] def multiline(elements: Seq[String]): String =
    if (elements.isEmpty) "None" else elements.map(el => s"\n\t$el").mkString
}

sealed abstract class WaitingForExternalInput extends PrettyPrinting {
  def toProtoV30: V30WaitingForExternalInput
}
case object WaitingForId extends WaitingForExternalInput {
  override def pretty: Pretty[WaitingForId.this.type] = prettyOfString(_ => "ID")

  override def toProtoV30: V30WaitingForExternalInput =
    V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_ID
}
case object WaitingForNodeTopology extends WaitingForExternalInput {
  override def pretty: Pretty[WaitingForNodeTopology.this.type] =
    prettyOfString(_ => "Node Topology")

  override def toProtoV30: V30WaitingForExternalInput =
    V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_NODE_TOPOLOGY
}
case object WaitingForInitialization extends WaitingForExternalInput {
  override def pretty: Pretty[WaitingForInitialization.this.type] =
    prettyOfString(_ => "Initialization")

  override def toProtoV30: V30WaitingForExternalInput =
    V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_INITIALIZATION
}

object WaitingForExternalInput {
  def fromProtoV30(
      externalInput: V30WaitingForExternalInput
  ): ParsingResult[Option[WaitingForExternalInput]] = externalInput match {
    case V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_UNSPECIFIED => Right(None)
    case V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_ID => Right(Some(WaitingForId))
    case V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_NODE_TOPOLOGY =>
      Right(Some(WaitingForNodeTopology))
    case V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_INITIALIZATION =>
      Right(Some(WaitingForInitialization))
    case V30WaitingForExternalInput.Unrecognized(unrecognizedValue) =>
      Left(UnrecognizedEnum("waiting_for_external_input", unrecognizedValue))
  }
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

  def toProtoV30: v30.StatusResponse.Status =
    v30.StatusResponse.Status(
      uid.toProtoPrimitive,
      Some(DurationConverter.toProtoPrimitive(uptime)),
      ports.fmap(_.unwrap),
      ByteString.EMPTY,
      active,
      topologyQueues = Some(topologyQueue.toProto),
      components = components.map(_.toProtoV30),
    )
}

object SimpleStatus {
  def fromProtoV30(proto: v30.StatusResponse.Status): ParsingResult[SimpleStatus] = {
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
      components <- proto.components.toList.traverse(ComponentStatus.fromProtoV30)
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
  def toProtoV30: v30.SequencerHealthStatus = v30.SequencerHealthStatus(isActive, details)

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
      statusP: v30.SequencerHealthStatus
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
  def toProtoV30: v30.SequencerAdminStatus = v30.SequencerAdminStatus(acceptsAdminChanges)

  override def toComponentHealthState: ComponentHealthState =
    ComponentHealthState.Ok(Option.when(acceptsAdminChanges)("sequencer accepts admin commands"))

  override def pretty: Pretty[SequencerAdminStatus] =
    SequencerAdminStatus.prettySequencerHealthStatus
}

object SequencerAdminStatus extends PrettyUtil with ShowUtil {
  def fromProto(
      statusP: v30.SequencerAdminStatus
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
  def toProto: v30.TopologyQueueStatus =
    v30.TopologyQueueStatus(manager = manager, dispatcher = dispatcher, clients = clients)

  def isIdle: Boolean = Seq(manager, dispatcher, clients).forall(_ == 0)

  override def pretty: Pretty[TopologyQueueStatus.this.type] = prettyOfClass(
    param("manager", _.manager),
    param("dispatcher", _.dispatcher),
    param("clients", _.clients),
  )
}

object TopologyQueueStatus {
  def fromProto(
      statusP: v30.TopologyQueueStatus
  ): ParsingResult[TopologyQueueStatus] = {
    val v30.TopologyQueueStatus(manager, dispatcher, clients) = statusP
    Right(TopologyQueueStatus(manager = manager, dispatcher = dispatcher, clients = clients))
  }
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

  def toProtoV30: v30.StatusResponse.Status = {
    val domains = connectedDomains.map { case (domainId, healthy) =>
      v30.ParticipantStatusInfo.ConnectedDomain(
        domain = domainId.toProtoPrimitive,
        healthy = healthy,
      )
    }.toList
    SimpleStatus(uid, uptime, ports, active, topologyQueue, components).toProtoV30
      .copy(extra = v30.ParticipantStatusInfo(domains, active).toByteString)
  }
}

object ParticipantStatus {

  private def connectedDomainFromProtoV30(
      proto: v30.ParticipantStatusInfo.ConnectedDomain
  ): ParsingResult[(DomainId, Boolean)] = {
    DomainId.fromProtoPrimitive(proto.domain, s"ParticipantStatus.connectedDomains").map {
      domainId =>
        (domainId, proto.healthy)
    }
  }

  def fromProtoV30(
      proto: v30.StatusResponse.Status
  ): ParsingResult[ParticipantStatus] =
    for {
      status <- SimpleStatus.fromProtoV30(proto)
      participantStatus <- ProtoConverter
        .parse[ParticipantStatus, v30.ParticipantStatusInfo](
          v30.ParticipantStatusInfo.parseFrom,
          participantStatusInfoP =>
            for {
              connectedDomains <- participantStatusInfoP.connectedDomains.traverse(
                connectedDomainFromProtoV30
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
    admin: SequencerAdminStatus,
    components: Seq[ComponentStatus],
) extends NodeStatus.Status {
  override def active: Boolean = sequencer.isActive
  def toProtoV30: v30.StatusResponse.Status = {
    val participants = connectedParticipants.map(_.toProtoPrimitive)
    SimpleStatus(uid, uptime, ports, active, topologyQueue, components).toProtoV30.copy(
      extra = v30
        .SequencerNodeStatus(
          participants,
          sequencer.toProtoV30.some,
          domainId.toProtoPrimitive,
          admin.toProtoV30.some,
        )
        .toByteString
    )
  }

  override def pretty: Pretty[SequencerNodeStatus] =
    prettyOfString(_ =>
      Seq(
        s"Sequencer id: ${uid.toProtoPrimitive}",
        s"Domain id: ${domainId.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected Participants: ${multiline(connectedParticipants.map(_.toString))}",
        show"Sequencer: $sequencer",
        s"Accepts admin changes: ${admin.acceptsAdminChanges}",
        s"details-extra: ${sequencer.details}",
        s"Components: ${multiline(components.map(_.toString))}",
      ).mkString(System.lineSeparator())
    )
}

object SequencerNodeStatus {
  def fromProtoV30(
      sequencerP: v30.StatusResponse.Status
  ): ParsingResult[SequencerNodeStatus] =
    for {
      status <- SimpleStatus.fromProtoV30(sequencerP)
      sequencerNodeStatus <- ProtoConverter.parse[SequencerNodeStatus, v30.SequencerNodeStatus](
        v30.SequencerNodeStatus.parseFrom,
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
            admin <- ProtoConverter.parseRequired(
              SequencerAdminStatus.fromProto,
              "admin",
              sequencerNodeStatusP.admin,
            )
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

  def toProtoV30: v30.StatusResponse.Status =
    SimpleStatus(uid, uptime, ports, active, topologyQueue, components).toProtoV30.copy(
      extra = v30
        .MediatorNodeStatus(domainId.toProtoPrimitive)
        .toByteString
    )
}

object MediatorNodeStatus {
  def fromProtoV30(proto: v30.StatusResponse.Status): ParsingResult[MediatorNodeStatus] =
    for {
      status <- SimpleStatus.fromProtoV30(proto)
      mediatorNodeStatus <- ProtoConverter.parse[MediatorNodeStatus, v30.MediatorNodeStatus](
        v30.MediatorNodeStatus.parseFrom,
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
