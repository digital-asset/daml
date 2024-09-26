// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.{InvariantViolation, UnrecognizedEnum}
import com.digitalasset.canton.admin.api.client.data.NodeStatus.*
import com.digitalasset.canton.admin.health.v30
import com.digitalasset.canton.admin.health.v30.NotInitialized.WaitingForExternalInput as V30WaitingForExternalInput
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{DurationConverter, ParsingResult}
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.version.ReleaseVersion

import java.time.Duration

sealed trait NodeStatus[+S <: NodeStatus.Status]
    extends PrettyPrinting
    with Product
    with Serializable {
  def trySuccess: S
  def successOption: Option[S]

  def isRunning: Boolean
  def isInitialized: Boolean

  /** Return the node activeness if it is known or None otherwise.
    */
  def isActive: Option[Boolean]
}

object NodeStatus {

  /** A failure to query the node's status */
  final case class Failure(msg: String) extends NodeStatus[Nothing] {
    override protected def pretty: Pretty[Failure] = prettyOfString(_.msg)
    override def trySuccess: Nothing =
      sys.error(s"Status did not complete successfully. Error: $msg")
    override def successOption: Option[Nothing] = None

    override def isActive: Option[Boolean] = None

    override def isInitialized: Boolean = false

    override def isRunning: Boolean = false
  }

  /** A node is running but not yet initialized. */
  final case class NotInitialized(active: Boolean, waitingFor: Option[WaitingForExternalInput])
      extends NodeStatus[Nothing] {
    override protected def pretty: Pretty[NotInitialized] =
      prettyOfClass(param("active", _.active), paramIfDefined("waitingFor", _.waitingFor))
    override def trySuccess: Nothing = sys.error(s"Node is not yet initialized.")
    override def successOption: Option[Nothing] = None

    override def isActive: Option[Boolean] = Some(active)

    override def isInitialized: Boolean = false

    override def isRunning: Boolean = true
  }

  final case class Success[S <: Status](status: S) extends NodeStatus[S] {
    override def trySuccess: S = status
    override protected def pretty: Pretty[Success.this.type] = prettyOfParam(_.status)
    override def successOption: Option[S] = status.some

    override def isActive: Option[Boolean] = Some(status.active)

    override def isInitialized: Boolean = true

    override def isRunning: Boolean = true
  }

  trait Status extends PrettyPrinting with Product with Serializable {
    def uid: UniqueIdentifier
    def uptime: Duration
    def ports: Map[String, Port]
    def active: Boolean
    def components: Seq[ComponentStatus]
  }

  def portsString(ports: Map[String, Port]): String =
    multiline(ports.map { case (portDescription, port) =>
      s"$portDescription: ${port.unwrap}"
    }.toSeq)

  def multiline(elements: Seq[String]): String =
    if (elements.isEmpty) "None" else elements.map(el => s"${System.lineSeparator()}\t$el").mkString
}

sealed abstract class WaitingForExternalInput extends PrettyPrinting

case object WaitingForId extends WaitingForExternalInput {
  override protected def pretty: Pretty[WaitingForId.this.type] = prettyOfString(_ => "ID")
}

case object WaitingForNodeTopology extends WaitingForExternalInput {
  override protected def pretty: Pretty[WaitingForNodeTopology.this.type] =
    prettyOfString(_ => "Node Topology")
}

case object WaitingForInitialization extends WaitingForExternalInput {
  override protected def pretty: Pretty[WaitingForInitialization.this.type] =
    prettyOfString(_ => "Initialization")
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
    version: ReleaseVersion,
) extends NodeStatus.Status {
  override protected def pretty: Pretty[SimpleStatus] =
    prettyOfString(_ =>
      Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: $version",
      ).mkString(System.lineSeparator())
    )
}

object SimpleStatus {
  def fromProtoV30(proto: v30.Status): ParsingResult[SimpleStatus] =
    for {
      uid <- UniqueIdentifier.fromProtoPrimitive(proto.uid, "Status.uid")
      uptime <- ProtoConverter
        .required("Status.uptime", proto.uptime)
        .flatMap(DurationConverter.fromProtoPrimitive)
      ports <- proto.ports.toList
        .traverse { case (description, port) =>
          Port
            .create(port)
            .leftMap(InvariantViolation.toProtoDeserializationError("port", _))
            .map(p => (description, p))
        }
        .map(_.toMap)
      topology <- ProtoConverter.parseRequired(
        TopologyQueueStatus.fromProto,
        "TopologyQueueStatus.topology_queues",
        proto.topologyQueues,
      )
      components <- proto.components.toList.traverse(ComponentStatus.fromProtoV30)
      version <- ReleaseVersion.fromProtoPrimitive(proto.version, "Status.version")
    } yield SimpleStatus(
      uid,
      uptime,
      ports,
      proto.active,
      topology,
      components,
      version,
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
  def isIdle: Boolean = Seq(manager, dispatcher, clients).forall(_ == 0)

  override protected def pretty: Pretty[TopologyQueueStatus.this.type] = prettyOfClass(
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
