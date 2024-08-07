// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.admin.api.client.data.NodeStatus.versionString
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.NodeStatus.{multiline, portsString}
import com.digitalasset.canton.health.admin.{v0, v1}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{DurationConverter, ParsingResult}
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

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
    def components: Seq[ComponentStatus]
  }

  def portsString(ports: Map[String, Port]): String =
    multiline(ports.map { case (portDescription, port) =>
      s"$portDescription: ${port.unwrap}"
    }.toSeq)

  def multiline(elements: Seq[String]): String =
    if (elements.isEmpty) "None" else elements.map(el => s"\n\t$el").mkString

  def versionString(version: Option[ReleaseVersion]): Seq[String] =
    version match {
      case Some(version) => Seq(s"Version: ${version.fullVersion}")
      case None => Seq()
    }

  def protocolVersionString(pv: Option[ProtocolVersion]): Seq[String] =
    pv match {
      case Some(pv) => Seq(s"Protocol version: ${pv.toString}")
      case None => Seq()
    }

  def protocolVersionsString(pvs: Option[Seq[ProtocolVersion]]): Seq[String] =
    pvs match {
      case Some(pvs) => Seq(s"Supported protocol version(s): ${pvs.mkString(", ")}")
      case None => Seq()
    }
}

final case class SimpleStatus(
    uid: UniqueIdentifier,
    uptime: Duration,
    ports: Map[String, Port],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: Option[ReleaseVersion],
) extends NodeStatus.Status {
  override def pretty: Pretty[SimpleStatus] =
    prettyOfString(_ =>
      (Seq(
        s"Node uid: ${uid.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
      ) ++ versionString(version)).mkString(System.lineSeparator())
    )
}

object SimpleStatus {
  def fromProtoV0(proto: v0.NodeStatus.Status): ParsingResult[SimpleStatus] =
    for {
      uid <- UniqueIdentifier.fromProtoPrimitive(proto.id, "NodeStatus.Status.id")
      uptime <- ProtoConverter
        .required("NodeStatus.Status.uptime", proto.uptime)
        .flatMap(DurationConverter.fromProtoPrimitive)
      ports <- proto.ports.toList
        .traverse { case (s, i) =>
          Port.create(i).leftMap(InvariantViolation.toProtoDeserializationError).map(p => (s, p))
        }
        .map(_.toMap)
      topology <- ProtoConverter.parseRequired(
        TopologyQueueStatus.fromProto,
        "TopologyQueueStatus.topology_queues",
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
      version = None,
    )

  def fromProtoV1(proto: v1.Status): ParsingResult[SimpleStatus] =
    for {
      uid <- UniqueIdentifier.fromProtoPrimitive(proto.uid, "v1.Status.uid")
      uptime <- ProtoConverter
        .required("v1.Status.uptime", proto.uptime)
        .flatMap(DurationConverter.fromProtoPrimitive)
      ports <- proto.ports.toList
        .traverse { case (s, i) =>
          Port.create(i).leftMap(InvariantViolation.toProtoDeserializationError).map(p => (s, p))
        }
        .map(_.toMap)
      topology <- ProtoConverter.parseRequired(
        TopologyQueueStatus.fromProto,
        "TopologyQueueStatus.topology_queues",
        proto.topologyQueues,
      )
      components <- proto.components.toList.traverse(ComponentStatus.fromProtoV0)
      version <- ReleaseVersion.fromProtoPrimitive(proto.version, "v1.Status.version")
    } yield SimpleStatus(
      uid,
      uptime,
      ports,
      proto.active,
      topology,
      components,
      Some(version),
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
