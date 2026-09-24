// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health.admin.data

import cats.syntax.functor.*
import cats.syntax.option.*
import com.digitalasset.canton.ProtoDeserializationError.UnrecognizedEnum
import com.digitalasset.canton.admin.health.v30
import com.digitalasset.canton.admin.health.v30.NotInitialized.WaitingForExternalInput as V30WaitingForExternalInput
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
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
    override protected def pretty: Pretty[Failure] = prettyOfString(_.msg)
    override def trySuccess: Nothing =
      sys.error(s"Status did not complete successfully. Error: $msg")
    override def successOption: Option[Nothing] = None

    override def isActive: Option[Boolean] = None
  }

  /** A node is running but not yet initialized. */
  final case class NotInitialized(active: Boolean, waitingFor: Option[WaitingForExternalInput])
      extends NodeStatus[Nothing] {
    override protected def pretty: Pretty[NotInitialized] =
      prettyOfClass(param("active", _.active), paramIfDefined("waitingFor", _.waitingFor))
    override def trySuccess: Nothing = sys.error(s"Node is not yet initialized.")
    override def successOption: Option[Nothing] = None

    override def isActive: Option[Boolean] = Some(active)

    def toProtoV30: v30.NotInitialized = {

      val waitingForP = waitingFor match {
        case Some(value) =>
          value match {
            case WaitingForId => V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_ID
            case WaitingForNodeTopology =>
              V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_NODE_TOPOLOGY
            case WaitingForInitialization =>
              V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_INITIALIZATION
          }
        case None => V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_UNSPECIFIED
      }

      v30.NotInitialized(active, waitingForP)
    }
  }

  final case class Success[S <: Status](status: S) extends NodeStatus[S] {
    override def trySuccess: S = status
    override protected def pretty: Pretty[Success.this.type] = prettyOfParam(_.status)
    override def successOption: Option[S] = status.some

    override def isActive: Option[Boolean] = Some(status.active)
  }

  trait Status extends PrettyPrinting with Product with Serializable {
    def uid: UniqueIdentifier
    def uptime: Duration
    def ports: Map[String, Port]
    def active: Boolean
    def version: ReleaseVersion
    def topologyQueue: TopologyQueueStatus

    def toProtoV30: v30.Status =
      v30.Status(
        uid.toProtoPrimitive,
        Some(DurationConverter.toProtoPrimitive(uptime)),
        ports.fmap(_.unwrap),
        active,
        topologyQueues = Some(topologyQueue.toProtoV30),
        components = components.map(_.toProtoV30),
        version = version.fullVersion,
      )

    def components: Seq[ComponentStatus]
  }

  def portsString(ports: Map[String, Port]): String =
    multiline(ports.map { case (portDescription, port) =>
      s"$portDescription: ${port.unwrap}"
    }.toSeq)

  def multiline(elements: Iterable[String]): String =
    if (elements.isEmpty) "None" else elements.map(el => s"${System.lineSeparator()}\t$el").mkString

  def protocolVersionsString(pvs: Seq[ProtocolVersion]): Seq[String] =
    Seq(s"Supported protocol version(s): ${pvs.mkString(", ")}")
}

sealed abstract class WaitingForExternalInput extends PrettyPrinting {
  def toProtoV30: V30WaitingForExternalInput
}
case object WaitingForId extends WaitingForExternalInput {
  override protected def pretty: Pretty[WaitingForId.this.type] = prettyOfString(_ => "ID")

  override def toProtoV30: V30WaitingForExternalInput =
    V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_ID
}
case object WaitingForNodeTopology extends WaitingForExternalInput {
  override protected def pretty: Pretty[WaitingForNodeTopology.this.type] =
    prettyOfString(_ => "Node Topology")

  override def toProtoV30: V30WaitingForExternalInput =
    V30WaitingForExternalInput.WAITING_FOR_EXTERNAL_INPUT_NODE_TOPOLOGY
}
case object WaitingForInitialization extends WaitingForExternalInput {
  override protected def pretty: Pretty[WaitingForInitialization.this.type] =
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

/** Topology manager queue status
  *
  * Status around topology management queues
  * @param manager
  *   number of queued commands in the topology manager
  * @param dispatcher
  *   number of queued transactions in the dispatcher
  * @param clients
  *   number of observed transactions that are not yet effective
  */
final case class TopologyQueueStatus(manager: Int, dispatcher: Int, clients: Int)
    extends PrettyPrinting {
  def toProtoV30: v30.TopologyQueueStatus =
    v30.TopologyQueueStatus(manager = manager, dispatcher = dispatcher, clients = clients)

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
