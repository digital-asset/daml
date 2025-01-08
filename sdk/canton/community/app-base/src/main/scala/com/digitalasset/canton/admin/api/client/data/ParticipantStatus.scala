// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.NodeStatus.*
import com.digitalasset.canton.admin.api.client.data.ParticipantStatus.SubmissionReady
import com.digitalasset.canton.admin.participant.v30 as participantV30
import com.digitalasset.canton.admin.participant.v30.ConnectedDomain.Health
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.admin.data.NodeStatus.multiline
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.time.Duration
import scala.collection.immutable

final case class ParticipantStatus(
    id: ParticipantId,
    uptime: Duration,
    ports: Map[String, Port],
    connectedSynchronizers: Map[SynchronizerId, SubmissionReady],
    active: Boolean,
    topologyQueue: TopologyQueueStatus,
    components: Seq[ComponentStatus],
    version: ReleaseVersion,
    supportedProtocolVersions: Seq[ProtocolVersion],
) extends NodeStatus.Status {

  val uid: UniqueIdentifier = id.uid

  private def connectedHealthyDomains: immutable.Iterable[SynchronizerId] =
    connectedSynchronizers.collect {
      case (synchronizerId, submissionReady) if submissionReady.unwrap => synchronizerId
    }

  private def connectedUnhealthyDomains: immutable.Iterable[SynchronizerId] =
    connectedSynchronizers.collect {
      case (synchronizerId, submissionReady) if !submissionReady.unwrap => synchronizerId
    }

  override protected def pretty: Pretty[ParticipantStatus] =
    prettyOfString(_ =>
      Seq(
        s"Participant id: ${id.toProtoPrimitive}",
        show"Uptime: $uptime",
        s"Ports: ${portsString(ports)}",
        s"Connected synchronizers: ${multiline(connectedHealthyDomains.map(_.toString))}",
        s"Unhealthy synchronizers: ${multiline(connectedUnhealthyDomains.map(_.toString))}",
        s"Active: $active",
        s"Components: ${multiline(components.map(_.toString))}",
        s"Version: $version",
        s"Supported protocol version(s): ${supportedProtocolVersions.mkString(", ")}",
      ).mkString(System.lineSeparator())
    )
}

object ParticipantStatus {
  final case class SubmissionReady(v: Boolean) {
    def unwrap: Boolean = v
  }

  private def connectedDomainFromProtoV30(
      proto: participantV30.ConnectedDomain
  ): ParsingResult[(SynchronizerId, SubmissionReady)] = for {
    synchronizerId <- SynchronizerId.fromProtoPrimitive(proto.synchronizerId, "synchronizer_id")
    isHealth <- proto.health match {
      case Health.HEALTH_UNSPECIFIED => Left(ProtoDeserializationError.FieldNotSet("health"))
      case Health.HEALTH_HEALTHY => Right(true)
      case Health.HEALTH_UNHEALTHY => Right(false)
      case Health.Unrecognized(i) => Left(ProtoDeserializationError.UnrecognizedEnum("health", i))
    }
  } yield (synchronizerId, SubmissionReady(isHealth))

  def fromProtoV30(
      proto: participantV30.ParticipantStatusResponse
  ): ParsingResult[NodeStatus[ParticipantStatus]] =
    proto.kind match {
      case participantV30.ParticipantStatusResponse.Kind.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("ParticipantStatusResponse.Kind"))

      case participantV30.ParticipantStatusResponse.Kind.Status(participantStatusP) =>
        for {
          statusP <- ProtoConverter.required(
            "ParticipantStatusResponse.common_status",
            participantStatusP.commonStatus,
          )
          status <- SimpleStatus.fromProtoV30(statusP)
          connectedDomains <- participantStatusP.connectedDomains.traverse(
            connectedDomainFromProtoV30
          )
          supportedProtocolVersions <- participantStatusP.supportedProtocolVersions.traverse(
            ProtocolVersion.fromProtoPrimitive(_, allowDeleted = true)
          )
        } yield NodeStatus.Success(
          ParticipantStatus(
            ParticipantId(status.uid),
            status.uptime,
            status.ports,
            connectedDomains.toMap,
            active = status.active,
            status.topologyQueue,
            status.components,
            status.version,
            supportedProtocolVersions,
          )
        )

      case participantV30.ParticipantStatusResponse.Kind.NotInitialized(notInitialized) =>
        WaitingForExternalInput
          .fromProtoV30(notInitialized.waitingForExternalInput)
          .map(NodeStatus.NotInitialized(notInitialized.active, _))
    }
}
