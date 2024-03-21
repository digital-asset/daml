// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.admin

import cats.implicits.*
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencer.admin.v30.{
  GetPeerNetworkStatusResponse,
  PeerEndpoint,
  PeerEndpointHealth as ProtoPeerEndpointHealth,
  PeerEndpointHealthStatus as ProtoPeerEndpointHealthStatus,
  PeerEndpointStatus as ProtoPeerEndpointStatus,
}

object EnterpriseSequencerBftAdminData {

  def endpointToProto(endpoint: Endpoint): PeerEndpoint =
    PeerEndpoint.of(endpoint.host, endpoint.port.unwrap)

  def endpointFromProto(peerEndpoint: PeerEndpoint): Either[String, Endpoint] =
    for {
      port <- Port.create(peerEndpoint.port).fold(e => Left(e.message), Right(_))
    } yield Endpoint(peerEndpoint.host, port)

  sealed trait PeerEndpointHealthStatus extends Product with Serializable
  object PeerEndpointHealthStatus {
    case object Unknown extends PeerEndpointHealthStatus
    case object Unauthenticated extends PeerEndpointHealthStatus
    case object Authenticated extends PeerEndpointHealthStatus
  }

  final case class PeerEndpointHealth(status: PeerEndpointHealthStatus, description: Option[String])
  final case class PeerEndpointStatus(endpoint: Endpoint, health: PeerEndpointHealth) {

    def toProto: ProtoPeerEndpointStatus =
      ProtoPeerEndpointStatus.of(
        Some(endpointToProto(endpoint)),
        Some(
          ProtoPeerEndpointHealth.of(
            health.status match {
              case PeerEndpointHealthStatus.Unknown =>
                ProtoPeerEndpointHealthStatus.PEER_ENDPOINT_HEALTH_STATUS_UNKNOWN_ENDPOINT
              case PeerEndpointHealthStatus.Unauthenticated =>
                ProtoPeerEndpointHealthStatus.PEER_ENDPOINT_HEALTH_STATUS_UNAUTHENTICATED
              case PeerEndpointHealthStatus.Authenticated =>
                ProtoPeerEndpointHealthStatus.PEER_ENDPOINT_HEALTH_STATUS_AUTHENTICATED
            },
            health.description,
          )
        ),
      )
  }

  final case class PeerNetworkStatus(endpointStatuses: Seq[PeerEndpointStatus]) {
    def +(status: PeerEndpointStatus): PeerNetworkStatus =
      copy(endpointStatuses = endpointStatuses :+ status)
  }

  object PeerNetworkStatus {

    def fromProto(response: GetPeerNetworkStatusResponse): Either[String, PeerNetworkStatus] =
      response.statuses
        .map { status =>
          for {
            protoEndpoint <- status.endpoint.toRight("Endpoint is missing")
            port <- Port.create(protoEndpoint.port).fold(e => Left(e.message), Right(_))
            endpoint = Endpoint(protoEndpoint.host, port)
            protoHealth <- status.health.toRight("Health is missing")
            healthDescription = protoHealth.description
            health <- protoHealth.status match {
              case ProtoPeerEndpointHealthStatus.PEER_ENDPOINT_HEALTH_STATUS_UNKNOWN_ENDPOINT =>
                Right(PeerEndpointHealthStatus.Unknown)
              case ProtoPeerEndpointHealthStatus.PEER_ENDPOINT_HEALTH_STATUS_UNAUTHENTICATED =>
                Right(PeerEndpointHealthStatus.Unauthenticated)
              case ProtoPeerEndpointHealthStatus.PEER_ENDPOINT_HEALTH_STATUS_AUTHENTICATED =>
                Right(PeerEndpointHealthStatus.Authenticated)
              case ProtoPeerEndpointHealthStatus.Unrecognized(unrecognizedValue) =>
                Left(s"Health status is unrecognised: $unrecognizedValue")
              case ProtoPeerEndpointHealthStatus.PEER_ENDPOINT_HEALTH_STATUS_UNSPECIFIED =>
                Left("Health status is unspecified")
            }
          } yield PeerEndpointStatus(endpoint, PeerEndpointHealth(health, healthDescription))
        }
        .sequence
        .map(PeerNetworkStatus(_))
  }
}
