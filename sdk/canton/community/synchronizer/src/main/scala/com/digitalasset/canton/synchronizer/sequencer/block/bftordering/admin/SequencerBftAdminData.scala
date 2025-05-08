// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin

import cats.implicits.*
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, Port}
import com.digitalasset.canton.config.{PemFile, PemString, TlsClientCertificate, TlsClientConfig}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencer.admin.v30.PeerEndpoint.Security
import com.digitalasset.canton.sequencer.admin.v30.PeerEndpoint.Security.Empty
import com.digitalasset.canton.sequencer.admin.v30.{
  GetOrderingTopologyResponse,
  GetPeerNetworkStatusResponse,
  PeerEndpoint as ProtoPeerEndpoint,
  PeerEndpointHealth as ProtoPeerEndpointHealth,
  PeerEndpointHealthStatus as ProtoPeerEndpointHealthStatus,
  PeerEndpointId as ProtoPeerEndpointId,
  PeerEndpointStatus as ProtoPeerEndpointStatus,
  PlainTextPeerEndpoint as ProtoPlainTextPeerEndpoint,
  TlsClientCertificate as ProtoTlsClientCertificate,
  TlsPeerEndpoint as ProtoTlsPeerEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.P2PEndpointConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.topology.SequencerId

import scala.util.{Failure, Success, Try}

object SequencerBftAdminData {

  def endpointToProto(endpoint: P2PEndpoint): ProtoPeerEndpoint =
    ProtoPeerEndpoint(
      endpoint.address,
      endpoint.port.unwrap,
      endpoint match {
        case _: GrpcNetworking.PlainTextP2PEndpoint =>
          ProtoPeerEndpoint.Security.PlainText(ProtoPlainTextPeerEndpoint())
        case GrpcNetworking.TlsP2PEndpoint(clientConfig) =>
          ProtoPeerEndpoint.Security.Tls(
            ProtoTlsPeerEndpoint(
              clientConfig.tlsConfig.flatMap(_.trustCollectionFile).map(_.pemBytes),
              clientConfig.tlsConfig.flatMap(_.clientCert).map { clientCertificate =>
                ProtoTlsClientCertificate(
                  clientCertificate.certChainFile.pemBytes,
                  clientCertificate.privateKeyFile.pemFile.unwrap.getAbsolutePath,
                )
              },
            )
          )
      },
    )

  def endpointIdToProto(endpointId: P2PEndpoint.Id): ProtoPeerEndpointId =
    ProtoPeerEndpointId(
      endpointId.address,
      endpointId.port.unwrap,
      endpointId.transportSecurity,
    )

  def endpointFromProto(peerEndpoint: ProtoPeerEndpoint): Either[String, P2PEndpoint] = {
    val address = peerEndpoint.address

    for {
      port <- Port.create(peerEndpoint.port).fold(e => Left(e.message), Right(_))
      peerEndpointOrError: Either[String, P2PEndpoint] = peerEndpoint.security match {
        case _: Security.PlainText =>
          Right(GrpcNetworking.PlainTextP2PEndpoint(address, port))
        case Security.Tls(tls) =>
          def rightTlsP2PEndpoint(
              clientCertificate: Option[TlsClientCertificate]
          ): Right[String, GrpcNetworking.TlsP2PEndpoint] =
            Right(
              GrpcNetworking.TlsP2PEndpoint(
                P2PEndpointConfig(
                  address,
                  port,
                  Some(
                    TlsClientConfig(
                      tls.customServerTrustCertificate.map(PemString(_)),
                      clientCert = clientCertificate,
                      enabled = true,
                    )
                  ),
                )
              )
            )
          tls.clientCertificate
            .map { clientCertificate =>
              Try(ExistingFile.tryCreate(clientCertificate.privateKeyFile)) match {
                case Failure(exception) => Left(exception.getMessage)
                case Success(privateKeyFile) =>
                  rightTlsP2PEndpoint(
                    Some(
                      TlsClientCertificate(
                        PemString(clientCertificate.certificateChain),
                        PemFile(privateKeyFile),
                      )
                    )
                  )
              }
            }
            .getOrElse(rightTlsP2PEndpoint(None))
        case Empty =>
          Left("Security is empty")
      }
      endpoint <- peerEndpointOrError
    } yield endpoint
  }

  def endpointIdFromProto(peerEndpointId: ProtoPeerEndpointId): Either[String, P2PEndpoint.Id] =
    for {
      port <- Port.create(peerEndpointId.port).fold(e => Left(e.message), Right(_))
    } yield P2PEndpoint.Id(
      peerEndpointId.address,
      port,
      peerEndpointId.tls,
    )

  sealed trait PeerEndpointHealthStatus extends Product with Serializable with PrettyPrinting {
    override val pretty: Pretty[this.type] = prettyOfObject[this.type]
  }
  object PeerEndpointHealthStatus {
    case object UnknownEndpoint extends PeerEndpointHealthStatus
    case object Unauthenticated extends PeerEndpointHealthStatus
    final case class Authenticated(sequencerId: SequencerId) extends PeerEndpointHealthStatus {
      override val pretty: Pretty[Authenticated.this.type] =
        prettyOfClass(param("sequencerId", _.sequencerId))
    }
  }

  final case class PeerEndpointHealth(status: PeerEndpointHealthStatus, description: Option[String])
      extends PrettyPrinting {

    override val pretty: Pretty[PeerEndpointHealth] =
      prettyOfClass(
        param("status", _.status),
        paramIfDefined("description", _.description.map(_.doubleQuoted)),
      )
  }

  final case class PeerEndpointStatus(
      endpointId: P2PEndpoint.Id,
      health: PeerEndpointHealth,
  ) extends PrettyPrinting {

    override val pretty: Pretty[PeerEndpointStatus] =
      prettyOfClass(param("endpointId", _.endpointId), param("health", _.health))

    def toProto: ProtoPeerEndpointStatus =
      ProtoPeerEndpointStatus(
        Some(endpointIdToProto(endpointId)),
        Some(
          ProtoPeerEndpointHealth(
            health.status match {
              case PeerEndpointHealthStatus.UnknownEndpoint =>
                Some(
                  ProtoPeerEndpointHealthStatus(
                    ProtoPeerEndpointHealthStatus.Status.UnknownEndpoint(
                      ProtoPeerEndpointHealthStatus.UnknownEndpoint()
                    )
                  )
                )
              case PeerEndpointHealthStatus.Unauthenticated =>
                Some(
                  ProtoPeerEndpointHealthStatus(
                    ProtoPeerEndpointHealthStatus.Status.Unauthenticated(
                      ProtoPeerEndpointHealthStatus.Unauthenticated()
                    )
                  )
                )
              case PeerEndpointHealthStatus.Authenticated(sequencerId) =>
                Some(
                  ProtoPeerEndpointHealthStatus(
                    ProtoPeerEndpointHealthStatus.Status.Authenticated(
                      ProtoPeerEndpointHealthStatus.Authenticated(sequencerId.toProtoPrimitive)
                    )
                  )
                )
            },
            health.description,
          )
        ),
      )
  }

  final case class PeerNetworkStatus(endpointStatuses: Seq[PeerEndpointStatus])
      extends PrettyPrinting {

    override val pretty: Pretty[PeerNetworkStatus] =
      prettyOfClass(param("endpoint statuses", _.endpointStatuses))

    def +(status: PeerEndpointStatus): PeerNetworkStatus =
      copy(endpointStatuses = endpointStatuses :+ status)

    def toProto: GetPeerNetworkStatusResponse =
      GetPeerNetworkStatusResponse(endpointStatuses.map(_.toProto))
  }

  object PeerNetworkStatus {

    def fromProto(response: GetPeerNetworkStatusResponse): Either[String, PeerNetworkStatus] =
      response.statuses
        .map { status =>
          for {
            protoEndpointId <- status.endpointId.toRight("Endpoint ID is missing")
            port <- Port.create(protoEndpointId.port).fold(e => Left(e.message), Right(_))
            endpointId = P2PEndpoint.Id(
              protoEndpointId.address,
              port,
              protoEndpointId.tls,
            )
            protoHealth <- status.health.toRight("Health is missing")
            healthDescription = protoHealth.description
            health <- protoHealth.status match {
              case Some(
                    ProtoPeerEndpointHealthStatus(
                      ProtoPeerEndpointHealthStatus.Status.UnknownEndpoint(_)
                    )
                  ) =>
                Right(PeerEndpointHealthStatus.UnknownEndpoint)
              case Some(
                    ProtoPeerEndpointHealthStatus(
                      ProtoPeerEndpointHealthStatus.Status.Unauthenticated(_)
                    )
                  ) =>
                Right(PeerEndpointHealthStatus.Unauthenticated)
              case Some(
                    ProtoPeerEndpointHealthStatus(
                      ProtoPeerEndpointHealthStatus.Status.Authenticated(
                        ProtoPeerEndpointHealthStatus.Authenticated(sequencerIdString)
                      )
                    )
                  ) =>
                SequencerId
                  .fromProtoPrimitive(sequencerIdString, "sequencerId")
                  .leftMap(_.toString)
                  .map(PeerEndpointHealthStatus.Authenticated(_))
              case _ =>
                Left("Health status is empty")
            }
          } yield PeerEndpointStatus(endpointId, PeerEndpointHealth(health, healthDescription))
        }
        .sequence
        .map(PeerNetworkStatus(_))
  }

  final case class OrderingTopology(currentEpoch: Long, sequencerIds: Seq[SequencerId]) {

    def toProto: GetOrderingTopologyResponse =
      GetOrderingTopologyResponse(currentEpoch, sequencerIds.map(SequencerNodeId.toBftNodeId))
  }

  object OrderingTopology {

    def fromProto(response: GetOrderingTopologyResponse): Either[String, OrderingTopology] =
      response.sequencerIds
        .map { sequencerIdString =>
          for {
            sequencerId <- SequencerId
              .fromProtoPrimitive(sequencerIdString, "sequencerId")
              .leftMap(_.toString)
          } yield sequencerId
        }
        .sequence
        .map(OrderingTopology(response.currentEpoch, _))
  }
}
