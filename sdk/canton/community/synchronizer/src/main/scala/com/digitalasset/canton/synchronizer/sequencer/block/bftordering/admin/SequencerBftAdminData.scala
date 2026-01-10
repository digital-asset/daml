// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin

import cats.implicits.*
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, Port}
import com.digitalasset.canton.config.{PemFile, PemString, TlsClientCertificate, TlsClientConfig}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyNameOnlyCase, PrettyPrinting}
import com.digitalasset.canton.sequencer.admin.v30.PeerEndpoint.Security
import com.digitalasset.canton.sequencer.admin.v30.PeerEndpoint.Security.Empty
import com.digitalasset.canton.sequencer.admin.v30.{
  Authenticated as ProtoAuthenticated,
  GetOrderingTopologyResponse,
  GetPeerNetworkStatusResponse,
  GetWriteReadinessResponse,
  PeerConnectionStatus as ProtoPeerConnectionStatus,
  PeerEndpoint as ProtoPeerEndpoint,
  PeerEndpointHealth as ProtoPeerEndpointHealth,
  PeerEndpointHealthStatus as ProtoPeerEndpointHealthStatus,
  PeerEndpointId as ProtoPeerEndpointId,
  PeerEndpointStatus as ProtoPeerEndpointStatus,
  PlainTextPeerEndpoint as ProtoPlainTextPeerEndpoint,
  TlsClientCertificate as ProtoTlsClientCertificate,
  TlsPeerEndpoint as ProtoTlsPeerEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.P2PEndpointConfig
import com.digitalasset.canton.topology.SequencerId

import scala.util.{Failure, Success, Try}

object SequencerBftAdminData {

  def endpointToProto(endpoint: P2PEndpoint): ProtoPeerEndpoint =
    ProtoPeerEndpoint(
      endpoint.address,
      endpoint.port.unwrap,
      endpoint match {
        case _: P2PGrpcNetworking.PlainTextP2PEndpoint =>
          ProtoPeerEndpoint.Security.PlainText(ProtoPlainTextPeerEndpoint())
        case P2PGrpcNetworking.TlsP2PEndpoint(clientConfig) =>
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
          Right(P2PGrpcNetworking.PlainTextP2PEndpoint(address, port))
        case Security.Tls(tls) =>
          def rightTlsP2PEndpoint(
              clientCertificate: Option[TlsClientCertificate]
          ): Right[String, P2PGrpcNetworking.TlsP2PEndpoint] =
            Right(
              P2PGrpcNetworking.TlsP2PEndpoint(
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

  sealed trait PeerEndpointHealthStatus extends PrettyNameOnlyCase with Serializable
  object PeerEndpointHealthStatus {
    case object UnknownEndpoint extends PeerEndpointHealthStatus
    case object Disconnected extends PeerEndpointHealthStatus
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

  sealed trait PeerConnectionStatus extends PrettyPrinting with Product with Serializable {
    def toProto: ProtoPeerConnectionStatus
  }
  object PeerConnectionStatus {

    final case class PeerEndpointStatus(
        p2pEndpointId: P2PEndpoint.Id,
        isOutgoingConnection: Boolean,
        health: PeerEndpointHealth,
    ) extends PeerConnectionStatus {

      override val pretty: Pretty[PeerEndpointStatus] =
        prettyOfClass(param("p2pEndpointId", _.p2pEndpointId), param("health", _.health))

      def toProto: ProtoPeerConnectionStatus =
        ProtoPeerConnectionStatus(
          ProtoPeerConnectionStatus.Status.PeerEndpointStatus(
            ProtoPeerEndpointStatus(
              Some(endpointIdToProto(p2pEndpointId)),
              isOutgoingConnection,
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
                    case PeerEndpointHealthStatus.Disconnected =>
                      Some(
                        ProtoPeerEndpointHealthStatus(
                          ProtoPeerEndpointHealthStatus.Status.Disconnected(
                            ProtoPeerEndpointHealthStatus.Disconnected()
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
                            ProtoAuthenticated(sequencerId.toProtoPrimitive)
                          )
                        )
                      )
                  },
                  health.description,
                )
              ),
            )
          )
        )
    }

    final case class PeerIncomingConnection(sequencerId: SequencerId) extends PeerConnectionStatus {

      override val pretty: Pretty[PeerIncomingConnection] =
        prettyOfClass(param("sequencerId", _.sequencerId))

      override def toProto: ProtoPeerConnectionStatus =
        ProtoPeerConnectionStatus(
          ProtoPeerConnectionStatus.Status.PeerIncomingConnection(
            ProtoAuthenticated(sequencerId.toProtoPrimitive)
          )
        )
    }
  }

  final case class PeerNetworkStatus(endpointStatuses: Seq[PeerConnectionStatus])
      extends PrettyPrinting {

    override val pretty: Pretty[PeerNetworkStatus] =
      prettyOfClass(param("endpoint statuses", _.endpointStatuses))

    def +(status: PeerConnectionStatus): PeerNetworkStatus =
      copy(endpointStatuses = endpointStatuses :+ status)

    def toProto: GetPeerNetworkStatusResponse =
      GetPeerNetworkStatusResponse(endpointStatuses.map(_.toProto))
  }

  object PeerNetworkStatus {

    def fromProto(response: GetPeerNetworkStatusResponse): Either[String, PeerNetworkStatus] =
      response.statuses
        .map(_.status)
        .map {
          case ProtoPeerConnectionStatus.Status.PeerEndpointStatus(
                ProtoPeerEndpointStatus(endpointId, isOutgoingConnection, health)
              ) =>
            for {
              protoEndpointId <- endpointId.toRight("Endpoint ID is missing")
              port <- Port.create(protoEndpointId.port).fold(e => Left(e.message), Right(_))
              endpointId = P2PEndpoint.Id(
                protoEndpointId.address,
                port,
                protoEndpointId.tls,
              )
              protoHealth <- health.toRight("Health is missing")
              healthDescription = protoHealth.description
              health <- protoHealth.status.toRight("Health status is missing")
              healthStatus <- health match {
                case ProtoPeerEndpointHealthStatus(
                      ProtoPeerEndpointHealthStatus.Status.UnknownEndpoint(_)
                    ) =>
                  Right(PeerEndpointHealthStatus.UnknownEndpoint)
                case ProtoPeerEndpointHealthStatus(
                      ProtoPeerEndpointHealthStatus.Status.Unauthenticated(_)
                    ) =>
                  Right(PeerEndpointHealthStatus.Unauthenticated)
                case ProtoPeerEndpointHealthStatus(
                      ProtoPeerEndpointHealthStatus.Status.Authenticated(
                        ProtoAuthenticated(sequencerIdString)
                      )
                    ) =>
                  SequencerId
                    .fromProtoPrimitive(sequencerIdString, "sequencerId")
                    .leftMap(_.toString)
                    .map(PeerEndpointHealthStatus.Authenticated(_))
                case ProtoPeerEndpointHealthStatus(
                      ProtoPeerEndpointHealthStatus.Status.Disconnected(_)
                    ) =>
                  Right(PeerEndpointHealthStatus.Disconnected)
                case _ =>
                  Left("Health status is empty")
              }
            } yield PeerConnectionStatus.PeerEndpointStatus(
              endpointId,
              isOutgoingConnection,
              PeerEndpointHealth(healthStatus, healthDescription),
            )
          case ProtoPeerConnectionStatus.Status.PeerIncomingConnection(authenticated) =>
            SequencerId
              .fromProtoPrimitive(authenticated.sequencerId, "sequencerId")
              .leftMap(_.message)
              .map(PeerConnectionStatus.PeerIncomingConnection(_))
          case _ =>
            Left("Peer connection status is empty")
        }
        .sequence
        .map(PeerNetworkStatus(_))
  }

  sealed trait WriteReadiness extends Product with Serializable {

    def p2p: WriteReadiness.P2P

    def toProto: GetWriteReadinessResponse =
      this match {
        case ready: WriteReadiness.Ready =>
          GetWriteReadinessResponse(
            GetWriteReadinessResponse.Readiness.Ready(
              GetWriteReadinessResponse.Ready(
                Some(ready.p2p.toProto)
              )
            )
          )
        case notReady: WriteReadiness.P2PNotReady =>
          GetWriteReadinessResponse(
            GetWriteReadinessResponse.Readiness.P2PNotReady(
              GetWriteReadinessResponse.P2PNotReady(
                Some(notReady.p2p.toProto)
              )
            )
          )
      }
  }

  object WriteReadiness {

    final case class P2P(authenticatedPeersCount: Int, requiredQuorum: Int) {

      def toProto: GetWriteReadinessResponse.P2P =
        GetWriteReadinessResponse.P2P(
          authenticatedPeersCount,
          requiredQuorum,
        )
    }

    object P2P {
      def fromProto(p2p: GetWriteReadinessResponse.P2P): P2P =
        P2P(
          p2p.authenticatedPeersCount,
          p2p.requiredQuorum,
        )
    }

    final case class Ready(override val p2p: P2P) extends WriteReadiness
    final case class P2PNotReady(override val p2p: P2P) extends WriteReadiness

    def fromProto(response: GetWriteReadinessResponse): Either[String, WriteReadiness] =
      response.readiness match {
        case GetWriteReadinessResponse.Readiness.Empty => Left("Readiness is empty")
        case GetWriteReadinessResponse.Readiness.Ready(ready) =>
          for {
            p2p <- ready.p2P.toRight("P2P info is missing")
          } yield WriteReadiness.Ready(WriteReadiness.P2P.fromProto(p2p))
        case GetWriteReadinessResponse.Readiness.P2PNotReady(notReady) =>
          for {
            p2p <- notReady.p2P.toRight("P2P info is missing")
          } yield WriteReadiness.P2PNotReady(WriteReadiness.P2P.fromProto(p2p))
      }
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
