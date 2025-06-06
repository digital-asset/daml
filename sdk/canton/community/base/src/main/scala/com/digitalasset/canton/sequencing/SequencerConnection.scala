// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.sequencer.v30
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.grpc.{ClientChannelBuilder, ManagedChannelBuilderProxy}
import com.digitalasset.canton.networking.{Endpoint, UrlValidator}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import com.digitalasset.canton.{ProtoDeserializationError, SequencerAlias}
import com.google.protobuf.ByteString

import java.net.URI
import java.util.concurrent.Executor

/** Our [[com.digitalasset.canton.config.ClientConfig]] provides the static configuration of API
  * connections between console and nodes, and between synchronizer members via the config files.
  * Participants however can connect to multiple synchronizers and sequencers, and the configuration
  * of these connections is more dynamic. The structures below are used to represent the dynamic
  * configuration of how a participant connects to a sequencer.
  */
sealed trait SequencerConnection extends PrettyPrinting {
  def withAlias(alias: SequencerAlias): SequencerConnection

  def toProtoV30: v30.SequencerConnection

  def addEndpoints(
      connection: String,
      additionalConnections: String*
  ): Either[String, SequencerConnection] =
    addEndpoints(new URI(connection), additionalConnections.map(new URI(_))*)

  def addEndpoints(
      connection: URI,
      additionalConnections: URI*
  ): Either[String, SequencerConnection]

  def addEndpoints(
      connection: SequencerConnection,
      additionalConnections: SequencerConnection*
  ): Either[String, SequencerConnection]

  def sequencerAlias: SequencerAlias

  def certificates: Option[ByteString]

  def withCertificates(certificates: ByteString): SequencerConnection

  def withSequencerId(sequencerId: SequencerId): SequencerConnection

  def sequencerId: Option[SequencerId]
}

final case class GrpcSequencerConnection(
    endpoints: NonEmpty[Seq[Endpoint]],
    transportSecurity: Boolean,
    customTrustCertificates: Option[ByteString],
    sequencerAlias: SequencerAlias,
    sequencerId: Option[SequencerId],
) extends SequencerConnection {

  override def certificates: Option[ByteString] = customTrustCertificates

  def mkChannelBuilder(clientChannelBuilder: ClientChannelBuilder, tracePropagation: Propagation)(
      implicit executor: Executor
  ): ManagedChannelBuilderProxy =
    ManagedChannelBuilderProxy(
      clientChannelBuilder
        .create(endpoints, transportSecurity, executor, customTrustCertificates, tracePropagation)
    )

  override def toProtoV30: v30.SequencerConnection =
    v30.SequencerConnection(
      v30.SequencerConnection.Type.Grpc(
        v30.SequencerConnection.Grpc(
          endpoints.map(_.toURI(transportSecurity).toString).toList,
          transportSecurity,
          customTrustCertificates,
        )
      ),
      alias = sequencerAlias.toProtoPrimitive,
      sequencerId = sequencerId.map(_.toProtoPrimitive),
    )

  override protected def pretty: Pretty[GrpcSequencerConnection] =
    prettyOfClass(
      param("sequencerAlias", _.sequencerAlias),
      paramIfDefined("sequencerId", _.sequencerId),
      param("endpoints", _.endpoints.map(_.toURI(transportSecurity)).toList),
      paramIfTrue("transportSecurity", _.transportSecurity),
      paramIfTrue("customTrustCertificates", _.customTrustCertificates.nonEmpty),
    )

  override def addEndpoints(
      connection: URI,
      additionalConnections: URI*
  ): Either[String, SequencerConnection] =
    for {
      newEndpoints <- Endpoint
        .fromUris(NonEmpty(Seq, connection, additionalConnections*))
    } yield copy(endpoints = endpoints ++ newEndpoints._1)

  override def addEndpoints(
      connection: SequencerConnection,
      additionalConnections: SequencerConnection*
  ): Either[String, SequencerConnection] =
    SequencerConnection
      .merge(this +: connection +: additionalConnections)

  override def withCertificates(certificates: ByteString): SequencerConnection =
    copy(customTrustCertificates = Some(certificates))

  override def withAlias(alias: SequencerAlias): SequencerConnection = copy(sequencerAlias = alias)

  override def withSequencerId(sequencerId: SequencerId): SequencerConnection =
    copy(sequencerId = Some(sequencerId))
}

object GrpcSequencerConnection {
  def create(
      connection: String,
      customTrustCertificates: Option[ByteString] = None,
      sequencerAlias: SequencerAlias = SequencerAlias.Default,
      sequencerId: Option[SequencerId] = None,
  ): Either[String, GrpcSequencerConnection] =
    for {
      uri <- UrlValidator.validate(connection).leftMap(_.message)
      endpointsWithTlsFlag <- Endpoint.fromUris(NonEmpty(Seq, uri))
      (endpoints, useTls) = endpointsWithTlsFlag
    } yield GrpcSequencerConnection(
      endpoints,
      useTls,
      customTrustCertificates,
      sequencerAlias,
      sequencerId,
    )

  def tryCreate(
      connection: String,
      customTrustCertificates: Option[ByteString] = None,
      sequencerAlias: SequencerAlias = SequencerAlias.Default,
  ): GrpcSequencerConnection =
    create(connection, customTrustCertificates, sequencerAlias) match {
      case Left(err) => throw new IllegalArgumentException(s"Invalid connection $connection : $err")
      case Right(es) => es
    }
}

object SequencerConnection {
  def fromProtoV30(
      configP: v30.SequencerConnection
  ): ParsingResult[SequencerConnection] =
    configP.`type` match {
      case v30.SequencerConnection.Type.Empty => Left(ProtoDeserializationError.FieldNotSet("type"))
      case v30.SequencerConnection.Type.Grpc(grpc) =>
        fromGrpcProto(grpc, configP.alias, configP.sequencerId)
    }

  private def fromGrpcProto(
      grpcP: v30.SequencerConnection.Grpc,
      alias: String,
      sequencerIdP: Option[String],
  ): ParsingResult[SequencerConnection] =
    for {
      uris <- grpcP.connections
        .traverse { connection =>
          UrlValidator
            .validate(connection)
            .leftMap(err =>
              ProtoDeserializationError
                .OtherError(s"Connection `$connection` is invalid: ${err.message}")
            )
        }
      urisNE <- NonEmpty.from(uris).toRight(ProtoDeserializationError.FieldNotSet("connections"))
      endpoints <- Endpoint
        .fromUris(urisNE)
        .leftMap(err => ProtoDeserializationError.ValueConversionError("connections", err))
      sequencerAlias <- SequencerAlias.fromProtoPrimitive(alias)
      sequencerId <- sequencerIdP.traverse(SequencerId.fromProtoPrimitive(_, "sequencer_id"))
    } yield GrpcSequencerConnection(
      endpoints._1,
      grpcP.transportSecurity,
      grpcP.customTrustCertificates,
      sequencerAlias,
      sequencerId,
    )

  def merge(connections: Seq[SequencerConnection]): Either[String, SequencerConnection] =
    for {
      connectionsNel <- NonEmpty
        .from(connections)
        .toRight("There must be at least one sequencer connection defined")
      _ <- Either.cond(
        connections.forall(_.sequencerAlias == connectionsNel.head1.sequencerAlias),
        (),
        "Sequencer connections can only be merged of the same alias",
      )
      firstSequencerIdO = connectionsNel.map(_.sequencerId).collectFirst { case Some(sequencerId) =>
        sequencerId
      }
      _ <- Either.cond(
        connections
          .flatMap(_.sequencerId)
          .forall(sequencerId => firstSequencerIdO.forall(_ == sequencerId)),
        (),
        "Sequencer connections can only be merged of the same sequencer id",
      )

      conn <- connectionsNel.head1 match {
        case grpc @ GrpcSequencerConnection(endpoints, _, _, _, _) =>
          for {
            allMergedEndpoints <- connectionsNel.tail1.flatTraverse {
              case grpc: GrpcSequencerConnection => Right(grpc.endpoints.forgetNE)
              case _ => Left("Cannot merge grpc and http sequencer connections")
            }
          } yield grpc.copy(
            endpoints = endpoints ++ allMergedEndpoints,
            sequencerId = firstSequencerIdO,
          )
      }
    } yield conn
}
