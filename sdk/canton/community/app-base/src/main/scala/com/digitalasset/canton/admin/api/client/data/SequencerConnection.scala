// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.console.ConsoleEnvironment
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.{Endpoint, UrlValidator}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection as GrpcSequencerConnectionInternal,
  SequencerConnection as SequencerConnectionInternal,
}
import com.digitalasset.canton.topology.SequencerId
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*

import java.net.URI

/** Our [[com.digitalasset.canton.config.ClientConfig]] provides the static configuration of API
  * connections between console and nodes, and between synchronizer members via the config files.
  * Participants however can connect to multiple synchronizers and sequencers, and the configuration
  * of these connections is more dynamic. The structures below are used to represent the dynamic
  * configuration of how a participant connects to a sequencer.
  */
sealed trait SequencerConnection extends PrettyPrinting {
  def withAlias(alias: SequencerAlias): SequencerConnection

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

  protected type Internal <: SequencerConnectionInternal

  private[canton] def toInternal: Internal
}

final case class GrpcSequencerConnection(
    endpoints: NonEmpty[Seq[Endpoint]],
    transportSecurity: Boolean,
    customTrustCertificates: Option[ByteString],
    sequencerAlias: SequencerAlias,
    sequencerId: Option[SequencerId],
) extends SequencerConnection {
  override def certificates: Option[ByteString] = customTrustCertificates

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
    Endpoint
      .fromUris(NonEmpty(Seq, connection, additionalConnections*))
      .map { case (newEndpoints, _) =>
        copy(endpoints = endpoints ++ newEndpoints)
      }

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

  protected type Internal = GrpcSequencerConnectionInternal

  private[canton] override def toInternal: Internal = this.transformInto[Internal]
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
  )(implicit consoleEnvironment: ConsoleEnvironment): GrpcSequencerConnection =
    create(connection, customTrustCertificates, sequencerAlias).valueOr(
      consoleEnvironment.raiseError
    )

  private[canton] def fromInternal(
      internal: GrpcSequencerConnectionInternal
  ): GrpcSequencerConnection =
    internal.transformInto[GrpcSequencerConnection]
}

object SequencerConnection {
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

  private[canton] def fromInternal(internal: SequencerConnectionInternal): SequencerConnection =
    internal.transformInto[SequencerConnection]
}
