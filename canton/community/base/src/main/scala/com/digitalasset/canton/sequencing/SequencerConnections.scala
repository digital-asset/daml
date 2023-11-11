// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.domain.api.{v0, v1}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseRequiredNonEmpty}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{ProtoDeserializationError, SequencerAlias}
import com.google.protobuf.ByteString

import java.net.URI

final case class SequencerConnections private (
    aliasToConnection: NonEmpty[Map[SequencerAlias, SequencerConnection]],
    sequencerTrustThreshold: PositiveInt,
) extends HasVersionedWrapper[SequencerConnections]
    with PrettyPrinting {
  require(
    aliasToConnection.sizeIs >= sequencerTrustThreshold.unwrap,
    s"sequencerTrustThreshold cannot be greater than number of sequencer connections. Found threshold of $sequencerTrustThreshold and ${aliasToConnection.size} sequencer connections",
  )

  aliasToConnection.foreach { case (alias, connection) =>
    require(
      alias == connection.sequencerAlias,
      "SequencerAlias in the Map must match SequencerConnection.sequencerAlias",
    )
  }

  def default: SequencerConnection = aliasToConnection.head1._2

  /** In case of BFT domain - multiple sequencers are required for proper functioning.
    * Some functionalities are only available in non-bft domain.
    * When nonBftSetup is false, it means that more than one sequencer connection is provided which doesn't imply a bft domain.
    */
  def nonBftSetup: Boolean = aliasToConnection.sizeIs == 1

  def connections: NonEmpty[Seq[SequencerConnection]] = aliasToConnection.map(_._2).toSeq

  def modify(
      sequencerAlias: SequencerAlias,
      m: SequencerConnection => SequencerConnection,
  ): SequencerConnections =
    aliasToConnection
      .get(sequencerAlias)
      .map { connection =>
        SequencerConnections(
          aliasToConnection.updated(
            sequencerAlias,
            m(connection),
          ),
          sequencerTrustThreshold,
        )
      }
      .getOrElse(this)

  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: URI,
      additionalConnections: URI*
  ): SequencerConnections =
    (Seq(connection) ++ additionalConnections).foldLeft(this) { case (acc, elem) =>
      acc.modify(sequencerAlias, _.addEndpoints(elem))
    }

  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: SequencerConnection,
      additionalConnections: SequencerConnection*
  ): SequencerConnections =
    (Seq(connection) ++ additionalConnections).foldLeft(this) { case (acc, elem) =>
      acc.modify(sequencerAlias, _.addEndpoints(elem))
    }

  def withCertificates(
      sequencerAlias: SequencerAlias,
      certificates: ByteString,
  ): SequencerConnections =
    modify(sequencerAlias, _.withCertificates(certificates))

  override def pretty: Pretty[SequencerConnections] =
    prettyOfParam(_.aliasToConnection.forgetNE)

  def toProtoV0: Seq[v0.SequencerConnection] = connections.map(_.toProtoV0)

  def toProtoV1: v1.SequencerConnections =
    new v1.SequencerConnections(connections.map(_.toProtoV0), sequencerTrustThreshold.unwrap)

  override protected def companionObj: HasVersionedMessageCompanionCommon[SequencerConnections] =
    SequencerConnections
}

object SequencerConnections
    extends HasVersionedMessageCompanion[SequencerConnections]
    with HasVersionedMessageCompanionDbHelpers[SequencerConnections] {

  def single(connection: SequencerConnection): SequencerConnections =
    new SequencerConnections(
      NonEmpty.mk(Seq, (connection.sequencerAlias, connection)).toMap,
      PositiveInt.tryCreate(1),
    )

  def many(
      connections: NonEmpty[Seq[SequencerConnection]],
      sequencerTrustThreshold: PositiveInt,
  ): Either[String, SequencerConnections] =
    if (connections.sizeIs == 1) {
      Right(SequencerConnections.single(connections.head1))
    } else if (connections.map(_.sequencerAlias).toSet.sizeCompare(connections) < 0) {
      val duplicatesAliases = connections.map(_.sequencerAlias).groupBy(identity).collect {
        case (alias, aliases) if aliases.lengthCompare(1) > 0 => alias
      }
      Left(s"Non-unique sequencer aliases detected: $duplicatesAliases")
    } else
      Either
        .catchOnly[IllegalArgumentException](
          new SequencerConnections(
            connections.map(conn => (conn.sequencerAlias, conn)).toMap,
            sequencerTrustThreshold,
          )
        )
        .leftMap(_.getMessage)

  def tryMany(
      connections: Seq[SequencerConnection],
      sequencerTrustThreshold: PositiveInt,
  ): SequencerConnections = {
    many(NonEmptyUtil.fromUnsafe(connections), sequencerTrustThreshold).valueOr(err =>
      throw new IllegalArgumentException(err)
    )
  }

  private def fromProtoV0V1(
      fieldName: String,
      connections: Seq[v0.SequencerConnection],
      sequencerTrustThreshold: PositiveInt,
  ): ParsingResult[SequencerConnections] = for {
    sequencerConnectionsNes <- parseRequiredNonEmpty(
      SequencerConnection.fromProtoV0,
      fieldName,
      connections,
    )
    _ <- Either.cond(
      sequencerConnectionsNes.map(_.sequencerAlias).toSet.size == sequencerConnectionsNes.size,
      (),
      ProtoDeserializationError.ValueConversionError(
        fieldName,
        "Every sequencer connection must have a unique sequencer alias",
      ),
    )
    sequencerConnections <- many(sequencerConnectionsNes, sequencerTrustThreshold).leftMap(
      ProtoDeserializationError.InvariantViolation(_)
    )
  } yield sequencerConnections

  def fromProtoV0(
      sequencerConnection: v0.SequencerConnection
  ): ParsingResult[SequencerConnections] =
    fromProtoV0V1("sequencer_connection", Seq(sequencerConnection), PositiveInt.tryCreate(1))

  def fromProtoV0(
      sequencerConnection: Seq[v0.SequencerConnection],
      sequencerTrustThreshold: Int,
  ): ParsingResult[SequencerConnections] =
    ProtoConverter
      .parsePositiveInt(sequencerTrustThreshold)
      .flatMap(fromProtoV0V1("sequencer_connections", sequencerConnection, _))

  def fromLegacyProtoV0(
      sequencerConnection: Seq[v0.SequencerConnection],
      providedSequencerTrustThreshold: Int,
  ): ParsingResult[SequencerConnections] = {
    val sequencerTrustThreshold =
      if (providedSequencerTrustThreshold == 0) sequencerConnection.size
      else providedSequencerTrustThreshold
    ProtoConverter
      .parsePositiveInt(sequencerTrustThreshold)
      .flatMap(fromProtoV0V1("sequencer_connections", sequencerConnection, _))
  }

  def fromProtoV1(
      sequencerConnections: v1.SequencerConnections
  ): ParsingResult[SequencerConnections] =
    ProtoConverter
      .parsePositiveInt(sequencerConnections.sequencerTrustThreshold)
      .flatMap(fromProtoV0V1("sequencer_connections", sequencerConnections.sequencerConnections, _))

  override def name: String = "sequencer connections"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.SequencerConnection)(fromProtoV0),
      _.default.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> ProtoCodec(
      ProtocolVersion.CNTestNet,
      supportedProtoVersion(v1.SequencerConnections)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )
}
