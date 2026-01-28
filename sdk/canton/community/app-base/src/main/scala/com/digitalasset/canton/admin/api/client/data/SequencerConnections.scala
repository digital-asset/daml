// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.{Id, Monad}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.ConsoleEnvironment
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.{
  SequencerConnectionPoolDelays as SequencerConnectionPoolDelaysInternal,
  SequencerConnectionValidation as SequencerConnectionValidationInternal,
  SequencerConnections as SequencerConnectionsInternal,
  SubmissionRequestAmplification as SubmissionRequestAmplificationInternal,
}
import com.digitalasset.canton.{SequencerAlias, config}
import com.google.protobuf.ByteString
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*

import java.net.URI

final case class SequencerConnections private (
    aliasToConnection: NonEmpty[Map[SequencerAlias, SequencerConnection]],
    sequencerTrustThreshold: PositiveInt,
    sequencerLivenessMargin: NonNegativeInt,
    submissionRequestAmplification: SubmissionRequestAmplification,
    sequencerConnectionPoolDelays: SequencerConnectionPoolDelays,
) extends PrettyPrinting {
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

  def connections: NonEmpty[Seq[SequencerConnection]] = aliasToConnection.map(_._2).toSeq

  def modify(
      sequencerAlias: SequencerAlias,
      m: SequencerConnection => SequencerConnection,
  ): SequencerConnections = modifyM[Id](sequencerAlias, m)

  private def modifyM[M[_]](
      sequencerAlias: SequencerAlias,
      m: SequencerConnection => M[SequencerConnection],
  )(implicit M: Monad[M]): M[SequencerConnections] =
    aliasToConnection
      .get(sequencerAlias)
      .map { connection =>
        M.map(m(connection)) { newSequencerConnection =>
          this.copy(
            aliasToConnection.updated(
              sequencerAlias,
              newSequencerConnection,
            )
          )
        }
      }
      .getOrElse(M.pure(this))

  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: URI,
      additionalConnections: URI*
  ): Either[String, SequencerConnections] =
    (Seq(connection) ++ additionalConnections).foldLeftM(this) { case (acc, elem) =>
      acc.modifyM(sequencerAlias, c => c.addEndpoints(elem))
    }

  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: SequencerConnection,
      additionalConnections: SequencerConnection*
  ): Either[String, SequencerConnections] =
    (Seq(connection) ++ additionalConnections).foldLeftM(this) { case (acc, elem) =>
      acc.modifyM(sequencerAlias, c => c.addEndpoints(elem))
    }

  def withCertificates(
      sequencerAlias: SequencerAlias,
      certificates: ByteString,
  ): SequencerConnections =
    modify(sequencerAlias, _.withCertificates(certificates))

  def withSubmissionRequestAmplification(
      submissionRequestAmplification: SubmissionRequestAmplification
  ): SequencerConnections =
    this.copy(submissionRequestAmplification = submissionRequestAmplification)

  def withSequencerTrustThreshold(
      sequencerTrustThreshold: PositiveInt
  ): Either[String, SequencerConnections] =
    Either.cond(
      aliasToConnection.sizeIs >= sequencerTrustThreshold.unwrap,
      this.copy(sequencerTrustThreshold = sequencerTrustThreshold),
      s"Sequencer trust threshold $sequencerTrustThreshold cannot be greater than number of sequencer connections ${aliasToConnection.size}",
    )

  def withLivenessMargin(sequencerLivenessMargin: NonNegativeInt): SequencerConnections =
    this.copy(sequencerLivenessMargin = sequencerLivenessMargin)

  override protected def pretty: Pretty[SequencerConnections] =
    prettyOfClass(
      param("connections", _.aliasToConnection.forgetNE),
      param("sequencer trust threshold", _.sequencerTrustThreshold),
      param("sequencer liveness margin", _.sequencerLivenessMargin),
      param("submission request amplification", _.submissionRequestAmplification),
      param("sequencer connection pool delays", _.sequencerConnectionPoolDelays),
    )

  private[canton] def toInternal(implicit
      consoleEnvironment: ConsoleEnvironment
  ): SequencerConnectionsInternal =
    SequencerConnectionsInternal
      .many(
        aliasToConnection.toSeq.map { case (_, connection) => connection.toInternal },
        sequencerTrustThreshold,
        sequencerLivenessMargin,
        submissionRequestAmplification.toInternal,
        sequencerConnectionPoolDelays.toInternal,
      )
      .valueOr(consoleEnvironment.raiseError)
}

object SequencerConnections {
  implicit private[data] def sequencerConnectionsToInternalTransformer(implicit
      consoleEnvironment: ConsoleEnvironment
  ): Transformer[SequencerConnections, SequencerConnectionsInternal] = _.toInternal

  implicit private[data] def sequencerConnectionsFromInternalTransformer(implicit
      consoleEnvironment: ConsoleEnvironment
  ): Transformer[SequencerConnectionsInternal, SequencerConnections] = fromInternal(_)

  def single(
      connection: SequencerConnection
  ): SequencerConnections =
    new SequencerConnections(
      aliasToConnection = NonEmpty(Map, connection.sequencerAlias -> connection),
      sequencerTrustThreshold = PositiveInt.one,
      sequencerLivenessMargin = NonNegativeInt.zero,
      submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
      sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
    )

  def many(
      connections: NonEmpty[Seq[SequencerConnection]],
      sequencerTrustThreshold: PositiveInt,
      sequencerLivenessMargin: NonNegativeInt,
      submissionRequestAmplification: SubmissionRequestAmplification,
      sequencerConnectionPoolDelays: SequencerConnectionPoolDelays,
  ): Either[String, SequencerConnections] = {
    val repeatedAliases = connections.groupBy(_.sequencerAlias).filter { case (_, connections) =>
      connections.lengthCompare(1) > 0
    }
    for {
      _ <- Either.cond(
        repeatedAliases.isEmpty,
        (),
        s"Repeated sequencer aliases found: $repeatedAliases",
      )
      sequencerConnections <- Either
        .catchOnly[IllegalArgumentException](
          new SequencerConnections(
            connections.map(conn => (conn.sequencerAlias, conn)).toMap,
            sequencerTrustThreshold,
            sequencerLivenessMargin,
            submissionRequestAmplification,
            sequencerConnectionPoolDelays,
          )
        )
        .leftMap(_.getMessage)
    } yield sequencerConnections
  }

  def tryMany(
      connections: Seq[SequencerConnection],
      sequencerTrustThreshold: PositiveInt,
      sequencerLivenessMargin: NonNegativeInt,
      submissionRequestAmplification: SubmissionRequestAmplification,
      sequencerConnectionPoolDelays: SequencerConnectionPoolDelays,
  )(implicit consoleEnvironment: ConsoleEnvironment): SequencerConnections = {
    val resultE = for {
      connectionsNE <- NonEmpty.from(connections).toRight("connections should not be empty")
      result <- many(
        connectionsNE,
        sequencerTrustThreshold,
        sequencerLivenessMargin,
        submissionRequestAmplification,
        sequencerConnectionPoolDelays,
      )
    } yield result

    resultE.valueOr(consoleEnvironment.raiseError)
  }

  private[canton] def fromInternal(internal: SequencerConnectionsInternal)(implicit
      consoleEnvironment: ConsoleEnvironment
  ): SequencerConnections =
    SequencerConnections
      .tryMany(
        internal.aliasToConnection.toSeq.map { case (_, connection) =>
          SequencerConnection.fromInternal(connection)
        },
        internal.sequencerTrustThreshold,
        internal.sequencerLivenessMargin,
        SubmissionRequestAmplification.fromInternal(internal.submissionRequestAmplification),
        SequencerConnectionPoolDelays.fromInternal(internal.sequencerConnectionPoolDelays),
      )
}

sealed trait SequencerConnectionValidation {
  private[canton] def toInternal: SequencerConnectionValidationInternal =
    this.transformInto[SequencerConnectionValidationInternal]
}

object SequencerConnectionValidation {
  object Disabled extends SequencerConnectionValidation
  object All extends SequencerConnectionValidation
  object Active extends SequencerConnectionValidation
  object ThresholdActive extends SequencerConnectionValidation
}

/** Configures the submission request amplification. Amplification makes sequencer clients send
  * eligible submission requests to multiple sequencers to overcome message loss in faulty
  * sequencers.
  *
  * @param factor
  *   The maximum number of times the submission request shall be sent.
  * @param patience
  *   How long the sequencer client should wait after an acknowledged submission to a sequencer to
  *   observe the receipt or error before it attempts to send the submission request again (possibly
  *   to a different sequencer).
  */
final case class SubmissionRequestAmplification(
    factor: PositiveInt,
    patience: config.NonNegativeFiniteDuration,
) extends PrettyPrinting {
  override protected def pretty: Pretty[SubmissionRequestAmplification] = prettyOfClass(
    param("factor", _.factor),
    param("patience", _.patience),
  )

  private[canton] def toInternal: SubmissionRequestAmplificationInternal =
    this.transformInto[SubmissionRequestAmplificationInternal]
}

object SubmissionRequestAmplification {
  val NoAmplification: SubmissionRequestAmplification =
    SubmissionRequestAmplificationInternal.NoAmplification
      .transformInto[SubmissionRequestAmplification]

  private[canton] def fromInternal(
      internal: SubmissionRequestAmplificationInternal
  ): SubmissionRequestAmplification =
    internal.transformInto[SubmissionRequestAmplification]
}

/** Configures the various delays used by the sequencer connection pool.
  *
  * @param minRestartDelay
  *   Minimum duration after which a failed sequencer connection is restarted.
  * @param maxRestartDelay
  *   Maximum duration after which a failed sequencer connection is restarted.
  * @param warnValidationDelay
  *   The duration after which a warning is issued if a started connection still fails validation.
  * @param subscriptionRequestDelay
  *   Delay between the attempts to obtain new sequencer connections for the sequencer subscription
  *   pool, when the current number of subscriptions is below `trustThreshold` + `livenessMargin`.
  */
final case class SequencerConnectionPoolDelays(
    minRestartDelay: config.NonNegativeFiniteDuration,
    maxRestartDelay: config.NonNegativeFiniteDuration,
    warnValidationDelay: config.NonNegativeFiniteDuration,
    subscriptionRequestDelay: config.NonNegativeFiniteDuration,
) extends PrettyPrinting {
  override protected def pretty: Pretty[SequencerConnectionPoolDelays] = prettyOfClass(
    param("minRestartDelay", _.minRestartDelay),
    param("maxRestartDelay", _.maxRestartDelay),
    param("warnValidationDelay", _.warnValidationDelay),
    param("subscriptionRequestDelay", _.subscriptionRequestDelay),
  )

  private[canton] def toInternal: SequencerConnectionPoolDelaysInternal =
    this.transformInto[SequencerConnectionPoolDelaysInternal]
}

object SequencerConnectionPoolDelays {
  val default: SequencerConnectionPoolDelays =
    SequencerConnectionPoolDelaysInternal.default.transformInto[SequencerConnectionPoolDelays]

  private[canton] def fromInternal(
      internal: SequencerConnectionPoolDelaysInternal
  ): SequencerConnectionPoolDelays =
    internal.transformInto[SequencerConnectionPoolDelays]
}
