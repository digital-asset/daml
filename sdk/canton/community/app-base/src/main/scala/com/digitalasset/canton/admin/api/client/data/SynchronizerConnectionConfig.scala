// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.catsinstances.`cats nonempty traverse`
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.console.ConsoleEnvironment
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig as SynchronizerConnectionConfigInternal
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*
import monocle.syntax.all.*

import java.net.URI

/** The synchronizer connection configuration object
  *
  * @param synchronizerAlias
  *   alias to be used internally to refer to this synchronizer connection
  * @param sequencerConnections
  *   Configuration for the sequencers. In case of BFT synchronizer - there could be sequencers with
  *   multiple connections. Each sequencer can also support high availability, so multiple endpoints
  *   could be provided for each individual sequencer.
  * @param manualConnect
  *   if set to true (default false), the synchronizer is not connected automatically on startup.
  * @param synchronizerId
  *   if the synchronizer id is known, then it can be passed as an argument. during the handshake,
  *   the participant will check that the synchronizer id on the remote port is indeed the one given
  *   in the configuration. the synchronizer id can not be faked by a synchronizer. therefore, this
  *   additional check can be used to really ensure that you are talking to the right synchronizer.
  * @param priority
  *   the priority of this synchronizer connection. if there are more than one synchronizer
  *   connections, the
  *   [[com.digitalasset.canton.participant.protocol.submission.routing.TransactionRoutingProcessor]]
  *   will pick the synchronizer connection with the highest priority if possible.
  * @param initialRetryDelay
  *   synchronizer connections are "resilient". i.e. if a connection is lost, the system will keep
  *   trying to reconnect to a synchronizer.
  * @param maxRetryDelay
  *   control the backoff parameter such that the retry interval does not grow above this value
  * @param timeTracker
  *   the synchronizer time tracker settings. don't change it unless you know what you are doing.
  * @param initializeFromTrustedSynchronizer
  *   if false will automatically generate a SynchronizerTrustCertificate when connecting to a new
  *   synchronizer.
  */
final case class SynchronizerConnectionConfig(
    synchronizerAlias: SynchronizerAlias,
    sequencerConnections: SequencerConnections,
    manualConnect: Boolean = false,
    // TODO(#26021) Consider accepting both LSId and PSId
    synchronizerId: Option[PhysicalSynchronizerId] = None,
    priority: Int = 0,
    initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
    maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
    timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
    initializeFromTrustedSynchronizer: Boolean = false,
) extends PrettyPrinting {

  /** Merges this connection config with provided config, but only if the provided config is
    * subsumed by this config. The provided configuration is considered subsumed, if all its
    * settings are also included in this configuration, with the following exceptions:
    *
    *   - the field `synchronizerId` may be set in either or none of the configs, but if it's set in
    *     both, it must be the same synchronizer id.
    *   - the provided sequencer connections must all be included in this config's sequencer
    *     connections with the same sequencer alias. The SequencerConnections' sequencerId fields
    *     may be set in either or none of the two configs, but if it's set in both, it must be the
    *     same sequencer id.
    * @return
    *   either the merged configuration if the other configuration is subsumed by this
    *   configuration, or an error
    */
  def subsumeMerge(
      other: SynchronizerConnectionConfig
  ): Either[String, SynchronizerConnectionConfig] =
    other match {
      case SynchronizerConnectionConfig(
            `synchronizerAlias`,
            SequencerConnections(
              otherAliasToConnection,
              `sequencerConnections`.sequencerTrustThreshold,
              `sequencerConnections`.`sequencerLivenessMargin`,
              `sequencerConnections`.submissionRequestAmplification,
              `sequencerConnections`.sequencerConnectionPoolDelays,
            ),
            `manualConnect`,
            otherSynchronizerId,
            `priority`,
            `initialRetryDelay`,
            `maxRetryDelay`,
            `timeTracker`,
            `initializeFromTrustedSynchronizer`,
          ) =>
        for {
          updatedSynchronizerId <- mergeOrRequireEqual(synchronizerId, otherSynchronizerId)
          _ <- Either.cond(
            otherAliasToConnection.keySet
              .diff(sequencerConnections.aliasToConnection.keySet)
              .isEmpty,
            (),
            "new synchronizer connection does not contain all previous sequencer connections",
          )
          updatedConnections <- sequencerConnections.aliasToConnection.toSeq.toNEF
            .traverse { case (_, thisConnection: GrpcSequencerConnection) =>
              otherAliasToConnection
                .get(thisConnection.sequencerAlias)
                .traverse {
                  case GrpcSequencerConnection(
                        otherEndPoints,
                        `thisConnection`.transportSecurity,
                        `thisConnection`.customTrustCertificates,
                        `thisConnection`.sequencerAlias,
                        otherSequencerId,
                      ) =>
                    for {
                      updatedSequencerId <- mergeOrRequireEqual(
                        thisConnection.sequencerId,
                        otherSequencerId,
                      )
                      _ <- Either.cond(
                        (otherEndPoints.toSet -- thisConnection.endpoints).isEmpty,
                        (),
                        "new sequencer connection does not contain all endpoints of the previously configured sequencer connection",
                      )
                    } yield thisConnection
                      .focus(_.endpoints)
                      .modify(_.++(otherEndPoints).distinct)
                      .focus(_.sequencerId)
                      .replace(updatedSequencerId)
                  case otherConnection =>
                    Left(
                      s"new sequencer connection $thisConnection not subsume the previously existing sequencer connection $otherConnection"
                    )
                }
                .map(_.getOrElse(thisConnection))
            }
          updatedSequencerConnections <- SequencerConnections.many(
            updatedConnections,
            sequencerConnections.sequencerTrustThreshold,
            sequencerConnections.sequencerLivenessMargin,
            sequencerConnections.submissionRequestAmplification,
            sequencerConnections.sequencerConnectionPoolDelays,
          )
        } yield this.copy(
          synchronizerId = updatedSynchronizerId,
          sequencerConnections = updatedSequencerConnections,
        )
      case _ =>
        Left("the new synchronizer config does not subsume the existing synchronizer config")
    }

  /** If both options are Some, then both need to contain the same value, otherwise return the value
    * that was Some or None, if neither option contains a value.
    */
  private def mergeOrRequireEqual[A](a: Option[A], b: Option[A]): Either[String, Option[A]] =
    (a, b) match {
      case (Some(aa), Some(bb)) => Either.cond(aa == bb, a, "Mismatch")
      case _ => Right(a.orElse(b))
    }

  /** Helper methods to avoid having to use NonEmpty[Seq in the console */
  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: String,
      additionalConnections: String*
  ): Either[String, SynchronizerConnectionConfig] =
    addEndpoints(sequencerAlias, new URI(connection), additionalConnections.map(new URI(_))*)

  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: URI,
      additionalConnections: URI*
  ): Either[String, SynchronizerConnectionConfig] = for {
    sequencerConnections <- sequencerConnections.addEndpoints(
      sequencerAlias,
      connection,
      additionalConnections*
    )
  } yield copy(sequencerConnections = sequencerConnections)

  def addConnection(connection: SequencerConnection): Either[String, SynchronizerConnectionConfig] =
    for {
      sequencerConnections <- sequencerConnections.addEndpoints(
        connection.sequencerAlias,
        connection,
      )
    } yield copy(sequencerConnections = sequencerConnections)

  def withCertificates(
      sequencerAlias: SequencerAlias,
      certificates: ByteString,
  ): SynchronizerConnectionConfig =
    copy(sequencerConnections = sequencerConnections.withCertificates(sequencerAlias, certificates))

  def tryWithSequencerTrustThreshold(
      threshold: PositiveInt
  )(implicit consoleEnvironment: ConsoleEnvironment): SynchronizerConnectionConfig =
    sequencerConnections
      .withSequencerTrustThreshold(sequencerTrustThreshold = threshold)
      .map(connections => copy(sequencerConnections = connections))
      .valueOr(consoleEnvironment.raiseError)

  /* Used in `connectivity.rst` doc test */
  def withSequencerLivenessMargin(livenessMargin: NonNegativeInt): SynchronizerConnectionConfig =
    copy(sequencerConnections =
      sequencerConnections
        .withLivenessMargin(sequencerLivenessMargin = livenessMargin)
    )

  def withPriority(priority: Int): SynchronizerConnectionConfig =
    this.copy(priority = priority)

  def withSubmissionRequestAmplification(
      submissionRequestAmplification: SubmissionRequestAmplification
  ): SynchronizerConnectionConfig =
    this.copy(sequencerConnections =
      sequencerConnections.withSubmissionRequestAmplification(submissionRequestAmplification)
    )

  override protected def pretty: Pretty[SynchronizerConnectionConfig] =
    prettyOfClass(
      param("synchronizer", _.synchronizerAlias),
      param("sequencerConnections", _.sequencerConnections),
      param("manualConnect", _.manualConnect),
      paramIfDefined("physicalSynchronizerId", _.synchronizerId),
      paramIfDefined("priority", x => Option.when(x.priority != 0)(x.priority)),
      paramIfDefined("initialRetryDelay", _.initialRetryDelay),
      paramIfDefined("maxRetryDelay", _.maxRetryDelay),
      paramIfNotDefault("timeTracker", _.timeTracker, SynchronizerTimeTrackerConfig()),
      paramIfNotDefault(
        "initializeFromTrustedSynchronizer",
        _.initializeFromTrustedSynchronizer,
        false,
      ),
    )

  private[canton] def toInternal(implicit
      consoleEnvironment: ConsoleEnvironment
  ): SynchronizerConnectionConfigInternal =
    this.transformInto[SynchronizerConnectionConfigInternal]
}

object SynchronizerConnectionConfig {
  def tryGrpcSingleConnection(
      synchronizerAlias: SynchronizerAlias,
      sequencerAlias: SequencerAlias,
      connection: String,
      manualConnect: Boolean = false,
      psid: Option[PhysicalSynchronizerId] = None,
      certificates: Option[ByteString] = None,
      priority: Int = 0,
      initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
      maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
      timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
      initializeFromTrustedSynchronizer: Boolean = false,
  )(implicit consoleEnvironment: ConsoleEnvironment): SynchronizerConnectionConfig = {
    val sequencerConnection =
      SequencerConnections.single(
        GrpcSequencerConnection.tryCreate(connection, certificates, sequencerAlias)
      )

    SynchronizerConnectionConfig(
      synchronizerAlias,
      sequencerConnection,
      manualConnect,
      psid,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
      initializeFromTrustedSynchronizer,
    )
  }

  def tryGrpc(
      synchronizerAlias: SynchronizerAlias,
      connections: Seq[SequencerConnection],
      manualConnect: Boolean = false,
      psid: Option[PhysicalSynchronizerId] = None,
      priority: Int = 0,
      initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
      maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
      timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
      initializeFromTrustedSynchronizer: Boolean = false,
      sequencerTrustThreshold: PositiveInt = PositiveInt.one,
      sequencerLivenessMargin: NonNegativeInt = NonNegativeInt.zero,
      submissionRequestAmplification: SubmissionRequestAmplification =
        SubmissionRequestAmplification.NoAmplification,
      sequencerConnectionPoolDelays: SequencerConnectionPoolDelays =
        SequencerConnectionPoolDelays.default,
  )(implicit consoleEnvironment: ConsoleEnvironment): SynchronizerConnectionConfig = {
    val sequencerConnections =
      SequencerConnections.tryMany(
        connections,
        sequencerTrustThreshold = sequencerTrustThreshold,
        sequencerLivenessMargin = sequencerLivenessMargin,
        submissionRequestAmplification = submissionRequestAmplification,
        sequencerConnectionPoolDelays = sequencerConnectionPoolDelays,
      )

    SynchronizerConnectionConfig(
      synchronizerAlias,
      sequencerConnections,
      manualConnect,
      psid,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
      initializeFromTrustedSynchronizer,
    )
  }

  private[canton] def fromInternal(internal: SynchronizerConnectionConfigInternal)(implicit
      consoleEnvironment: ConsoleEnvironment
  ): SynchronizerConnectionConfig =
    internal.transformInto[SynchronizerConnectionConfig]
}
