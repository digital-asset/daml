// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.synchronizer

import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import com.google.protobuf.ByteString

import java.net.URI

/** The synchronizer connection configuration object
  *
  * @param synchronizerAlias alias to be used internally to refer to this synchronizer connection
  * @param sequencerConnections Configuration for the sequencers. In case of BFT synchronizer - there could be sequencers with multiple connections.
  *                             Each sequencer can also support high availability, so multiple endpoints could be provided for each individual sequencer.
  * @param manualConnect if set to true (default false), the synchronizer is not connected automatically on startup.
  * @param synchronizerId if the synchronizer id is known, then it can be passed as an argument. during the handshake, the
  *                 participant will check that the synchronizer id on the remote port is indeed the one given
  *                 in the configuration. the synchronizer id can not be faked by a synchronizer. therefore, this additional
  *                 check can be used to really ensure that you are talking to the right synchronizer.
  * @param priority the priority of this synchronizer connection. if there are more than one synchronizer connections,
  *                 the [[com.digitalasset.canton.participant.protocol.submission.routing.SynchronizerRouter]]
  *                 will pick the synchronizer connection with the highest priority if possible.
  * @param initialRetryDelay synchronizer connections are "resilient". i.e. if a connection is lost, the system will keep
  *                          trying to reconnect to a synchronizer.
  * @param maxRetryDelay control the backoff parameter such that the retry interval does not grow above this value
  * @param timeTracker the synchronizer time tracker settings. don't change it unless you know what you are doing.
  * @param initializeFromTrustedSynchronizer if false will automatically generate a SynchronizerTrustCertificate when connecting to a new synchronizer.
  */
final case class SynchronizerConnectionConfig(
    synchronizerAlias: SynchronizerAlias,
    sequencerConnections: SequencerConnections,
    manualConnect: Boolean = false,
    synchronizerId: Option[SynchronizerId] = None,
    priority: Int = 0,
    initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
    maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
    timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
    initializeFromTrustedSynchronizer: Boolean = false,
) extends HasVersionedWrapper[SynchronizerConnectionConfig]
    with PrettyPrinting {

  override protected def companionObj = SynchronizerConnectionConfig

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
  } yield (
    copy(sequencerConnections = sequencerConnections)
  )

  def addConnection(connection: SequencerConnection): Either[String, SynchronizerConnectionConfig] =
    for {
      sequencerConnections <- sequencerConnections.addEndpoints(
        connection.sequencerAlias,
        connection,
      )
    } yield (
      copy(sequencerConnections = sequencerConnections)
    )

  def withCertificates(
      sequencerAlias: SequencerAlias,
      certificates: ByteString,
  ): SynchronizerConnectionConfig =
    copy(sequencerConnections = sequencerConnections.withCertificates(sequencerAlias, certificates))

  override protected def pretty: Pretty[SynchronizerConnectionConfig] =
    prettyOfClass(
      param("synchronizer", _.synchronizerAlias),
      param("sequencerConnections", _.sequencerConnections),
      param("manualConnect", _.manualConnect),
      paramIfDefined("synchronizerId", _.synchronizerId),
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

  def toProtoV30: v30.SynchronizerConnectionConfig =
    v30.SynchronizerConnectionConfig(
      synchronizerAlias = synchronizerAlias.unwrap,
      sequencerConnections = sequencerConnections.toProtoV30.some,
      manualConnect = manualConnect,
      synchronizerId = synchronizerId.fold("")(_.toProtoPrimitive),
      priority = priority,
      initialRetryDelay = initialRetryDelay.map(_.toProtoPrimitive),
      maxRetryDelay = maxRetryDelay.map(_.toProtoPrimitive),
      timeTracker = timeTracker.toProtoV30.some,
      initializeFromTrustedSynchronizer = initializeFromTrustedSynchronizer,
    )
}

object SynchronizerConnectionConfig
    extends HasVersionedMessageCompanion[SynchronizerConnectionConfig]
    with HasVersionedMessageCompanionDbHelpers[SynchronizerConnectionConfig] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v33,
      supportedProtoVersion(v30.SynchronizerConnectionConfig)(fromProtoV30),
      _.toProtoV30,
    )
  )
  override def name: String = "synchronizer connection config"

  def grpc(
      sequencerAlias: SequencerAlias,
      synchronizerAlias: SynchronizerAlias,
      connection: String,
      manualConnect: Boolean = false,
      synchronizerId: Option[SynchronizerId] = None,
      certificates: Option[ByteString] = None,
      priority: Int = 0,
      initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
      maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
      timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
      initializeFromTrustedSynchronizer: Boolean = false,
  ): SynchronizerConnectionConfig =
    SynchronizerConnectionConfig(
      synchronizerAlias,
      SequencerConnections.single(
        GrpcSequencerConnection.tryCreate(connection, certificates, sequencerAlias)
      ),
      manualConnect,
      synchronizerId,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
      initializeFromTrustedSynchronizer,
    )

  def fromProtoV30(
      synchronizerConnectionConfigP: v30.SynchronizerConnectionConfig
  ): ParsingResult[SynchronizerConnectionConfig] = {
    val v30.SynchronizerConnectionConfig(
      synchronizerAlias,
      sequencerConnectionsPO,
      manualConnect,
      synchronizerId,
      priority,
      initialRetryDelayP,
      maxRetryDelayP,
      timeTrackerP,
      initializeFromTrustedSynchronizer,
    ) =
      synchronizerConnectionConfigP
    for {
      alias <- SynchronizerAlias
        .create(synchronizerAlias)
        .leftMap(err => InvariantViolation(s"SynchronizerConnectionConfig.synchronizer_alias", err))
      sequencerConnections <- ProtoConverter
        .required("sequencerConnections", sequencerConnectionsPO)
        .flatMap(SequencerConnections.fromProtoV30)
      synchronizerId <- OptionUtil
        .emptyStringAsNone(synchronizerId)
        .traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
      initialRetryDelay <- initialRetryDelayP.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("initialRetryDelay")
      )
      maxRetryDelay <- maxRetryDelayP.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("maxRetryDelay")
      )
      timeTracker <- ProtoConverter.parseRequired(
        SynchronizerTimeTrackerConfig.fromProto,
        "timeTracker",
        timeTrackerP,
      )
    } yield SynchronizerConnectionConfig(
      alias,
      sequencerConnections,
      manualConnect,
      synchronizerId,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
      initializeFromTrustedSynchronizer,
    )
  }
}
